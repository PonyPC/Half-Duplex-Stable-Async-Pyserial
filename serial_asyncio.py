#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Experimental implementation of asyncio support.
#
# This file is part of pySerial. https://github.com/pyserial/pyserial-asyncio
# (C) 2015-2017 pySerial-team
#
# SPDX-License-Identifier:    BSD-3-Clause
"""\
Support asyncio with serial ports. EXPERIMENTAL

Posix platforms only, Python 3.4+ only.

Windows event loops can not wait for serial ports with the current
implementation. It should be possible to get that working though.
"""
import asyncio
import os
import zlib

import serial

try:
	import termios
except ImportError:
	termios = None

__version__ = '0.4'
DEBUG = False

class SerialTransport(asyncio.Transport):
	"""An asyncio transport model of a serial communication channel.

	A transport class is an abstraction of a communication channel.
	This allows protocol implementations to be developed against the
	transport abstraction without needing to know the details of the
	underlying channel, such as whether it is a pipe, a socket, or
	indeed a serial port.


	You generally won’t instantiate a transport yourself; instead, you
	will call `create_serial_connection` which will create the
	transport and try to initiate the underlying communication channel,
	calling you back when it succeeds.
	"""
	EOT = b"\xFE"
	ESC = b"\xFF"

	def __init__(self, loop, protocol, serial_instance, buffer_size, address):
		super().__init__()
		self._loop = loop
		self._protocol = protocol
		self._serial = serial_instance
		if buffer_size:
			self._buffer_size = buffer_size - 3 # end(1)
		else:
			self._buffer_size = 0
		self._address = address
		self._closing = False
		self._max_read_size = 1024
		self._is_sending = False
		self._confirm_task = None
		self._retry_count = 0
		self._sending_seq = 0
		self._sending_frames = []
		self._write_buffer = []
		self._read_frames = []
		self._read_buffer = b''
		self._poll_wait_time = 0.0005

		# Asynchronous I/O requires non-blocking devices
		self._serial.timeout = 0
		self._serial.write_timeout = 0

		# These two callbacks will be enqueued in a FIFO queue by asyncio
		loop.call_soon(protocol.connection_made, self)
		loop.call_soon(self._ensure_reader)

	def __repr__(self):
		return '{self.__class__.__name__}({self.loop}, {self._protocol}, {self.serial})'.format(self=self)

	@property
	def loop(self):
		"""The asyncio event loop used by this SerialTransport."""
		return self._loop

	@property
	def serial(self):
		"""The underlying Serial instance."""
		return self._serial

	def is_closing(self):
		"""Return True if the transport is closing or closed."""
		return self._closing

	def can_write_eof(self):
		"""Serial ports do not support the concept of end-of-file.

		Always returns False.
		"""
		return False

	def write_eof(self):
		raise NotImplementedError("Serial connections do not support end-of-file")

	def close(self):
		"""Close the transport gracefully.

		Any buffered data will be written asynchronously. No more data
		will be received and further writes will be silently ignored.
		After all buffered data is flushed, the protocol's
		connection_lost() method will be called with None as its
		argument.
		"""
		if not self._closing:
			self._close(None)

	def abort(self):
		"""Close the transport immediately.

		Pending operations will not be given opportunity to complete,
		and buffered data will be lost. No more data will be received
		and further writes will be ignored.  The protocol's
		connection_lost() method will eventually be called.
		"""
		self._abort(None)

	def get_write_buffer_size(self):
		"""The number of bytes in the write buffer.

		This buffer is unbounded, so the result may be larger than the
		the high water mark.
		"""
		return sum(map(len, self._write_buffer)) + sum(map(len, self._sending_frames))
				
	def _crcbin(self, crc):
		return crc.to_bytes(length=4, byteorder='big', signed=False)
		
	def _crcint(self, crc):
		return int.from_bytes(crc, byteorder='big', signed=False)
		
	def _decode(self, data):
		return data.replace(self.ESC + self.EOT, self.EOT).replace(self.ESC + self.ESC, self.ESC)
		
	def _encode(self, data):
		return data.replace(self.ESC, self.ESC + self.ESC).replace(self.EOT, self.ESC + self.EOT)
		
	def write(self, data):
		"""Write some data to the transport.

		This method does not block; it buffers the data and arranges
		for it to be sent out asynchronously.  Writes made after the
		transport has been closed will be ignored."""
		if self._closing:
			return

		if not self._buffer_size:
			# Attempt to send it right away first
			try:
				self._serial.write(data)
			except serial.SerialException as exc:
				self._fatal_error(exc, 'Fatal write error on serial transport')
				return
		else:
			self._write_buffer.append(data)
			if not self._is_sending:
				self._is_sending = True
				self._loop.call_soon(self._write_ready)

	def _avaliable_buffer(self):
		if len(self._write_buffer) > 0:
			self._sending_frames.clear()
			data = self._write_buffer.pop(0)
			if len(data) > 1:
				address = data[0]
				encoded_data = self._encode(data[1:])
				if len(encoded_data) <= self._buffer_size - 8: # crc
					header = bytes([address, self.FAST_TRANS])
					crc = self._crcbin(zlib.crc32(header + data[1:]))
					self._sending_frames.append(header + encoded_data + self._encode(crc))
				else:
					index = 0
					while index < len(encoded_data):
						if index + self._buffer_size < len(encoded_data): # 在正确编码的情况下，如果数据结尾是ESC，那肯定会有一个前导ESC，所以不检查等于的情况
							tmp = encoded_data[index:index + self._buffer_size]
							index += self._buffer_size
							shrink = False
							for i in range(1, len(tmp) + 1):
								if tmp[-i] != self.ESC[0]:
									break
								shrink = not shrink
							if shrink:
								tmp = tmp[:-1]
								index -= 1
							self._sending_frames.append(tmp)
						else:
							self._sending_frames.append(encoded_data[index:])
							break
					frame_count = len(self._sending_frames)
					if frame_count > 0xFF:
						return
					for i in range(frame_count):
						self._sending_frames[i] = bytes([address, self.DO_TRANS]) + self._sending_frames[i]
					header = bytes([address, self.INIT_TRANS])
					crc = self._crcbin(zlib.crc32(header))
					self._sending_frames.insert(0, header + self._encode(crc))
					header = bytes([address, self.FINISH_TRANS])
					crc = self._crcbin(zlib.crc32(header))
					self._sending_frames.append(header + self._encode(crc))
				
				self._retry_count = 0
				self._sending_seq = 0
		else:
			self._is_sending = False
			
	def _spend_time(self, data):
		return len(data) / 512 + 0.6 # caculate for your lora speed

	INIT_TRANS = 0x01
	DO_TRANS = 0x02
	RE_TRANS = 0x03
	FINISH_TRANS = 0x04
	FAST_TRANS = 0x05
			
	def _write_ready(self):
		if self._confirm_task:
			self._confirm_task = None
		if not self._sending_frames:
			self._avaliable_buffer()
		if self._sending_frames:
			if self._retry_count < 3:
				if self._sending_seq < len(self._sending_frames):
					data = self._sending_frames[self._sending_seq]
					if self._retry_count > 0 and data[1] == self.DO_TRANS:
						data = bytes([data[0], self.RE_TRANS]) + data[2:]
					if DEBUG:
						print('try ' + str(self._retry_count) + ' action ' + str(data[1]))
					self._retry_count += 1
					try:
						self._serial.write(data + self.EOT)
					except serial.SerialException as exc:
						self._fatal_error(exc, 'Fatal write error on serial transport')
						return

					wait_time = self._spend_time(data) + 0.4
					self._confirm_task = self._loop.call_later(wait_time, self._write_ready)
					return
				else:
					if DEBUG:
						if self._sending_seq == len(self._sending_frames):
							print('finish msg')
			else:
				if DEBUG:
					print('give up msg')
			self._sending_frames.clear()
			self._loop.call_soon(self._write_ready)
		elif self._is_sending:
			self._loop.call_soon(self._write_ready)
		else:
			if self._closing and self._flushed():
				self._close()
	
	def _read_ready(self):
		try:
			data = self._serial.read(self._max_read_size)
		except serial.SerialException as e:
			self._close(exc=e)
		else:
			if data:
				if not self._buffer_size:
					self._protocol.data_received(data)
				else:
					self._read_buffer += data
					if self.EOT in data:
						index = 0
						escaped = False
						while index < len(self._read_buffer):
							if self._read_buffer[index] == self.ESC[0]:
								escaped = not escaped
							else:
								if not escaped:
									if self._read_buffer[index] == self.EOT[0]:
										origin_data = self._read_buffer[:index]
										self._read_buffer = self._read_buffer[index+1:]
										index = -1
										if 0 <= len(origin_data) - 2 <= self._buffer_size:
											address = origin_data[0]
											action = origin_data[1]
											data = self._decode(origin_data[2:])
											if self._address == address:
												if action == self.INIT_TRANS and len(data) == 4:
													if DEBUG:
														print('INIT_TRANS')
													crc = self._crcint(data[-4:])
													if zlib.crc32(origin_data[:2] + data[:-4]) == crc:
														self._read_frames = []
														self._serial.write(origin_data + self.EOT)
												elif action == self.DO_TRANS and len(data) > 5:
													if DEBUG:
														print('DO_TRANS')
													self._read_frames.append(data)
													crc = self._crcbin(zlib.crc32(origin_data))
													self._serial.write(origin_data[:2] + self._encode(crc) + self.EOT)
												elif action == self.RE_TRANS and len(data) > 5:
													if DEBUG:
														print('RE_TRANS')
													if self._read_frames:
														self._read_frames[-1] = data
													else:
														self._read_frames.append(data)
													crc = self._crcbin(zlib.crc32(bytes([address, self.DO_TRANS]) + origin_data[2:]))
													self._serial.write(origin_data[:2] + self._encode(crc) + self.EOT)
												elif action == self.FINISH_TRANS and len(data) == 4:
													if DEBUG:
														print('FINISH_TRANS')
													crc = self._crcint(data[-4:])
													if zlib.crc32(origin_data[:2] + data[:-4]) == crc:
														if self._read_frames:
															real_data = b''.join(self._read_frames)
															self._read_frames = []
															self._serial.write(origin_data + self.EOT)
															self._protocol.data_received(real_data)
												elif action == self.FAST_TRANS:
													if DEBUG:
														print('FAST_TRANS')
													crc = self._crcint(data[-4:])
													if zlib.crc32(origin_data[:2] + data[:-4]) == crc:
														crc = self._crcbin(zlib.crc32(origin_data))
														self._serial.write(origin_data[:2] + self._encode(crc) + self.EOT)
														self._protocol.data_received(data[:-4])
											elif self._address == 0x00:
												if action == self.INIT_TRANS:
													if DEBUG:
														print('r:INIT_TRANS')
													if self._is_sending and self._sending_frames:
														if self._sending_seq == 0:
															local = self._sending_frames[self._sending_seq]
															if origin_data == local:
																self._sending_seq += 1
																self._retry_count = 0
															if self._confirm_task:
																self._confirm_task.cancel()
																self._confirm_task = None
															self._loop.call_soon(self._write_ready)
												elif action == self.DO_TRANS and len(data) == 4:
													if DEBUG:
														print('r:DO_TRANS')
													if self._is_sending and self._sending_frames:
														if 0 < self._sending_seq < len(self._sending_frames) - 1:
															local = self._sending_frames[self._sending_seq]
															crc = self._crcint(data[-4:])
															if zlib.crc32(local) == crc:
																self._sending_seq += 1
																self._retry_count = 0
															if self._confirm_task:
																self._confirm_task.cancel()
																self._confirm_task = None
															self._loop.call_soon(self._write_ready)
												if action == self.FINISH_TRANS:
													if DEBUG:
														print('r:FINISH_TRANS')
													if self._is_sending and self._sending_frames:
														if self._sending_seq == len(self._sending_frames) - 1:
															local = self._sending_frames[self._sending_seq]
															if origin_data == local:
																self._sending_seq += 1
																self._retry_count = 0
															if self._confirm_task:
																self._confirm_task.cancel()
																self._confirm_task = None
															self._loop.call_soon(self._write_ready)
												elif action == self.FAST_TRANS and len(data) == 4:
													if DEBUG:
														print('r:FAST_TRANS')
													if self._is_sending and self._sending_frames:
														if len(self._sending_frames) == 1:
															local = self._sending_frames[0]
															crc = self._crcint(data[-4:])
															if zlib.crc32(local) == crc:
																self._sending_seq += 1
																self._retry_count = 0
															if self._confirm_task:
																self._confirm_task.cancel()
																self._confirm_task = None
															self._loop.call_soon(self._write_ready)
								escaped = False
							index += 1

	if os.name == "nt":
		def _poll_read(self):
			if not self._closing:
				try:
					if self.serial.in_waiting:
						self._loop.call_soon(self._read_ready)
					self._loop.call_later(self._poll_wait_time, self._poll_read)
				except serial.SerialException as exc:
					self._fatal_error(exc, 'Fatal write error on serial transport')

		def _ensure_reader(self):
			if not self._closing:
				self._loop.call_later(self._poll_wait_time, self._poll_read)

		def _remove_reader(self):
			pass

	else:
		def _ensure_reader(self):
			if not self._closing:
				self._loop.add_reader(self._serial.fileno(), self._read_ready)

		def _remove_reader(self):
			self._loop.remove_reader(self._serial.fileno())

	def _fatal_error(self, exc, message='Fatal error on serial transport'):
		"""Report a fatal error to the event-loop and abort the transport."""
		self._loop.call_exception_handler({
			'message': message,
			'exception': exc,
			'transport': self,
			'protocol': self._protocol,
		})
		self._abort(exc)

	def _flushed(self):
		"""True if the write buffer is empty, otherwise False."""
		return self.get_write_buffer_size() == 0

	def _close(self, exc=None):
		"""Close the transport gracefully.

		If the write buffer is already empty, writing will be
		stopped immediately and a call to the protocol's
		connection_lost() method scheduled.

		If the write buffer is not already empty, the
		asynchronous writing will continue, and the _write_ready
		method will call this _close method again when the
		buffer has been flushed completely.
		"""
		self._closing = True
		self._remove_reader()
		if self._flushed():
			self._loop.call_soon(self._call_connection_lost, exc)

	def _abort(self, exc):
		"""Close the transport immediately.

		Pending operations will not be given opportunity to complete,
		and buffered data will be lost. No more data will be received
		and further writes will be ignored.  The protocol's
		connection_lost() method will eventually be called with the
		passed exception.
		"""
		self._closing = True
		self._remove_reader()
		self._loop.call_soon(self._call_connection_lost, exc)

	def _call_connection_lost(self, exc):
		"""Close the connection.

		Informs the protocol through connection_lost() and clears
		pending buffers and closes the serial connection.
		"""
		assert self._closing
		try:
			self._serial.flush()
		except (serial.SerialException if os.name == "nt" else termios.error):
			# ignore serial errors which may happen if the serial device was
			# hot-unplugged.
			pass
		try:
			self._protocol.connection_lost(exc)
		finally:
			self._confirm_task = None
			self._sending_frames.clear()
			self._write_buffer.clear()
			self._read_frames.clear()
			self._read_buffer = b''
			self._serial.close()
			self._serial = None
			self._protocol = None
			self._loop = None

@asyncio.coroutine
def create_serial_connection(loop, protocol_factory, buffer_size, address, *args, **kwargs):
	ser = serial.serial_for_url(*args, **kwargs)
	protocol = protocol_factory()
	transport = SerialTransport(loop, protocol, ser, buffer_size, address)
	return (transport, protocol)