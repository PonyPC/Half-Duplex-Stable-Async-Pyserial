#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import os
import serial
import crc16

try:
	import termios
except ImportError:
	termios = None

DEBUG = False

INIT_TRANS = 0x01
DO_TRANS = 0x02
RE_TRANS = 0x03
FINISH_TRANS = 0x04
SHORT_TRANS = 0x05
REPLY_TRANS = 0x06
NODELAY_TRANS = 0x07
EOT = b'\xFE'
ESC = b'\xFF'
	
def _decode(data):
	return data.replace(ESC + EOT, EOT).replace(ESC + ESC, ESC)
	
def _encode(data):
	return data.replace(ESC, ESC + ESC).replace(EOT, ESC + EOT)

_MAX_CRC_SIZE = 4
				
def _gencrc(data):
	crc = crc16.crc16xmodem(data)
	return crc.to_bytes(length=2, byteorder='big', signed=False)
	
def _checkcrc(data, crc):
	return _gencrc(data) == crc
	
def _buildframe(addr, action, data = b''):
	header = bytes([addr, action])
	crc = _gencrc(header + data)
	return header, crc

class DataFrame():
	def __init__(self, transport, addr, data, callback, need_reply, retry):
		self._transport = transport
		self._addr = addr
		self._callback = callback
		self._retry = retry
		self._remain = retry
		self._need_reply = need_reply
		self._reply = None
		self._seq = 0
		self._frames = []
		
		encoded_data = _encode(data)
		if len(encoded_data) <= transport.buffer_size - _MAX_CRC_SIZE: # crc
			header, crc = _buildframe(self._addr, REPLY_TRANS if need_reply else SHORT_TRANS, data)
			self._frames.append(header + encoded_data + _encode(crc))
		else:
			index = 0
			while index < len(encoded_data):
				if index + transport.buffer_size < len(encoded_data): # 在正确编码的情况下，如果数据结尾是ESC，那肯定会有一个前导ESC，所以不检查等于的情况
					tmp = encoded_data[index:index + transport.buffer_size]
					index += transport.buffer_size
					shrink = False
					for i in range(1, len(tmp) + 1):
						if tmp[-i] != ESC[0]:
							break
						shrink = not shrink
					if shrink:
						tmp = tmp[:-1]
						index -= 1
					self._frames.append(tmp)
				else:
					self._frames.append(encoded_data[index:])
					break
			header, crc = _buildframe(self._addr, INIT_TRANS)
			self._frames.insert(0, header + _encode(crc))
			header, crc = _buildframe(self._addr, FINISH_TRANS)
			self._frames.append(header + _encode(crc))

	def status(self):
		if self._seq < len(self._frames):
			if len(self._frames) == 1:
				if self._need_reply:
					return 'reply'
				else:
					return 'short'
			else:
				if self._seq == 0:
					return 'init'
				elif self._seq == len(self._frames) - 1:
					return 'finish'
				else:
					return 'do'
		return None
	
	def crc(self):
		if self._seq < len(self._frames):
			data = self._frames[self._seq]
			if len(self._frames) > 1:
				if 0 < self._seq < len(self._frames) - 1:
					trans_code = DO_TRANS if self._remain == self._retry - 1 else RE_TRANS
					_, realcrc = _buildframe(self._addr, trans_code, self._frames[self._seq])
					return realcrc
				else:
					return data
		return None
	
	def msg(self):
		if self._remain > 0:
			if self._seq < len(self._frames):
				data = self._frames[self._seq]
				if len(self._frames) == 1:
					if DEBUG:
						print(('> REPLY_TRANS' if self._need_reply else '> SHORT_TRANS') + ' remain ' + str(self._remain))
					self._remain -= 1
					return data
				else:
					if 0 < self._seq < len(self._frames) - 1:
						trans_code = DO_TRANS if self._remain == self._retry else RE_TRANS
						if DEBUG:
							print('> ' + ('DO_TRANS' if trans_code == DO_TRANS else 'RE_TRANS') + ' remain ' + str(self._remain))
						self._remain -= 1
						return bytes([self._addr, trans_code]) + self._frames[self._seq]
					else:
						if DEBUG:
							print('> ' + ('INIT_TRANS' if data[1] == INIT_TRANS else 'FINISH_TRANS') + ' remain ' + str(self._remain))
						self._remain -= 1
						return data
			else:
				if self._seq == len(self._frames):
					if self._callback:
						if self._need_reply:
							self._callback(self._reply)
						else:
							self._callback(True)
					if DEBUG:
						print('√')
				return None
		else:
			if self._callback:
				self._callback(False)
			if DEBUG:
				print('×')
			return None

	def nextmsg(self):
		self._remain = self._retry
		self._seq += 1

	def setreply(self, reply):
		self._reply = reply
		
class SerialTransport(asyncio.Transport):
	def __init__(self, loop, protocol, serial_instance, buffer_size, address):
		super().__init__()
		self._loop = loop
		self._protocol = protocol
		self._serial = serial_instance
		self._closing = False
		self._max_read_size = 1024
		self._poll_wait_time = 0.0005
		if buffer_size:
			self._buffer_size = buffer_size - 3 # end(1)
		else:
			self._buffer_size = 0
		self._is_sending = False
		self._address = address
		self._confirm_task = None
		self._msgs = []
		self._nodelay_msg = None
		self._read_frames = []
		self._read_buffer = b''

		# Asynchronous I/O requires non-blocking devices
		self._serial.timeout = 0
		self._serial.write_timeout = 0

		# These two callbacks will be enqueued in a FIFO queue by asyncio
		loop.call_soon(protocol.connection_made, self)
		loop.call_soon(self._ensure_reader)
		
	def send(self, addr, data, callback = None, need_reply = False, retry = 3):
		"""Write some data to the transport.

		This method does not block; it buffers the data and arranges
		for it to be sent out asynchronously.  Writes made after the
		transport has been closed will be ignored."""
		if self._closing:
			return

		if self._buffer_size:
			self._msgs.append(DataFrame(self, addr, data, callback, need_reply, retry))
			if not self._is_sending:
				self._is_sending = True
				self._loop.call_soon(self._write_ready)
		
	def nodelay(self, addr, data):
		if self._closing:
			return

		if self._buffer_size:
			encoded_data = _encode(data)
			if len(encoded_data) <= self._buffer_size - _MAX_CRC_SIZE: # crc
				header, crc = _buildframe(addr, NODELAY_TRANS, data)
				self._nodelay_msg = header + encoded_data + _encode(crc)
				if not self._is_sending:
					self._is_sending = True
					self._loop.call_soon(self._write_ready)
			
	def _spend_time(self, data_len): # caculate for your lora speed
		return data_len / 300 + 0.4
		# current sx1278 runs at 4.8kbps
		# if DEBUG:
			# print('data_len ' + str(data_len + 1))
			
	def _write_ready(self):
		if self._confirm_task:
			self._confirm_task = None
		if self._nodelay_msg:
			data = self._nodelay_msg
			self._nodelay_msg = None
			if data:
				wait_time = self._spend_time(len(data))
				self.write(data + EOT)
				self._confirm_task = self._loop.call_later(wait_time, self._write_ready)
				if DEBUG:
					print('> NODELAY_TRANS')
			else:
				self._loop.call_soon(self._write_ready)
		elif len(self._msgs) > 0:
			data = self._msgs[0].msg()
			if data:
				if self._msgs[0].status() == 'reply':
					wait_time = self._spend_time(len(data)) + self._spend_time(self._buffer_size)
				else:
					wait_time = self._spend_time(len(data)) + self._spend_time(0)
				self.write(data + EOT)
				self._confirm_task = self._loop.call_later(wait_time, self._write_ready)
			else:
				self._msgs.pop(0)
				self._loop.call_soon(self._write_ready)
		else:
			self._is_sending = False
			if self._closing and self._flushed():
				self._loop.call_soon(self._call_connection_lost)

	def _on_read_data(self, origin_data):
		address = origin_data[0]
		action = origin_data[1]
		data = _decode(origin_data[2:])
		if self._address == address: # rover
			if action == INIT_TRANS and len(data) == 2:
				_, realcrc = _buildframe(address, action)
				if realcrc == data[-2:]:
					self._read_frames = []
					self.write(origin_data + EOT)
					if DEBUG:
						print(': INIT_TRANS')
				elif DEBUG:
					print(': INIT_TRANS crc ×')
			elif action == FINISH_TRANS and len(data) == 2:
				_, realcrc = _buildframe(address, action)
				if realcrc == data[-2:]:
					if self._read_frames:
						real_data = b''.join(self._read_frames)
						self._read_frames = []
						self.write(origin_data + EOT)
						self._protocol.data_received(real_data)
						if DEBUG:
							print(': FINISH_TRANS')
				elif DEBUG:
					print(': FINISH_TRANS crc ×')
			elif (action == DO_TRANS or action == RE_TRANS) and len(data) > 2:
				if DEBUG:
					print(': DO_TRANS' if action == DO_TRANS else ': RE_TRANS')
				if action == RE_TRANS and self._read_frames:
					self._read_frames[-1] = data
				else:
					self._read_frames.append(data)
				header, realcrc = _buildframe(address, action, data)
				self.write(header + _encode(realcrc) + EOT)
			elif action == SHORT_TRANS and len(data) > 2:
				_, realcrc = _buildframe(address, action, data[:-2])
				if realcrc == data[-2:]:
					header, realcrc = _buildframe(address, action)
					self.write(header + _encode(realcrc) + EOT)
					self._protocol.data_received(data[:-2])
					if DEBUG:
						print(': SHORT_TRANS')
				elif DEBUG:
					print(': SHORT_TRANS crc ×')
			elif action == REPLY_TRANS and len(data) > 2:
				_, realcrc = _buildframe(address, action, data[:-2])
				if realcrc == data[-2:]:
					reply = self._protocol.data_received(data[:-2])
					encoded_reply = _encode(reply)
					if len(encoded_reply) < self._buffer_size - _MAX_CRC_SIZE:
						header, realcrc = _buildframe(address, action, reply)
						self.write(header + encoded_reply + _encode(realcrc) + EOT)
					if DEBUG:
						print(': REPLY_TRANS')
				elif DEBUG:
					print(': REPLY_TRANS crc ×')
			elif action == NODELAY_TRANS and len(data) > 2:
				_, realcrc = _buildframe(address, action, data[:-2])
				if realcrc == data[-2:]:
					reply = self._protocol.data_received(data[:-2])
					if DEBUG:
						print(': NODELAY_TRANS')
				elif DEBUG:
					print(': NODELAY_TRANS crc ×')
			elif DEBUG:
				print(': Unknown ×')
				print(address, action, data)

		elif self._address == 0x00:
			if action == INIT_TRANS:
				if self._is_sending and len(self._msgs) > 0:
					if self._msgs[0].status() == 'init':
						local = self._msgs[0].crc()
						if origin_data == local:
							self._msgs[0].nextmsg()
							if DEBUG:
								print('>> INIT_TRANS')
						elif DEBUG:
							print('>> INIT_TRANS equal ×')
						if self._confirm_task:
							self._confirm_task.cancel()
							self._confirm_task = None
						self._loop.call_soon(self._write_ready)
			elif action == FINISH_TRANS:
				if self._is_sending and len(self._msgs) > 0:
					if self._msgs[0].status() == 'finish':
						local = self._msgs[0].crc()
						if origin_data == local:
							self._msgs[0].nextmsg()
							if DEBUG:
								print('>> FINISH_TRANS')
						elif DEBUG:
							print('>> FINISH_TRANS equal ×')
						if self._confirm_task:
							self._confirm_task.cancel()
							self._confirm_task = None
						self._loop.call_soon(self._write_ready)
			elif (action == DO_TRANS or action == RE_TRANS) and len(data) == 2:
				if self._is_sending and len(self._msgs) > 0:
					if self._msgs[0].status() == 'do':
						realcrc = self._msgs[0].crc()
						if realcrc == data[-2:]:
							self._msgs[0].nextmsg()
							if DEBUG:
								print('>> DO_TRANS' if action == DO_TRANS else '>> RE_TRANS')
						elif DEBUG:
							print('>> DO_TRANS crc ×' if action == DO_TRANS else '>> RE_TRANS crc ×')
						if self._confirm_task:
							self._confirm_task.cancel()
							self._confirm_task = None
						self._loop.call_soon(self._write_ready)
			elif action == SHORT_TRANS and len(data) == 2:
				if self._is_sending and len(self._msgs) > 0:
					if self._msgs[0].status() == 'short':
						_, realcrc = _buildframe(address, action)
						if realcrc == data[-2:]:
							self._msgs[0].nextmsg()
							if DEBUG:
								print('>> SHORT_TRANS')
						elif DEBUG:
							print('>> SHORT_TRANS crc ×')
						if self._confirm_task:
							self._confirm_task.cancel()
							self._confirm_task = None
						self._loop.call_soon(self._write_ready)
			elif action == REPLY_TRANS and len(data) > 2:
				if self._is_sending and len(self._msgs) > 0:
					if self._msgs[0].status() == 'reply':
						_, realcrc = _buildframe(address, action, data[:-2])
						if realcrc == data[-2:]:
							self._msgs[0].setreply(data[:-2])
							self._msgs[0].nextmsg()
							if DEBUG:
								print('>> REPLY_TRANS')
						elif DEBUG:
							print('>> REPLY_TRANS crc ×')
						if self._confirm_task:
							self._confirm_task.cancel()
							self._confirm_task = None
						self._loop.call_soon(self._write_ready)
			elif DEBUG:
				print('>> Unknown ×')
				print(address, action, data)

	@property
	def buffer_size(self):
		return self._buffer_size

	def get_write_buffer_size(self):
		"""The number of bytes in the write buffer.

		This buffer is unbounded, so the result may be larger than the
		the high water mark.
		"""
		return len(self._msgs)

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
		
	def write(self, data):
		try:
			self._serial.write(data)
		except serial.SerialException as exc:
			self._fatal_error(exc, 'Fatal write error on serial transport')
						
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
					if EOT in data:
						index = 0
						escaped = False
						while index < len(self._read_buffer):
							if self._read_buffer[index] == ESC[0]:
								escaped = not escaped
							else:
								if not escaped:
									if self._read_buffer[index] == EOT[0]:
										origin_data = self._read_buffer[:index]
										self._read_buffer = self._read_buffer[index+1:]
										index = -1
										if 2 <= len(origin_data) <= self._buffer_size + 2:
											self._on_read_data(origin_data)
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
			self._msgs.clear()
			self._confirm_task = None
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