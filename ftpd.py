#!python
# coding=utf-8
# FTP Server v2
# Author: Gerald <gera2ld@163.com>
# Require: Python 3.4+
# RFC 959, 2389
import asyncio,logging,traceback,time,os,socket,sys
from tarfile import filemode
from . import ftpconf

class Transporter:
	reader=None
	writer=None
	bufsize=0x1000
	def __init__(self, conf, user):
		self.conf=conf
		self.user=user
		self.bytes_sent=0
		self.bytes_received=0
		self.connected=asyncio.Future()
	def close(self):
		'''
		Flush data and close connection.
		'''
		if self.writer:
			self.writer.close()
	@asyncio.coroutine
	def push(self, data):
		# TODO: limit speed
		if isinstance(data, bytes):
			self.writer.write(data)
			yield from self.writer.drain()
			self.bytes_sent+=len(data)
		else:
			for chunk in data:
				self.writer.write(chunk)
				yield from self.writer.drain()
				self.bytes_sent+=len(chunk)
	@asyncio.coroutine
	def pull(self):
		# TODO: limit speed
		chunk=yield from asyncio.wait_for(
				self.reader.read(self.bufsize),self.conf.data_timeout)
		self.bytes_received+=len(chunk)
		return chunk

class PSVTransporter(Transporter):
	def __init__(self, conf, user):
		super().__init__(conf, user)
		self.closed=True
	def connect(self, host):
		self.port=yield from asyncio.wait_for(self.conf.ports.get(), 1)
		self.con=yield from asyncio.start_server(self.onconnect,
				host=host, port=self.port, backlog=1)
		self.closed=False
	def onconnect(self, reader, writer):
		self.connected.set_result(True)
		self.reader=reader
		self.writer=writer
		asyncio.async(self.close_server())
	@asyncio.coroutine
	def close_server(self):
		if not self.closed:
			self.con.close()
			self.closed=True
			yield from self.con.wait_closed()
			self.conf.ports.put_nowait(self.port)

class PRTTransporter(Transporter):
	def __init__(self, conf, user):
		super().__init__(conf, user)
	def connect(self, host, port):
		self.con=asyncio.open_connection(host=host, port=port)
		reader,writer=yield from asyncio.wait_for(self.con, 5)
		self.reader=reader
		self.writer=writer
		self.connected.set_result(True)

class FTPHandler(asyncio.Protocol):
	conf=None
	responses={
		125: 'Data connection already open; transfer starting.',
		150: 'File status okay; about to open data connection.',
		200: 'Command okay.',
		211: 'No features supported.',
		213: 'File status.',
		215: 'System Info',
		220: 'Welcome to Gerald\'s FTP server.',
		221: 'Goodbye.',
		226: 'Closing data connection.',
		227: 'Entering Passive Mode (h1,h2,h3,h4,p1,p2)',
		229: 'Entering Extended Passive Mode (|||port|)',
		230: 'User logged in, proceed.',
		250: 'Requested file action okay, completed.',
		257: '"PATHNAME" created.',
		331: 'User name okay, need password.',
		332: 'Need account for login.',
		350: 'Requested file action pending further information.',
		421: 'Service not available, closing control connection.',
		425: 'Can\'t open data connection.',
		426: 'Connection closed; transfer aborted.',
		430: 'Invalid username or password.',
		500: 'Syntax error, command unrecognized.',
		501: 'Syntax error in parameters or arguments.',
		502: 'Command not implemented.',
		503: 'Bad sequence of commands.',
		504: 'Command not implemented for that parameter.',
		522: 'Network protocol not supported, use (1)',
		530: 'Not logged in.',
		550: 'Requested action not taken.',
		}
	def connection_made(self, transport):
		self.transport=transport
		self.encoding=self.conf.encoding
		self.rbuf=[]
		self.user=None
		self.username=None
		self.directory='/'
		self.mode='s'
		self.type='i'
		self.stru='f'
		self.ret=None
		self.transporter=None
		self.remote_addr=transport.get_extra_info('peername')
		self.local_addr=transport.get_extra_info('sockname')
		self.queue=asyncio.Queue()
		self.closed=asyncio.Future()
		asyncio.async(self.handle())
	def data_received(self, data):
		self.rbuf.append(data)
		if data.find(b'\n')<0: return
		line,_,buf=b''.join(self.rbuf).partition(b'\n')
		self.rbuf=[]
		if buf: self.rbuf.append(buf)
		line=line.decode(self.encoding)
		if line and line[-1]=='\r': line=line[:-1]
		cmd,_,args=line.partition(' ')
		cmd=cmd.upper()
		self.queue.put_nowait((cmd,args))
		#self.transport.pause_reading()
	def connection_lost(self, exc):
		self.closed.set_result(True)
		self.queue.put_nowait((None,None))
	def log_message(self, message, direction='>'):
		user='null' if self.user is None else self.user.name
		logging.info('%s@%s(%d) %s %s',user,self.remote_addr[0],self.connection_id,direction,message)
	def push_status(self, data):
		self.transport.write(data.encode(self.encoding))
		self.log_message(data.strip(),'<')
	def send_status(self, code, message=None, data=None):
		# data = first_line, data_lines
		if message is None:
			message=self.responses[code] if code in self.responses else ''
		if data:
			self.push_status('%d-%s\r\n' % (code,data[0]))
			for i in data[1]:
				self.push_status(' %s\r\n' % i)
		self.push_status('%d %s\r\n' % (code,message))
	def real_path(self, path=''):
		path=os.path.join(self.directory, path)
		path=os.path.normpath(path).replace('\\','/').lstrip('./')
		realpath=os.path.join(self.user.homedir,path)
		path='/'+path
		perm=self.user.perm
		for i,j,k in self.user.alias:
			if path.startswith(i):
				realpath=os.path.join(j,os.path.relpath(path,i))
				perm=k
		return path,realpath,perm
	def format_time(self,t):
		t=time.localtime(t)
		now=time.localtime()
		if t.tm_year==now.tm_year:
			return time.strftime('%b %d %H:%M',t)
		else:
			return time.strftime('%b %d %Y',t)
	def list_dir(self, path):
		dirs={}
		files={}
		for i in os.listdir(path):
			f=os.path.join(path,i)
			try:
				st=list(os.stat(f))
			except:
				continue
			s='%s 1 user group %d %s %s\n' % (filemode(st[0]),st[6],self.format_time(st[8]),i)
			if os.path.isdir(f): dirs[i]=s
			else: files[i]=s
		wd=os.path.normpath(self.directory).replace('\\','/')
		# TODO: add alias
		d=''.join(list(dirs.values())+list(files.values()))
		return d
	def denied(self, perm):
		'''
		Check permission.
		Note: self.perm is not always self.user.perm
		'''
		if self.perm.find(perm)<0:
			self.send_status(550, 'Permission denied.')
			return True
		return False

	@asyncio.coroutine
	def handle_close(self):
		self.transport.close()
		try:
			yield from asyncio.wait_for(self.closed,1)
		except asyncio.TimeoutError as e:
			logging.error('Failed closing transport!')
			raise e
		self.log_message('Connection closed.','=')
		with (yield from self.conf.lock):
			self.conf.connections[None]-=1
			self.conf.connections[self.remote_addr[0]]-=1
	@asyncio.coroutine
	def handle(self):
		'''
		A coroutin to handle slow procedures.
		'''
		with (yield from self.conf.lock):
			ip=self.remote_addr[0]
			n=self.conf.connections.get(ip,0)
			self.conf.connections[ip]=self.connection_id=n+1
			self.conf.connections[None]+=1
		if (self.conf.max_connection and
				self.conf.connections[None]>self.conf.max_connection):
			self.send_status(421, '%d users (the maximum) logged in.' % self.conf.max_connection)
			yield from self.handle_close()
			return
		elif (self.conf.max_user_connection and
				self.connection_id>self.conf.max_user_connection):
			self.send_status(530, 'Number of connections per IP is limited.')
			yield from self.handle_close()
			return
		else:
			self.send_status(220)
		while True:
			try:
				cmd,args=yield from asyncio.wait_for(
						self.queue.get(), self.conf.control_timeout)
			except asyncio.TimeoutError:
				self.send_status(421, 'Control connection timed out.')
				self.transport.close()
				break
			if cmd is None:	# connection closed
				break
			self.log_message(cmd+' '+args)
			if self.user is None and cmd not in ('USER','PASS','QUIT'):
				self.send_status(530)
			else:
				handle=getattr(self, 'ftp_'+cmd, None)
				if handle is None:
					self.send_status(502)
				else:
					try:
						self.ret=yield from handle(args)
					except:
						traceback.print_exc()
						self.ret=None
						self.send_status(500)
		yield from self.handle_close()
	@asyncio.coroutine
	def handle_transporter(self, callback, *args):
		if self.transporter is None:
			self.send_status(500, 'Data connection must be open first.')
			return
		elif self.transporter.connected.done():
			self.send_status(125)
		else:
			self.send_status(150)
			try:
				yield from asyncio.wait_for(self.transporter.connected,5)
			except asyncio.TimeoutError:
				self.send_status(421, 'Data connection time out.')
				return
		try:
			yield from callback(*args)
		except asyncio.TimeoutError:
			self.send_status(421, 'Data channel time out.')
		except socket.error:
			self.send_status(426, 'Socket error.')
		else:
			self.send_status(226, 'Transfer completed.')
		finally:
			self.transporter=None
	@asyncio.coroutine
	def handle_push_data(self, data):
		#if self.type=='a':
		if isinstance(data, str):	# in case sent by LIST
			data=data.encode(self.encoding,'replace')
		yield from self.transporter.push(data)
		self.transporter.close()
	@asyncio.coroutine
	def push_data(self, data):
		'''
		Download from FTP server.
		'''
		yield from self.handle_transporter(self.handle_push_data, data)
	@asyncio.coroutine
	def handle_pull_data(self, fileobj):
		while True:
			chunk=yield from self.transporter.pull()
			if not chunk: break
			if self.type=='a':
				# FIXME: broken between chunks
				chunk=chunk.decode(self.encoding,'replace')
			fileobj.write(chunk)
		fileobj.close()
	@asyncio.coroutine
	def pull_data(self, fileobj):
		'''
		Upload to FTP server.
		'''
		yield from self.handle_transporter(self.handle_pull_data, fileobj)
	@asyncio.coroutine
	def ftp_USER(self, args):
		self.username=args.lower()
		if self.username in self.conf.users:
			o=self.conf.users[self.username]
			self.send_status(331,o.loginmsg)
		else:
			self.send_status(430)
	@asyncio.coroutine
	def ftp_PASS(self, args):
		if self.username is None:
			self.send_status(332)
			return
		if self.username in self.conf.users:
			o=self.conf.users[self.username]
			if o.pwd is None or o.pwd==args:
				self.user=o
				self.perm=o.perm
				self.send_status(230)
				return
		self.send_status(430)
	@asyncio.coroutine
	def ftp_QUIT(self, args):
		self.send_status(221)
		self.transport.close()
	@asyncio.coroutine
	def ftp_PWD(self, args):
		self.send_status(257, '"%s" is current directory.' % self.directory)
	@asyncio.coroutine
	def ftp_CWD(self, args):
		if self.denied('e'): return
		if not args:
			path,realpath,perm=self.real_path(args)
		else:
			if self.directory=='/' and args=='..':
				self.send_status(550, '"/" has no parent directory.')
				return
			path,realpath,perm=self.real_path(args)
		if not os.path.isdir(realpath):
			self.send_status(550, 'Directory not found.')
		else:
			self.directory=path
			self.perm=perm
			self.send_status(250, 'Directory changed to %s.' % self.directory)
	@asyncio.coroutine
	def ftp_CDUP(self, args):
		yield from self.ftp_CWD('..')
	@asyncio.coroutine
	def ftp_MODE(self, args):
		mode=args.lower()
		if mode=='s':
			self.send_status(200, 'Mode set to S.')
		else:
			self.send_status(504, 'Unsupported mode: %s.' % args)
			return
		self.mode=mode
	@asyncio.coroutine
	def ftp_TYPE(self, args):
		t=args.lower()
		if t=='i':
			self.send_status(200, 'Type set to I.')
		elif t=='a':
			self.send_status(200, 'Type set to A.')
		else:
			self.send_status(504, 'Unsupported type: %s.' % args)
			return
		self.type=t
	@asyncio.coroutine
	def ftp_STRU(self, args):
		stru=args.lower()
		if stru=='f':
			self.send_status(200, 'Structure set to F.')
		else:
			self.send_status(504, 'Unsupported structure: %s.' % args)
			return
		self.stru=stru
	@asyncio.coroutine
	def ftp_PASV(self, args):
		'''
		Passive mode, only support IPv4.
		'''
		self.transporter=None
		try:
			self.transporter=PSVTransporter(self.conf,self.user)
			yield from self.transporter.connect(self.conf.host)
		except asyncio.TimeoutError:
			self.send_status(500)
		else:
			self.send_status(227, 'Entering Passive Mode (%s,%d,%d)' % (
				self.local_addr[0].replace('.',','),
				self.transporter.port//256,
				self.transporter.port%256
			))
	@asyncio.coroutine
	def ftp_PORT(self, args):
		'''
		Port mode, only support IPv4.
		'''
		self.transporter=None
		try:
			args=args.split(',')
			host='.'.join(args[:4])
			port=int(args[4])*256+int(args[5])
			self.transporter=PRTTransporter(self.conf,self.user)
			yield from self.transporter.connect(host,port)
		except asyncio.TimeoutError:
			self.send_status(421, 'Data channel time out.')
		else:
			self.send_status(200)
	@asyncio.coroutine
	def ftp_LIST(self, args):
		if self.denied('l'): return
		if args[:3].strip()=='-a':
			args=args[3:].strip()
		if not args:
			path,realpath,perm=self.real_path()
		else:
			path,realpath,perm=self.real_path(args)
		if os.path.isfile(realpath):
			self.send_status(213)
		elif os.path.isdir(realpath):
			yield from self.push_data(self.list_dir(realpath))
		else:
			self.send_status(550, 'Directory not found.')
	@asyncio.coroutine
	def ftp_SIZE(self, args):
		path,realpath,perm=self.real_path(args)
		if os.path.isfile(realpath):
			self.send_status(213, str(os.path.getsize(realpath)))
		else:
			self.send_status(501)
	@asyncio.coroutine
	def ftp_REST(self, args):
		try:
			pos=int(args)
			self.send_status(350, 'Restarting at %d. Send STOR or RETR to initiate transfer.' % pos)
		except:
			pos=0
			self.send_status(501, 'REST requires a value greater than or equal to 0.')
		return pos
	@asyncio.coroutine
	def ftp_RETR(self, args):
		if self.denied('r'): return
		path,realpath,perm=self.real_path(args)
		if os.path.isfile(realpath):
			yield from self.push_data(
					ftpconf.FileProducer(realpath,self.type,self.ret))
		else:
			self.send_status(550)
	@asyncio.coroutine
	def ftp_FEAT(self, args):
		if self.conf.features:
			self.send_status(211,'END',
					('Features supported:',self.conf.features))
		else:
			self.send_status(211)
	@asyncio.coroutine
	def ftp_OPTS(self, args):
		sp,_,cmd=args.lower().partition(' ')
		if sp=='utf8':
			if cmd=='on': self.encoding='utf-8'
			elif cmd=='off': self.encoding=self.conf.encoding
			else:
				self.send_status(501)
				return
			self.send_status(200, 'UTF-8 turned %s.' % cmd)
		else: self.send_status(501)
	@asyncio.coroutine
	def ftp_SYST(self, args):
		if sys.platform.startswith('win'): name='WINDOWS-NT-6.1'
		else: name='UNKNOWN'
		self.send_status(215,'%s FTP/Gerald' % name)
	@asyncio.coroutine
	def ftp_NOOP(self, args):
		self.send_status(200)
	@asyncio.coroutine
	def ftp_RNFR(self, args):
		if self.denied('f'): return
		path,realpath,perm=self.real_path(args)
		if not os.path.exists(realpath):
			self.send_status(550, 'No such file or directory.')
		elif path=='/':
			self.send_status(550, 'Can\'t rename root directory.')
		else:
			self.send_status(350, 'Ready for destination name.')
			return realpath
	@asyncio.coroutine
	def ftp_RNTO(self, args):
		if self.denied('f'): return
		if self.ret is None:
			self.send_status(503)
		else:
			path,realpath,perm=self.real_path(args)
			try:
				os.rename(self.ret,realpath)
				self.send_status(250, 'Renaming ok.')
			except:
				self.send_status(550)
	@asyncio.coroutine
	def ftp_MKD(self, args):
		if self.denied('m'): return
		path,realpath,perm=self.real_path(args)
		try:
			os.mkdir(realpath)
			self.send_status(257, '"%s" directory is created.' % args)
		except:
			self.send_status(550)
	@asyncio.coroutine
	def ftp_RMD(self, args):
		if self.denied('d'): return
		path,realpath,perm=self.real_path(args)
		if f=='/':
			self.send_status(550, 'Can\'t remove root directory.')
		else:
			def remove_dir(self,top):
				for root,dirs,files in os.walk(top,False):
					for i in files: os.remove(os.path.join(root,i))
					for i in dirs: remove_dir(os.path.join(root,i))
				os.rmdir(top)
			try:
				remove_dir(realpath)
				self.send_status(250, 'Directory removed.')
			except:
				self.send_status(550)
	@asyncio.coroutine
	def ftp_STOR(self, args):
		if self.denied('w'): return
		path,realpath,perm=self.real_path(args)
		mode='r+' if self.ret else 'w'
		if self.type=='i': mode+='b'
		fileobj=open(realpath, mode)
		if self.ret:
			try:
				fileobj.seek(self.ret)
			except:
				self.send_status(501,
						'Failed storing data at pos: %s' % self.ret)
		else:
			yield from self.pull_data(fileobj)
	@asyncio.coroutine
	def ftp_APPE(self, args):
		if self.denied('a'): return
		path,realpath,perm=self.real_path(args)
		mode='a'
		if self.type=='i': mode+='b'
		fileobj=open(realpath, mode)
		yield from self.pull_data(fileobj)
	@asyncio.coroutine
	def ftp_DELE(self, args):
		if self.denied('d'): return
		path,realpath,perm=self.real_path(args)
		try:
			os.remove(realpath)
			self.send_status(250, 'File removed.')
		except:
			self.send_status(550)
