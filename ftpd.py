#!python
# coding=utf-8
# FTP Server v2
# Author: Gerald <gera2ld@163.com>
# Require: Python 3.4+
# RFC 959, 2389
import asyncio,logging,traceback,time,os,socket,platform
from tarfile import filemode
from . import ftpdconf
SERVER_NAME='FTPD/Gerald'

class Transporter:
	reader=None
	writer=None
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
		delta=(self.conf.buf_out/self.user.max_down
				if self.user.max_down else 0)
		loop=asyncio.get_event_loop()
		for chunk in data:
			t=loop.time()
			try: self.writer.write(chunk)
			except: break
			yield from self.writer.drain()
			self.bytes_sent+=len(chunk)
			dt=delta-loop.time()+t
			if dt>0: yield from asyncio.sleep(dt)
	@asyncio.coroutine
	def pull(self, fileobj, enc=None):
		delta=(self.conf.buf_in/self.user.max_up
				if self.user.max_up else 0)
		loop=asyncio.get_event_loop()
		while True:
			t=loop.time()
			chunk=yield from asyncio.wait_for(
				self.reader.read(self.conf.buf_in),self.conf.data_timeout)
			if not chunk: break
			if enc:
				chunk=chunk.decode(enc,'replace')
			fileobj.write(chunk)
			self.bytes_received+=len(chunk)
			dt=delta-loop.time()+t
			if dt>0: yield from asyncio.sleep(dt)

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

class FTPHandler:
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
	conf=None
	features=[
		'UTF8',
		'MLST Type*;Size*;Modify*;Perm*;',
	]
	mlst_facts_available=['Type','Size','Modify','Perm']
	mlst_facts_dict=dict(map(lambda x:(x.lower(),x),mlst_facts_available))
	def __init__(self, reader, writer):
		self.reader=reader
		self.writer=writer
		self.conf=writer.transport._server.conf
		self.encoding=self.conf.encoding
		self.user=None
		self.username=None
		self.directory='/'
		self.mode='s'
		self.type='i'
		self.stru='f'
		self.mlst_facts=list(self.mlst_facts_available)
		self.ret=None
		self.transporter=None
		self.remote_addr=writer.get_extra_info('peername')
		self.local_addr=writer.get_extra_info('sockname')
		asyncio.async(self.handle())
	def log_message(self, message, direction='>'):
		user='null' if self.user is None else self.user.name
		logging.info('%s@%s(%d) %s %s',user,self.remote_addr[0],self.connection_id,direction,message)
	def push_status(self, data):
		self.writer.write(data.encode(self.encoding))
		self.log_message(data.rstrip(),'<')
	def real_path(self, path=None):
		path=os.path.join(self.directory, path or '')
		path=os.path.normpath(path).replace('\\','/')
		while True:
			if path.startswith('../'):
				path=path[3:]
			elif path.startswith('/'):
				path=path[1:]
			else:
				break
		realpath=os.path.join(self.user.homedir,path)
		path='/'+path
		perm=self.user.perm
		for i,j,k in self.user.alias:
			if path.startswith(i):
				realpath=os.path.join(j,os.path.relpath(path,i))
				if k: perm=k
		return path,realpath,perm
	def time_string_for_list(self,t):
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
			st=os.stat(f)
			s='%s 1 user group %d %s %s\n' % (filemode(st.st_mode),st.st_size,self.time_string_for_list(st.st_mtime),i)
			if os.path.isdir(f): dirs[i]=s
			else: files[i]=s
		wd=os.path.normpath(self.directory).replace('\\','/')
		# TODO: add alias
		d=''.join(list(dirs.values())+list(files.values()))
		return d.encode(self.encoding,'replace')
	def send_status(self, code, message=None, data=None):
		# data = first_line, data_lines
		if message is None:
			message=self.responses[code] if code in self.responses else ''
		if data:
			self.push_status('%d-%s\r\n' % (code,data[0]))
			for i in data[1]:
				self.push_status(' %s\r\n' % i)
		self.push_status('%d %s\r\n' % (code,message))
	def denied(self, perm, perms):
		'''
		Check permission.
		'''
		if perms.find(perm)<0:
			self.send_status(550, 'Permission denied.')
			return True
		return False
	def get_info(self, pathinfo, itype=None):
		info=[]
		if isinstance(pathinfo,str):
			path,realpath,perm=self.real_path(pathinfo)
		else:
			path,realpath,perm=pathinfo
		if os.path.isfile(realpath):
			if itype is None: itype='file'
			else: assert itype=='file'
			perms='rwadf'
		elif os.path.isdir(realpath):
			if itype is None: itype='dir'
			else: assert itype in ('dir','cdir','pdir')
			perms='eldfm'
		else:
			self.send_status(550, 'File or directory not found.')
			return
		st=os.stat(realpath)
		for i in self.mlst_facts:
			if i=='Type':
				info.append('Type='+itype)
			elif i=='Size':
				if itype=='file':
					info.append('Size=%d' % st.st_size)
			elif i=='Perm':
				p=[]
				for i in perms:
					if i in perm: p.append(i)
				info.append('Perm='+''.join(p))
			elif i=='Modify':
				t=time.localtime(st.st_mtime)
				info.append('Modify=%04d%02d%02d%02d%02d%02d'
						% (t.tm_year,t.tm_mon,t.tm_mday,t.tm_hour,t.tm_min,t.tm_sec))
		info.append(' '+path)
		return itype,';'.join(info)

	@asyncio.coroutine
	def handle_close(self):
		self.writer.close()
		self.log_message('Connection closed.','=')
		self.conf.connections[None]-=1
		self.conf.connections[self.remote_addr[0]]-=1
	@asyncio.coroutine
	def handle(self):
		'''
		A coroutin to handle slow procedures.
		'''
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
				line=yield from asyncio.wait_for(
						self.reader.readline(), self.conf.control_timeout)
				line=line.strip().decode()
				cmd,_,args=line.partition(' ')
			except asyncio.TimeoutError:
				self.send_status(421, 'Control connection timed out.')
				self.writer.close()
				break
			if not cmd: break
			self.log_message(line)
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
		except:
			self.send_status(426, 'Error occurred.')
		else:
			self.send_status(226, 'Transfer completed.')
		finally:
			self.transporter=None
	@asyncio.coroutine
	def handle_push_data(self, data):
		'''
		data must be bytes or bytes generator
		'''
		if isinstance(data, bytes): data=[data]
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
		yield from self.transporter.pull(fileobj,
				self.encoding if self.type=='a' else None)
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
			if not o.pwd or o.pwd==args:
				self.user=o
				self.send_status(230)
				return
		self.send_status(430)
	@asyncio.coroutine
	def ftp_QUIT(self, args):
		self.send_status(221)
		self.writer.close()
	@asyncio.coroutine
	def ftp_PWD(self, args):
		self.send_status(257, '"%s" is current directory.' % self.directory)
	@asyncio.coroutine
	def ftp_CWD(self, args):
		if not args:
			path,realpath,perm=self.real_path(args)
		else:
			if self.directory=='/' and args=='..':
				self.send_status(550, '"/" has no parent directory.')
				return
			path,realpath,perm=self.real_path(args)
		if self.denied('e',perm): return
		if not os.path.isdir(realpath):
			self.send_status(550, 'Directory not found.')
		else:
			self.directory=path
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
		if args[:3].strip()=='-a':
			args=args[3:].strip()
		path,realpath,perm=self.real_path(args)
		if self.denied('l',perm): return
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
		path,realpath,perm=self.real_path(args)
		if self.denied('r',perm): return
		if os.path.isfile(realpath):
			yield from self.push_data(
					ftpdconf.FileProducer(realpath,self.type,self.conf.buf_out,self.ret))
		else:
			self.send_status(550)
	@asyncio.coroutine
	def ftp_FEAT(self, args):
		if self.features:
			self.send_status(211,'END',
					('Features supported:',self.features))
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
		elif sp=='mlst':
			self.mlst_facts=[]
			data=['MLST OPTS ']
			for i in filter(None,cmd.strip().split(';')):
				k=self.mlst_facts_dict.get(i)
				if k:
					self.mlst_facts.append(k)
					data.append(k+';')
			self.send_status(200, ''.join(data))
		else:
			self.send_status(501)
	@asyncio.coroutine
	def ftp_SYST(self, args):
		self.send_status(215,platform.platform()+' '+SERVER_NAME)
	@asyncio.coroutine
	def ftp_NOOP(self, args):
		self.send_status(200)
	@asyncio.coroutine
	def ftp_RNFR(self, args):
		path,realpath,perm=self.real_path(args)
		if self.denied('f',perm): return
		if not os.path.exists(realpath):
			self.send_status(550, 'No such file or directory.')
		elif path=='/':
			self.send_status(550, 'Can\'t rename root directory.')
		else:
			self.send_status(350, 'Ready for destination name.')
			return realpath
	@asyncio.coroutine
	def ftp_RNTO(self, args):
		if self.ret is None:
			self.send_status(503)
		else:
			path,realpath,perm=self.real_path(args)
			if self.denied('f',perm): return
			try:
				os.rename(self.ret,realpath)
				self.send_status(250, 'Renaming ok.')
			except:
				self.send_status(550)
	@asyncio.coroutine
	def ftp_MKD(self, args):
		path,realpath,perm=self.real_path(args)
		if self.denied('m',perm): return
		try:
			os.mkdir(realpath)
			self.send_status(257, '"%s" directory is created.' % args)
		except:
			self.send_status(550)
	@asyncio.coroutine
	def ftp_RMD(self, args):
		path,realpath,perm=self.real_path(args)
		if self.denied('d',perm): return
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
		path,realpath,perm=self.real_path(args)
		if self.denied('w',perm): return
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
		path,realpath,perm=self.real_path(args)
		if self.denied('a',perm): return
		mode='a'
		if self.type=='i': mode+='b'
		fileobj=open(realpath, mode)
		yield from self.pull_data(fileobj)
	@asyncio.coroutine
	def ftp_DELE(self, args):
		path,realpath,perm=self.real_path(args)
		if self.denied('d',perm): return
		try:
			os.remove(realpath)
			self.send_status(250, 'File removed.')
		except:
			self.send_status(550)
	@asyncio.coroutine
	def ftp_MLST(self, args):
		path,realpath,perm=self.real_path(args)
		if self.denied('l',perm): return
		itype,info=self.get_info((path,realpath,perm))
		data=[]
		data.append('Listing %s: %s' % (itype,args))
		data.append(info)
		self.send_status(250,'End',data)
	@asyncio.coroutine
	def ftp_MLSD(self, args):
		path,realpath,perm=self.real_path(args)
		if self.denied('l',perm): return
		if not os.path.isdir(realpath):
			self.send_status(550, 'Directory not found.')
			return
		data=[]
		# current dir
		itype,info=self.get_info((path,realpath,perm),'cdir')
		data.append(info)
		# parent dir
		p=os.path.dirname(path)
		if p!=path:
			itype,info=self.get_info(p,'pdir')
			data.append(info)
		for i in os.listdir(realpath):
			r=os.path.join(realpath,i)
			itype,info=self.get_info((i,r,perm))
			data.append(info)
		data='\n'.join(data).encode(self.encoding)
		yield from self.push_data(data)
