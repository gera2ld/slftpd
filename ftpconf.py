#!python
# coding=utf-8
import logging,os,asyncio

class FTPUser:
	'''
	perm
		can be all or some of 'elrwadfm'.
		 e for edit current directory.
		 l for list contents of current directory.
		 r for retrieving files from server.
		 w for writing (new) files to server.
		 a for appending data to files on server.
		 d for deleting files and directories on server.
		 f for renaming items on server.
		 m for making directories on server.
	'''
	def __init__(self,name='anonymous',pwd=None,
			homedir=os.path.expanduser('~'),
			perm='elr',loginmsg=None,
			max_down=0,max_up=0):
		self.name=name
		self.pwd=pwd
		self.homedir=os.path.normpath(homedir)
		self.perm=perm
		self.loginmsg=loginmsg
		self.max_down=max_down*1024
		self.max_up=max_up*1024
		self.alias=[]
		self.add_alias('/',self.homedir)
	def add_alias(self, src, dest, perm=None):
		src=os.path.normpath(src)
		for i in self.alias:
			if i[0]==src:
				logging.warning('Alias for %s already exists.',src)
				return
		if not os.path.isabs(src):
			logging.warning('Invalid source path: %s',src)
			return
		dest=os.path.abspath(os.path.join(self.homedir,os.path.expanduser(dest)))
		if not os.path.isdir(dest):
			logging.warning('Invalid destination path: %s',dest)
			return
		if perm is None: perm=self.perm
		self.alias.append((src,dest,perm))

class FTPConfig:
	encoding='utf-8'
	buf_in=buf_out=0x1000
	def __init__(self, host='0.0.0.0', port=21):
		self.host=host
		self.port=port
		self.users={}
		self.ports=asyncio.Queue()
		self.connections={None:0}
		self.lock=asyncio.Lock()
		self.max_connection=200
		self.max_user_connection=1
		self.control_timeout=120
		self.data_timeout=10
		self.features=['UTF8']
	def add_user(self, user):
		self.users[user.name]=user
	def set_ports(self, start, end):
		for i in range(start,end):
			self.ports.put_nowait(i)

class FileProducer:
	def __init__(self, path, type, bufsize, offset=0):
		mode='r'
		if type=='i': mode+='b'
		self.bufsize=bufsize
		self.fp=open(path,mode)
		if offset: self.fp.seek(offset)
	def __iter__(self):
		return self
	def __next__(self):
		data=self.fp.read(self.bufsize)
		if data:
			return data
		else:
			raise StopIteration
