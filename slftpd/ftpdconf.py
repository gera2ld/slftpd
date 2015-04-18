#!python
# coding=utf-8
import logging,os,asyncio,configparser

class FTPUser:
	def __init__(self,name='anonymous',pwd='',
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
		src=os.path.normpath(src).replace('\\','/')
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
	buf_in=buf_out=0x1000
	def __init__(self, filename='ftpd.cfg'):
		self.connections={None:0}
		conf=[os.path.join(os.path.dirname(__file__),filename),os.path.expanduser('~/.gerald/'+filename)]
		cp=configparser.ConfigParser(default_section='server')
		cp.read(conf)
		self.encoding=cp.get('server','encoding',fallback='utf-8')
		self.host=cp.get('server','host',fallback='0.0.0.0')
		self.port=cp.getint('server','port',fallback=21)
		self.max_connection=cp.getint('server','max_connection',fallback=200)
		self.max_user_connection=cp.getint('server','max_user_connection',fallback=1)
		self.control_timeout=cp.getint('server','control_timeout',fallback=120)
		self.data_timeout=cp.getint('server','data_timeout',fallback=10)
		ports_start=cp.getint('server','ports_start',fallback=8030)
		ports_end=cp.getint('server','ports_end',fallback=8040)
		self.ports=asyncio.Queue()
		for i in range(ports_start,ports_end):
			self.ports.put_nowait(i)
		self.users={}
		fb_max_down=cp.getint('server','max_down',fallback=0)
		fb_max_up=cp.getint('server','max_up',fallback=0)
		for name in cp.sections():
			kw={
				'name':name,
				'pwd':cp.get(name,'password',fallback=''),
				'homedir':os.path.expanduser(cp.get(name,'homedir',fallback='~')),
				'perm':cp.get(name,'permission',fallback='elr'),
				'loginmsg':cp.get(name,'loginmsg',fallback=None),
				'max_down':cp.getint(name,'max_down',fallback=fb_max_down),
				'max_up':cp.getint(name,'max_up',fallback=fb_max_up),
			}
			self.users[name]=FTPUser(**kw)

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
