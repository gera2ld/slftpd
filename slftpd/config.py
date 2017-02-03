import logging, os, asyncio, configparser

def _normpath(path):
    return os.path.normpath(path).replace('\\', '/')

class DirRule:
    '''A rule to find the real path and permissions of a directory.

    - attrs MUST be an iterable object, which may iter following attributes:
      - permission {String} composed of `elrwadfm`
        - e for editing current directory.
        - l for listing contents of current directory.
        - r for retrieving files from server.
        - w for writing (new) files to server.
        - a for appending data to files on server.
        - d for deleting files and directories on server.
        - f for renaming items on server.
        - m for making directories on server.
      - max_down {Integer} Number of bytes
      - max_up {Integer} Number of bytes
    '''
    def __init__(self, src, dest, attrs=()):
        if not src.endswith('/'): src += '/'
        if not dest.endswith('/'): dest += '/'
        self.src = src
        self.dest = os.path.expanduser(dest)
        self.attrs = attrs

class FTPUser:
    def __init__(self, name='anonymous', pwd='',
            homedir='.', attrs=(),
            loginmsg=None, max_connection=1):
        self.name = name
        self.pwd = pwd
        self.homedir = self.normpath(homedir)
        self.loginmsg = loginmsg
        self.max_connection = max_connection
        self.rules = [DirRule('/', homedir, attrs)]

    def normpath(self, path):
        return _normpath(os.path.expanduser(path))

    def add_rule(self, src, dest, **kw):
        src = self.normpath(src)
        dest = self.normpath(dest)
        rule = DirRule(src, dest, kw)
        self.rules.append(rule)

    def apply_rules(self, path):
        context = {}
        realpath = None
        for rule in self.rules:
            if path.startswith(rule.src):
                context.update(rule.attrs)
                realpath = os.path.join(rule.dest, os.path.relpath(path, rule.src))
        return realpath, context

class Config:
    buf_in = buf_out = 0x1000
    encoding = 'utf-8'
    host = '0.0.0.0'
    port = 8021
    max_connection = 200
    max_user_connection = 1
    control_timeout = 120
    data_timeout = 10
    ports = None
    default_attrs = (
        ('permission', 'elr'),
        ('max_down', 0),
        ('max_up', 0),
    )

    def __init__(self):
        self.connections = {None: 0}
        self.users = {}

    def set_ports(self, ports_start=8030, ports_end=8040):
        if self.ports is not None:
            logging.warn('Ports already initialized!')
            return
        self.ports = asyncio.Queue()
        for i in range(ports_start, ports_end):
            self.ports.put_nowait(i)

    def add_user(self, name, **kw):
        kw.setdefault('attrs', self.default_attrs)
        kw.setdefault('max_connection', self.max_user_connection)
        user = FTPUser(name, **kw)
        if name in self.users:
            logging.warn('User [%s] already exists and is replaced by the new entry!', name)
        self.users[name] = user
        return user

    def add_anonymous_user(self, **kw):
        kw.setdefault('loginmsg', 'User ANONYMOUS okay, use email as password.')
        return self.add_user('anonymous', **kw)

    def get_port(self):
        if self.ports is None: self.set_ports()
        return self.ports.get()

    def put_port(self, port):
        self.ports.put_nowait(port)

    def normpath(self, path):
        return _normpath(path)
