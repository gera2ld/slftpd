'''
FTP Server v2
Author: Gerald <i@gerald.top>
Requirements: Python 3.5+
RFC 959, 2389
'''
import asyncio, logging, traceback, time, os, socket, platform, stat
SERVER_NAME = 'SLFTPD/2'

def time_string(timestamp):
    time_obj = time.localtime(timestamp)
    now = time.localtime()
    if time_obj.tm_year == now.tm_year:
        return time.strftime('%b %d %H:%M', time_obj)
    else:
        return time.strftime('%b %d %Y', time_obj)

class FileProducer:
    def __init__(self, path, type, bufsize, offset=0):
        mode = 'r'
        if type == 'i': mode += 'b'
        self.bufsize = bufsize
        self.fp = open(path, mode)
        if offset: self.fp.seek(offset)

    def __iter__(self):
        return self

    def __next__(self):
        data = self.fp.read(self.bufsize)
        if data:
            return data
        else:
            self.fp.close()
            raise StopIteration

class Transporter:
    reader = None
    writer = None
    def __init__(self, config, context):
        self.config = config
        self.context = context
        self.bytes_sent = 0
        self.bytes_received = 0
        self.connected = asyncio.Future()

    def close(self):
        '''Flush data and close connection.'''
        if self.writer:
            self.writer.close()

    async def push(self, data):
        max_down = self.context.get('max_down')
        delta = self.config.buf_out / max_down if max_down else 0
        loop = asyncio.get_event_loop()
        for chunk in data:
            start_time = loop.time()
            try:
                self.writer.write(chunk)
            except:
                break
            await self.writer.drain()
            self.bytes_sent += len(chunk)
            sleep_time = delta - loop.time() + start_time
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    async def pull(self, fileobj, enc=None):
        max_up = self.context.get('max_up')
        delta = self.config.buf_in / max_up if max_up else 0
        loop = asyncio.get_event_loop()
        while True:
            start_time = loop.time()
            chunk = await asyncio.wait_for(
                self.reader.read(self.config.buf_in), self.config.data_timeout)
            if not chunk: break
            if enc:
                chunk = chunk.decode(enc, 'replace')
            fileobj.write(chunk)
            self.bytes_received += len(chunk)
            sleep_time = delta - loop.time() + start_time
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

class PSVTransporter(Transporter):
    def __init__(self, *k, **kw):
        super().__init__(*k, **kw)
        self.closed = True

    async def connect(self, host):
        self.port = await asyncio.wait_for(self.config.get_port(), 1)
        self.con = await asyncio.start_server(self.onconnect,
                host=host, port=self.port, backlog=1)
        self.closed = False

    def onconnect(self, reader, writer):
        self.connected.set_result(True)
        self.reader, self.writer = reader, writer
        asyncio.ensure_future(self.close_server())

    async def close_server(self):
        if not self.closed:
            self.con.close()
            self.closed = True
            await self.con.wait_closed()
            self.config.put_port(self.port)

class PRTTransporter(Transporter):
    async def connect(self, host, port):
        self.con = asyncio.open_connection(host=host, port=port)
        reader, writer = await asyncio.wait_for(self.con, 5)
        self.reader, self.writer = reader, writer
        self.connected.set_result(True)

def get_mlst_handlers():
    mlst_type = lambda itype, **kw: 'Type=' + itype
    mlst_size = lambda st, itype, **kw: 'Size=%d' % st.st_size if itype == 'file' else None
    mlst_perm = lambda perm, **kw: 'Perm=' + perm
    def mlst_modify(st, **kw):
        time_obj = time.localtime(st.st_mtime)
        return 'Modify=%04d%02d%02d%02d%02d%02d' % (
            time_obj.tm_year,
            time_obj.tm_mon,
            time_obj.tm_mday,
            time_obj.tm_hour,
            time_obj.tm_min,
            time_obj.tm_sec,
        )
    return {
        'type': mlst_type,
        'size': mlst_size,
        'perm': mlst_perm,
        'modify': mlst_modify,
    }

class FTPHandler:
    responses = {
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
    features = (
        'UTF8',
        'MLST Type*;Size*;Modify*;Perm*;',
    )
    mlst_facts_available = (
        'Type',
        'Size',
        'Modify',
        'Perm',
    )
    mlst_handlers = get_mlst_handlers()

    def __init__(self, config, reader, writer):
        self.config = config
        self.reader = reader
        self.writer = writer
        self.encoding = config.encoding
        self.user = None
        self.username = None
        self.directory = '/'
        self.context = {}
        self.mode = 's'
        self.type = 'i'
        self.stru = 'f'
        self.ret = None
        self.transporter = None
        self.remote_addr = writer.get_extra_info('peername')
        self.local_addr = writer.get_extra_info('sockname')
        self.set_mlst_facts()

    def set_mlst_facts(self, facts=None):
        if facts is None:
            facts = self.mlst_facts_available
        keys = []
        mlst_facts = self.mlst_facts = []
        for fact in facts:
            handle = self.mlst_handlers.get(fact.lower())
            if handle is not None:
                keys.append(fact)
                mlst_facts.append(handle)
        return keys

    def log_message(self, message, direction='>'):
        username = 'null' if self.user is None else self.user.name
        logging.info('%s@%s(%d) %s %s', username, self.remote_addr[0], self.connection_id, direction, message)

    def push_status(self, data):
        self.writer.write(data.encode(self.encoding))
        self.log_message(data.rstrip(), '<')

    def access(self, path=''):
        path = self.config.normpath(os.path.join(self.directory, path))
        while True:
            if path.startswith('../'):
                path = path[3:]
            else:
                break
        realpath, context = self.user.apply_rules(path)
        context['path'] = path
        context['realpath'] = realpath
        return context

    def list_dir(self, realpath):
        '''List directory entries.

        Each line looks like this:

        -rwxrwxrwx 1 user group 1024 Feb 4 2017 config.py
        '''
        dirs = {}
        files = {}
        for name in os.listdir(realpath):
            item = os.path.join(realpath,name)
            try:
                st = os.stat(item)
            except:
                continue
            line = '%s %d user group %d %s %s\n' % (
                    stat.filemode(st.st_mode), 1,
                    st.st_size, time_string(st.st_mtime), name)
            if os.path.isdir(item):
                dirs[name] = line
            else:
                files[name] = line
        res = ''.join(line for dic in (dirs, files) for line in dic.values())
        return res.encode(self.encoding, 'replace')

    def send_status(self, code, message=None, data=None):
        # data = first_line, data_lines
        if message is None:
            message = self.responses[code] if code in self.responses else ''
        if data:
            self.push_status('%d-%s\r\n' % (code, data[0]))
            for i in data[1]:
                self.push_status(' %s\r\n' % i)
        self.push_status('%d %s\r\n' % (code, message))

    def denied(self, perm, context=None):
        '''Check permission.'''
        if context is None:
            context = self.context
        if perm not in context['permission']:
            self.send_status(550, 'Permission denied.')
            return True

    permission_file = set('rwadf')
    permission_dir = set('eldfm')
    def get_info(self, itype=None, context=None):
        if context is None:
            context = self.context
        realpath = context['realpath']
        if os.path.isfile(realpath):
            itype = itype or 'file'
            perm = ''.join(permission_file.intersection(context['permission']))
        elif os.path.isdir(realpath):
            itype = itype or 'dir'
            assert itype in ('dir', 'cdir', 'pdir')
            perm = ''.join(permission_dir.intersection(context['permission']))
        else:
            self.send_status(550, 'File or directory not found.')
            return
        st = os.stat(realpath)
        info = []
        for handle in self.mlst_facts:
            res = handle(itype=itype, st=st, perm=perm)
            if res: info.append(res)
        info.append(' ' + path)
        return itype, ';'.join(info)

    def handle_close(self):
        self.writer.close()
        self.log_message('Connection closed.', '=')
        self.config.connections[None] -= 1
        self.config.connections[self.remote_addr[0]] -= 1

    async def handle(self):
        '''A coroutin to handle slow procedures.'''
        ip = self.remote_addr[0]
        config = self.config
        n = config.connections.get(ip, 0)
        config.connections[ip] = self.connection_id = n + 1
        config.connections[None] += 1
        if (config.max_connection and
                config.connections[None] > config.max_connection):
            self.send_status(421, '%d users (the maximum) logged in.' % config.max_connection)
            self.handle_close()
            return
        elif (self.user and self.user.max_connection and
                self.connection_id > self.user.max_connection):
            self.send_status(530, 'Number of connections per IP is limited.')
            self.handle_close()
            return
        else:
            self.send_status(220)
        while True:
            try:
                line = await asyncio.wait_for(
                        self.reader.readline(), config.control_timeout)
                line = line.strip().decode()
                cmd, _, args = line.partition(' ')
            except asyncio.TimeoutError:
                self.send_status(421, 'Control connection timed out.')
                self.writer.close()
                break
            if not cmd: break
            self.log_message(line)
            cmd = cmd.upper()
            if self.user is None and cmd not in ('USER', 'PASS', 'QUIT'):
                self.send_status(530)
                continue
            handle = getattr(self, 'ftp_' + cmd, None)
            if handle is None:
                self.send_status(502)
                continue
            try:
                ret = handle(args)
                if asyncio.iscoroutine(ret):
                    ret = await ret
                self.ret = ret
            except:
                traceback.print_exc()
                self.ret = None
                self.send_status(500)
        self.handle_close()

    async def handle_transporter(self, callback, *args):
        if self.transporter is None:
            self.send_status(500, 'Data connection must be open first.')
            return
        elif self.transporter.connected.done():
            self.send_status(125)
        else:
            self.send_status(150)
            try:
                await asyncio.wait_for(self.transporter.connected, 5)
            except asyncio.TimeoutError:
                self.send_status(421, 'Data connection time out.')
                return
        try:
            await callback(*args)
        except asyncio.TimeoutError:
            self.send_status(421, 'Data channel time out.')
        except:
            import traceback
            traceback.print_exc()
            self.send_status(426, 'Error occurred.')
        else:
            self.send_status(226, 'Transfer completed.')
        finally:
            self.transporter = None

    async def handle_push_data(self, data):
        '''Push data to client.

        data must be bytes or bytes generator'''
        if isinstance(data, bytes): data = [data]
        await self.transporter.push(data)
        self.transporter.close()

    async def push_data(self, data):
        '''Download from FTP server.'''
        await self.handle_transporter(self.handle_push_data, data)

    async def handle_pull_data(self, fileobj):
        '''Pull data from client.'''
        await self.transporter.pull(fileobj,
                self.encoding if self.type == 'a' else None)

    async def pull_data(self, fileobj):
        '''Upload to FTP server.'''
        await self.handle_transporter(self.handle_pull_data, fileobj)

    def ftp_USER(self, args):
        self.username = args.lower()
        user = self.config.users.get(self.username)
        if user is None:
            self.send_status(430)
        else:
            self.send_status(331, user.loginmsg)

    def ftp_PASS(self, args):
        if self.username is None:
            self.send_status(332)
            return
        user = self.config.users.get(self.username)
        if user:
            if not user.pwd or user.pwd == args:
                self.user = user
                self.send_status(230)
                return
        self.send_status(430)

    def ftp_QUIT(self, args):
        self.send_status(221)
        self.writer.close()

    def ftp_PWD(self, args):
        self.send_status(257, '"%s" is current directory.' % self.directory)

    def ftp_CWD(self, args):
        if self.directory == '/' and args == '..':
            self.send_status(550, '"/" has no parent directory.')
            return
        self.context = self.access(args)
        if self.denied('e'): return
        if not os.path.isdir(self.context['realpath']):
            self.send_status(550, 'Directory not found.')
        else:
            path = self.context['path']
            self.directory = path
            self.send_status(250, 'Directory changed to %s.' % path)

    def ftp_CDUP(self, args):
        self.ftp_CWD('..')

    def ftp_MODE(self, args):
        mode = args.lower()
        if mode == 's':
            self.send_status(200, 'Mode set to S.')
        else:
            self.send_status(504, 'Unsupported mode: %s.' % args)
            return
        self.mode = mode

    def ftp_TYPE(self, args):
        type_ = args.lower()
        if type_ == 'i':
            self.send_status(200, 'Type set to I.')
        elif type_ == 'a':
            self.send_status(200, 'Type set to A.')
        else:
            self.send_status(504, 'Unsupported type: %s.' % args)
            return
        self.type = type_

    def ftp_STRU(self, args):
        stru = args.lower()
        if stru == 'f':
            self.send_status(200, 'Structure set to F.')
        else:
            self.send_status(504, 'Unsupported structure: %s.' % args)
            return
        self.stru = stru

    async def ftp_PASV(self, args):
        '''Passive mode, only support IPv4.'''
        self.transporter = None
        try:
            self.transporter = PSVTransporter(self.config, self.context)
            await self.transporter.connect(self.config.host)
        except asyncio.TimeoutError:
            self.send_status(500)
        else:
            self.send_status(227, 'Entering Passive Mode (%s,%d,%d)' % (
                self.local_addr[0].replace('.', ','),
                self.transporter.port // 256,
                self.transporter.port % 256,
            ))

    async def ftp_PORT(self, args):
        '''Port mode, only support IPv4.'''
        self.transporter = None
        try:
            args = args.split(',')
            host = '.'.join(args[:4])
            port = (int(args[4]) << 8) + int(args[5])
            self.transporter = PRTTransporter(self.config, self.context)
            await self.transporter.connect(host, port)
        except asyncio.TimeoutError:
            self.send_status(421, 'Data channel time out.')
        else:
            self.send_status(200)

    async def ftp_LIST(self, args):
        if args[:3].strip() == '-a':
            args = args[3:].strip()
        self.context = self.access(args)
        if self.denied('l'): return
        realpath = self.context['realpath']
        if os.path.isfile(realpath):
            self.send_status(213)
        elif os.path.isdir(realpath):
            await self.push_data(self.list_dir(realpath))
        else:
            self.send_status(550, 'Directory not found.')

    def ftp_SIZE(self, args):
        self.context = self.access(args)
        realpath = self.context['realpath']
        if os.path.isfile(realpath):
            self.send_status(213, str(os.path.getsize(realpath)))
        else:
            self.send_status(501)

    def ftp_REST(self, args):
        try:
            pos = int(args)
            self.send_status(350, 'Restarting at %d. Send STOR or RETR to initiate transfer.' % pos)
        except:
            pos = 0
            self.send_status(501, 'REST requires a value greater than or equal to 0.')
        return pos

    async def ftp_RETR(self, args):
        self.context = self.access(args)
        if self.denied('r'): return
        realpath = self.context['realpath']
        if os.path.isfile(realpath):
            await self.push_data(
                    FileProducer(realpath, self.type, self.config.buf_out, self.ret))
        else:
            self.send_status(550)

    def ftp_FEAT(self, args):
        if self.features:
            self.send_status(211, 'END', ('Features supported:', self.features))
        else:
            self.send_status(211)

    def ftp_OPTS(self, args):
        sp, _, cmd = args.lower().partition(' ')
        if sp == 'utf8':
            if cmd == 'on':
                self.encoding = 'utf-8'
            elif cmd == 'off':
                self.encoding = self.config.encoding
            else:
                self.send_status(501)
                return
            self.send_status(200, 'UTF-8 turned %s.' % cmd)
        elif sp == 'mlst':
            facts = self.set_mlst_facts(cmd.strip().split(';'))
            self.send_status(200, 'MLST OPTS ' + ';'.join(facts) + ';')
        else:
            self.send_status(501)

    def ftp_SYST(self, args):
        self.send_status(215, 'UNIX ' + platform.system() + ' ' + SERVER_NAME)

    def ftp_NOOP(self, args):
        self.send_status(200)

    def ftp_RNFR(self, args):
        self.context = self.access(args)
        if self.denied('f'): return
        realpath = self.context['realpath']
        if not os.path.exists(realpath):
            self.send_status(550, 'No such file or directory.')
        elif self.context['path'] == '/':
            self.send_status(550, 'Can\'t rename root directory.')
        else:
            self.send_status(350, 'Ready for destination name.')
            return realpath

    def ftp_RNTO(self, args):
        if self.ret is None:
            self.send_status(503)
        else:
            self.context = self.access(args)
            if self.denied('f'): return
            try:
                os.rename(self.ret, self.context['realpath'])
                self.send_status(250, 'Renaming ok.')
            except:
                self.send_status(550)

    def ftp_MKD(self, args):
        self.context = self.access(args)
        if self.denied('m'): return
        try:
            os.mkdir(self.context['realpath'])
            self.send_status(257, '"%s" directory is created.' % args)
        except:
            self.send_status(550)

    def ftp_RMD(self, args):
        self.context = self.access(args)
        if self.denied('d'): return
        if self.context['path'] == '/':
            self.send_status(550, 'Can\'t remove root directory.')
        else:
            def remove_dir(top):
                for root, dirs, files in os.walk(top, False):
                    for item in files:
                        os.remove(os.path.join(root, item))
                    for item in dirs:
                        remove_dir(os.path.join(root, item))
                os.rmdir(top)
            try:
                remove_dir(self.context['realpath'])
                self.send_status(250, 'Directory removed.')
            except:
                self.send_status(550)

    async def ftp_STOR(self, args):
        self.context = self.access(args)
        if self.denied('w'): return
        mode = 'r+' if self.ret else 'w'
        if self.type == 'i': mode += 'b'
        with open(self.context['realpath'], mode) as fileobj:
            if self.ret:
                try:
                    fileobj.seek(self.ret)
                except:
                    self.send_status(501,
                            'Failed storing data at pos: %s' % self.ret)
            else:
                await self.pull_data(fileobj)

    async def ftp_APPE(self, args):
        self.context = self.access(args)
        if self.denied('a'): return
        mode = 'a'
        if self.type == 'i': mode += 'b'
        with open(self.context['realpath'], mode) as fileobj:
            await self.pull_data(fileobj)

    def ftp_DELE(self, args):
        self.context = self.access(args)
        if self.denied('d'): return
        try:
            os.remove(self.context['realpath'])
            self.send_status(250, 'File removed.')
        except:
            self.send_status(550)

    def ftp_MLST(self, args):
        self.context = self.access(args)
        if self.denied('l'): return
        itype, info = self.get_info()
        data = 'Listing %s: %s' % (itype, args), info
        self.send_status(250, 'End', data)

    async def ftp_MLSD(self, args):
        self.context = self.access(args)
        if self.denied('l'): return
        path = self.context['path']
        realpath = self.context['realpath']
        if not os.path.isdir(realpath):
            self.send_status(550, 'Directory not found.')
            return
        data = []
        # current dir
        itype, info = self.get_info('cdir')
        data.append(info)
        # parent dir
        parent = os.path.dirname(path)
        if parent != path:
            itype, info = self.get_info('pdir', self.access(parent))
            data.append(info)
        permission = context['permission']
        for item in os.listdir(realpath):
            fullpath = os.path.join(realpath, item)
            itype, info = self.get_info(context=dict(realpath=fullpath, permission=permission))
            data.append(info)
        data = '\n'.join(data).encode(self.encoding)
        await self.push_data(data)
