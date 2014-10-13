#!python
# coding=utf-8
import logging,asyncio,platform
from . import ftpd,ftpdconf

if __name__=='__main__':
	logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')
	logging.info('FTP Server v2/%s %s - by Gerald'
			% (platform.python_implementation(),platform.python_version()))
	loop=asyncio.get_event_loop()
	conf=ftpdconf.FTPConfig()
	coro=loop.create_server(ftpd.FTPHandler,conf.host,conf.port)
	server=loop.run_until_complete(coro)
	server.conf=conf
	logging.info('Serving on %s, port %d',*server.sockets[0].getsockname()[:2])
	loop.run_forever()
