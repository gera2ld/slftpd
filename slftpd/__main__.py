#!python
# coding=utf-8
import logging,asyncio,platform
from . import ftpd
from .ftpdconf import config

if __name__=='__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info('FTP Server v2/%s %s - by Gerald'
			% (platform.python_implementation(),platform.python_version()))
	loop=asyncio.get_event_loop()
	coro=asyncio.start_server(ftpd.FTPHandler,config.host,config.port)
	server=loop.run_until_complete(coro)
	logging.info('Serving on %s, port %d',*server.sockets[0].getsockname()[:2])
	loop.run_forever()
