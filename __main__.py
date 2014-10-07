#!python
# coding=utf-8
import logging,asyncio,platform
from . import ftpd,ftpconf

if __name__=='__main__':
	logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s: %(message)s')
	logging.info('FTP Server v2/%s %s - by Gerald'
			% (platform.python_implementation(),platform.python_version()))
	loop=asyncio.get_event_loop()
	# TODO: add config file
	conf=ftpd.FTPHandler.conf=ftpconf.FTPConfig()
	conf.add_user(ftpconf.FTPUser(
		perm='elrwa',
		loginmsg='User ANONYMOUS okay, use email as password.',
		max_down=0,
		max_up=0,
	))
	conf.set_ports(8030,8040)
	coro=loop.create_server(ftpd.FTPHandler,conf.host,conf.port)
	server=loop.run_until_complete(coro)
	logging.info('Serving on %s, port %d',*server.sockets[0].getsockname()[:2])
	try:
		loop.run_forever()
	finally:
		server.close()
		loop.close()
