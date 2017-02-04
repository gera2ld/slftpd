import asyncio
from . import ftpd
from .log import logger

class FTPServer:
    def __init__(self, config):
        self.config = config

    async def handle(self, reader, writer):
        handler = ftpd.FTPHandler(self.config, reader, writer)
        await handler.handle()

    async def serve(self):
        loop = asyncio.get_event_loop()
        self.server = await asyncio.start_server(self.handle, self.config.host, self.config.port)
        self.sockets = self.server.sockets

def serve(config):
    loop = asyncio.get_event_loop()
    server = FTPServer(config)
    loop.run_until_complete(server.serve())
    for sock in server.sockets:
        logger.info('Serving on %s, port %d', *sock.getsockname()[:2])
    loop.run_forever()
