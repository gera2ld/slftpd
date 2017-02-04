import logging, platform
from . import __version__
from .config import Config
from .server import serve
from .log import logger

import argparse, sys
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
ch.setFormatter(fmt)
logger.addHandler(ch)

parser = argparse.ArgumentParser(description='FTP server by Gerald.')
parser.add_argument('-p', '--port', default=8021, help='the port for the server to bind')
parser.add_argument('-H', '--homedir', default='.', help='the home directory of anonymous user')
args = parser.parse_args()

logger.info('FTP Server v%s/%s %s - by Gerald'
        % (__version__, platform.python_implementation(), platform.python_version()))
config = Config()
config.port = args.port
config.add_anonymous_user(homedir=args.homedir)
serve(config)
