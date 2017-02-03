import logging, platform
from .config import Config
from . import serve

logging.basicConfig(level=logging.INFO)
logging.info('FTP Server v2/%s %s - by Gerald'
        % (platform.python_implementation(), platform.python_version()))
config = Config()
config.add_anonymous_user(homedir='~')
serve(config)
