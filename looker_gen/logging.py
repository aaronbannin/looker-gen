import logging
from logging.config import fileConfig

fileConfig('logging.ini')
log = logging.getLogger()
