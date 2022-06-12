import logging
from logging.config import fileConfig

try:
    fileConfig("logging.ini")
except:
    pass
finally:
    log = logging.getLogger()
