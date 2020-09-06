import os

NOTIFIER_PATH = os.environ['SCTYS_PROJECT'] + '/sctys_notify'
TEMP_PATH = os.environ['SCTYS_PROJECT'] + '/tmp'


class FileIOSetting(object):
    FILEIO_LOGGER_PATH = os.environ['SCTYS_PROJECT'] + '/Log/log_sctys_io/'
    FILEIO_LOGGER_FILE = 'file_io.log'
    FILEIO_LOGGER_LEVEL = 'DEBUG'
    FILEIO_NO_WORKERS = 100
    FILEIO_NOTIFIER = 'slack'
