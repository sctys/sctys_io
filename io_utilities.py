import os
import logging


def set_logger(logger_path, logger_file_name, logger_level, logger_name):
    logger_file = os.path.join(logger_path, logger_file_name)
    logging.basicConfig(level=getattr(logging, logger_level),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(filename=logger_file), logging.StreamHandler()])
    logger = logging.getLogger(logger_name)
    return logger
