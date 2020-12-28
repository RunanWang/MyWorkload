import logging


def get_file_logger():
    logger = logging.getLogger()
    logger.setLevel(level=logging.DEBUG)
    handler = logging.FileHandler("./result/log.log")
    # handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_cmd_logger():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()
    return logger
