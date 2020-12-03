import logging

def getFileLogger():
    logging.basicConfig(
        level = logging.DEBUG, 
        filename='../result/log.log', 
        format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
        )
    
    filehandler = logging.FileHandler('../result/log.log', encoding='utf-8')
    logging.getLogger().addHandler(filehandler)
    logger = logging.getLogger()
    return logger

def getCMDLogger():
    logging.basicConfig(
        level = logging.DEBUG,
        format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
        )
    logger = logging.getLogger()
    return logger