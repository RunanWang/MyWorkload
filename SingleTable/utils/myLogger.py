import logging

def getMyLogger():
    logging.basicConfig(
        level = logging.DEBUG, 
        filename='../log.log', 
        format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
        )
    
    filehandler = logging.FileHandler('../log.log', encoding='utf-8')
    logging.getLogger().addHandler(filehandler)
    logger = logging.getLogger()
    return logger