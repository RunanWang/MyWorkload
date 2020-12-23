import logging

def getCMDLogger():
    # logging.basicConfig(
    #     level = logging.DEBUG, 
    #     filename='./result/log.log', 
    #     format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
    #     )
    # logging.getLogger().addHandler(filehandler)
    # logger = logging.getLogger()
    
    
    logger = logging.getLogger()
    logger.setLevel(level = logging.DEBUG)
    handler = logging.FileHandler("./result/log.log")
    # handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

# def getCMDLogger():
#     logging.basicConfig(
#         level = logging.DEBUG,
#         format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
#         )
#     logger = logging.getLogger()
#     return logger