import generateSQL
import time
import config.config as constant
import signal
from utils.myLogger import getCMDLogger

logger = getCMDLogger()

def sigintHandler(signum, frame):
    logger.warn("generator terminate!")
    exit()

# 生产一条insert并插入队列中
def generateInsert(queue):
    count = 0
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        sql = generateSQL.insertOne()
        queue.put(sql)
        count = count + 1
        if count % 1000 == 0:
            logger.info("total insert num: " + str(count))
        time.sleep(constant.RATE_INSERT*constant.INTERNAL_SLEEP_TIME)

# 生产一条update并插入队列中
def generateUpdate(queue):
    count = 0
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        sql = generateSQL.updateByID()
        queue.put(sql)
        count = count + 1
        if count % 1000 == 0:
            logger.info("total update num: " + str(count))
        time.sleep(constant.RATE_UPDATE*constant.INTERNAL_SLEEP_TIME)

# 生产一条简单search并插入队列中
def generateSimpleSearch(queue):
    count = 0
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        # sql = generateSQL.searchCountPerCity()
        # queue.put(sql)
        # sql = generateSQL.searchF1()
        # queue.put(sql)
        # sql = generateSQL.searchF2()
        # queue.put(sql)
        sql = generateSQL.searchByID()
        queue.put(sql)
        # sql = generateSQL.searchCountPerCity()
        # queue.put(sql)
        sql = generateSQL.searchByID()
        queue.put(sql)
        sql = generateSQL.searchByID()
        queue.put(sql)
        sql = generateSQL.searchByID()
        queue.put(sql)
        sql = generateSQL.searchByID()
        queue.put(sql)
        count = count + 5
        if count % 100000 == 0:
            logger.info("total search num: " + str(count))
        time.sleep(constant.RATE_SIMPLE_SEARCH*constant.INTERNAL_SLEEP_TIME)

# 生产一条复杂search并插入队列中
def generateComplexSearch(queue):
    count = 0
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        sql = generateSQL.searchAll()
        queue.put(sql)
        count = count + 1
        if count % 3000 ==0:
            logger.info("total search num:" + str(count))
        time.sleep(constant.RATE_COMPLEX_SEARCH*constant.INTERNAL_SLEEP_TIME)