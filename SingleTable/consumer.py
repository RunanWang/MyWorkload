import multiprocessing
import pymysql
import logging
import signal

logging.basicConfig(level = logging.DEBUG,format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def sigintHandler(signum, frame):
    logger.warn("consumer terminate")
    # 需要最后做的事情
    # print("执行最后的清理工作。")
    exit()

def makeConnect():
    conn = pymysql.connect(host="127.0.0.1", port=36036, user="root", password="1", database="test", charset="utf8")
    return conn

# 从队列里取出并执行每一条SQL
def outputQ(queue):
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        # 取出一条
        sql = queue.get()
        # 执行
        conn = makeConnect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception:
            logger.warn(Exception)
            conn.rollback()
        conn.close()

