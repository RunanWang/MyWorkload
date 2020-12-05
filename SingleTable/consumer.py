import multiprocessing
import logging
import signal
import utils.db_utils

def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('utils.db_utils.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass

def sigintHandler(signum, frame):
    logger.warn("consumer terminate")
    # 需要最后做的事情
    # print("执行最后的清理工作。")
    exit()

# 从队列里取出并执行每一条SQL
def outputQ(name, queue, counter):
    driverClass = createDriverClass(name)
    driver = driverClass()
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        # 取出一条
        sql = queue.get()
        # 执行
        try:
            driver.exec(sql)
        except Exception:
            pass
        # 计数统计QPS
        with counter.get_lock():
            counter.value += 1

