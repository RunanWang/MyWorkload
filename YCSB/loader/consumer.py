import drivers
from utils.myLogger import getCMDLogger

# 从队列里取出并执行每一条SQL
def excuteOneInQueue(driver, name, queue, counter):
    logger = getCMDLogger()
    while True:
        try:
            # 取出一条
            sql = queue.get()
            # 执行
            driver.exec(sql)
            # 计数统计QPS
            with counter.get_lock():
                counter.value += 1
        except KeyboardInterrupt:
            logger.warn("Loader Consumer "+ name +" Terminated!!")
            # driver.close()
            break

