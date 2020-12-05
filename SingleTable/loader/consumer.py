import signal
import drivers
from utils.myLogger import getCMDLogger



# 收到ctrl-c信号时优雅退出
def sigintHandler(signum, frame):
    getCMDLogger().info("consumer terminate")
    exit()

# 从队列里取出并执行每一条SQL
def excuteOneInQueue(driver, name, queue, counter):
    # 注册信号接收hook
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        # 取出一条
        sql = queue.get()
        # 执行
        driver.exec(sql)
        # 计数统计QPS
        with counter.get_lock():
            counter.value += 1

