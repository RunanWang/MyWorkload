import signal
import drivers
from utils.myLogger import getCMDLogger

# 寻找name对应的driver
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass

# 收到ctrl-c信号时优雅退出
def sigintHandler(signum, frame):
    getCMDLogger().info("consumer terminate")
    exit()

# 从队列里取出并执行每一条SQL
def excuteOneInQueue(name, queue, counter):
    # 寻找对应的数据库执行Driver
    driverClass = createDriverClass(name)
    driver = driverClass()

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

