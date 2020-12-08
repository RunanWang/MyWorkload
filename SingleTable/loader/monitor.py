import os
import time
import multiprocessing

import drivers
import loader.producer as producer
import loader.consumer as consumer
import config.config as constant
from utils.myLogger import getCMDLogger

# 寻找name对应的driver
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass

# 维护队列并监控生产者消费者
def monitor(name):
    logger = getCMDLogger()
    counter  = multiprocessing.Value('i',lock=True)
    queue = multiprocessing.Queue(100)
    record1 = []   # generator
    record2 = []   # executer

    # 寻找对应的数据库执行Driver
    driverClass = createDriverClass(name)
    driver = driverClass()
    cursor, conn = driver.getconn()
        
    record1 = producer.workload2generator(name, queue)

    for i in range(8):
        cname = "consumer" + str(i)
        process = multiprocessing.Process(target=consumer.excuteOneInQueue,args=(driver, cname, queue, counter))
        process.start()
        record2.append(process)
        logger.info("consumer "+ str(i) + " start!")

    cursor.close()
    conn.close()

    while True:
        try:
            logger.info("Queue Size Status: "+str(queue.qsize()))
            with counter.get_lock():
                logger.info("QPS: "+str(counter.value/20))
                counter.value = 0
            time.sleep(20)
        except KeyboardInterrupt:
            # logger.warn("KeyboardInterrupt!!")
            queue.close()
            for p in record2:  
                p.terminate()
                p.join()
            for p in record1:
                p.terminate()
                p.join()
                time.sleep(0.1)
            time.sleep(1)
            # driver.close()
            logger.warn("Loader Monitor Terminated!!")
            break
