import os
import time
import multiprocessing

import loader.producer as producer
import loader.consumer as consumer
import config.config as constant
from utils.myLogger import getCMDLogger

# 维护队列并监控生产者消费者
def monitor(name):
    logger = getCMDLogger()
    counter  = multiprocessing.Value('i',lock=True)
    queue = multiprocessing.Queue(100)
    record1 = []   # generator
    record2 = []   # executer
        
    record1 = producer.workload2generator(name, queue)

    for i in range(2):
        process = multiprocessing.Process(target=consumer.excuteOneInQueue,args=(name, queue, counter))
        process.start()
        record2.append(process)
        logger.info("consumer "+ str(i) + " start!")

    while True:
        try:
            logger.info("Queue Size Status: "+str(queue.qsize()))
            with counter.get_lock():
                logger.info("QPS: "+str(counter.value/20))
                counter.value = 0
            time.sleep(20)
        except KeyboardInterrupt:
            logger.warn("KeyboardInterrupt!!")
            for p in record1:  
                p.terminate()
            for p in record2:
                p.terminate()
            time.sleep(1)
            logger.warn("Finish!!")
            break
