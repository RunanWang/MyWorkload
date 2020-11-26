import os
import multiprocessing
import time
import producer
import consumer
import logging
import probe
import constant

logging.basicConfig(level = logging.DEBUG,format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# 维护队列并监控生产者消费者
def monitor():
    # lock  = multiprocessing.Lock()
    queue = multiprocessing.Queue(100)
    control = multiprocessing.Queue(20)
    record1 = []   # generator
    record2 = []   # executer
    record3 = []   # probe
        
    process = multiprocessing.Process(target=producer.generateInsert,args=(queue,))
    process.start()
    record1.append(process)
    logger.info("producer.generateInsert start!")
        
    process = multiprocessing.Process(target=producer.generateUpdate,args=(queue,))
    process.start()
    record1.append(process)
    logger.info("producer.generateUpdate start!")

    process = multiprocessing.Process(target=producer.generateSimpleSearch,args=(queue,))
    process.start()
    record1.append(process)
    logger.info("producer.generateSimpleSearch start!")

    process = multiprocessing.Process(target=producer.generateComplexSearch,args=(queue,))
    process.start()
    record1.append(process)
    logger.info("producer.generateComplexSearch start!")

    for i in range(10):
        process = multiprocessing.Process(target=consumer.outputQ,args=(queue,))
        process.start()
        record2.append(process)
        logger.info("consumer "+ str(i) + " start!")
    
    for i in range(0, len(constant.PROBE_SQL_LIST)):
        process = multiprocessing.Process(target=probe.cronProbe,args=(i,))
        process.start()
        record3.append(process)
        logger.info("probe "+ str(i) + " start!")
        time.sleep(constant.PROBE_TIME_BETWEEN_SQL)


    while True:
        try:
            logger.info("Queue Size Status: "+str(queue.qsize()))
            time.sleep(20)
        except KeyboardInterrupt:
            logger.warn("KeyboardInterrupt!!")
            for p in record1:  
                p.terminate()
            for p in record2:
                p.terminate()
            for p in record3:
                p.terminate()
            time.sleep(1)
            logger.warn("Finish!!")
            break

if __name__ == "__main__":
    monitor()