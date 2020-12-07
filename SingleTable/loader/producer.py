import time
import multiprocessing
import config.config
from utils.myLogger import getCMDLogger

logger = getCMDLogger()

# 寻找name对应的Workload
def createWorkloadClass(name):
    full_name = "%sWorkload" % name.title()
    mod = __import__('workload.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass

def producer(itemname, sqlGen, stime, queue):
    count = 0
    while True:
        try:
            sql = sqlGen()
            queue.put(sql)
            count = count + 1
            if count % 1000 == 0:
                logger.info("total "+ itemname +" excuted num: " + str(count))
            time.sleep(stime)
        except KeyboardInterrupt:
            logger.warn("Loader Producer "+ itemname +" Terminated!!")
            break

def workload2generator(name, queue):
    record = []   # generator
    workloadClass = createWorkloadClass(name)
    workload = workloadClass()
    for item, v in workload.get_workload().items():
        sqlGen = getattr(workload, item)
        stime = v * config.config.INTERNAL_SLEEP_TIME
        process = multiprocessing.Process(target=producer,args=(item, sqlGen, stime, queue,))
        process.start()
        record.append(process)
        logger.info("producer."+ item +" started!")
    return record
