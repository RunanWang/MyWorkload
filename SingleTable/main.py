import loader.monitor as monitor
import prober.mysqlprober as prober
import init
import time
import multiprocessing
import config.config
import utils.myLogger

logger = utils.myLogger.getCMDLogger()
# init.init_db_table("mysql")
processM = multiprocessing.Process(target=monitor.monitor,args=("mysql",))
processM.start()
record = []
for i in range(len(config.config.PROBE_SQL_LIST)):
    process = multiprocessing.Process(target=prober.cronProbe,args=(i,))
    process.start()
    record.append(process)
    logger.info("probe "+ str(i) + " start!")
    time.sleep(config.config.PROBE_TIME_BETWEEN_SQL)


while True:
    try:
        pass
    except KeyboardInterrupt:
        processM.terminate()
        for p in record:
            p.terminate()
        time.sleep(1)
        logger.warn("Finish!!")