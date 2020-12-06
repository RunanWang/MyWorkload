import loader.monitor as monitor
import prober.mysqlprober as prober
import init
import time
import multiprocessing
import utils.myLogger

logger = utils.myLogger.getCMDLogger()
# init.init_db_table("mysql")
# processM = multiprocessing.Process(target=monitor.monitor,args=("mysql",))
# processM.start()
processP = multiprocessing.Process(target=prober.probe_monitor, args=())
processP.start()

while True:
    try:
        pass
    except KeyboardInterrupt:
        # processM.terminate()
        processP.terminate()
        time.sleep(2)
        logger.warn("All Terminated!!")