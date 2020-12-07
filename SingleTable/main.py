import loader.monitor as monitor
import prober.mysqlprober as prober
import init
import time
import multiprocessing
import utils.myLogger

processList = []
logger = utils.myLogger.getCMDLogger()
init.init_db_table("mysql")
processM = multiprocessing.Process(target=monitor.monitor,args=("mysql",))
processM.start()
processP = multiprocessing.Process(target=prober.probe_monitor, args=())
processP.start()

processList.append(processP)
while True:
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        for p in processList:
            p.terminate()
            p.join()
            time.sleep(2)
        logger.warn("All Terminated, Bye!")
        break