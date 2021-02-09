from loader.monitor import Monitor
import time
from utils.myLogger import get_cmd_logger
import multiprocessing

# loader = Loader("mysql")
# loader.monitor_warehouse(1)
# transaction = Transaction("mysql")
# transaction.delivery(1)
# transaction.new_order(1, 1)
# transaction.payment(1, 1)
# transaction.stock_level(1)
# transaction.order_status(1)

# logger = get_cmd_logger()
# logger.info("Start!")
# monitor = Monitor("mysql")
# process = multiprocessing.Process(target=monitor.transaction_workload,args=())
# process.start()
# monitor2 = Monitor("mysql")
# process = multiprocessing.Process(target=monitor2.transaction_workload,args=())
# process.start()
# while True:
#     logger.info("QPS: " + str(monitor.get_counter()+monitor2.get_counter()))
#     time.sleep(1)

monitor = Monitor("mysql")
monitor.init_warehouse_item()