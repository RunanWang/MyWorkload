from TPC.loader.loader import Loader
from TPC.loader.transaction import Transaction
from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config
import multiprocessing
import time


class Monitor(object):
    def __init__(self, name):
        # 待分配的下一个warehouse-id
        self.max_warehouse_id = 1
        # warehouse-list的锁
        self.warehouse_id_lock = multiprocessing.Lock()
        # 所有完成的warehouse-id
        self.warehouse_id_list = [1]
        # warehouse-list的锁
        self.warehouse_id_list_lock = multiprocessing.Lock()

        self.logger = get_cmd_logger()
        driver_class = self.create_driver_class(name)
        self.driver = driver_class()
        self.transaction = Transaction(self.driver)
        self.loader = Loader(self.driver)

        # 计算QPS的计数器
        self.counter = multiprocessing.Value('i', lock=True)

    # 寻找name对应的driver
    @staticmethod
    def create_driver_class(name):
        full_name = "%sDriver" % name.title()
        mod = __import__('TPC.drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
        klass = getattr(mod, full_name)
        return klass

    def load_item(self):
        self.loader.monitor_item()
        self.logger.info("Monitor Load of Item Complete")

    def load_warehouse(self):
        # 分配warehouse_id并更新id值
        self.warehouse_id_lock.acquire()
        warehouse_id = self.max_warehouse_id
        self.max_warehouse_id = self.max_warehouse_id + 1
        self.warehouse_id_lock.release()
        # 进行load
        self.loader.monitor_warehouse(warehouse_id)
        # 把更新之后的内容加入id_list中
        self.warehouse_id_list_lock.acquire()
        self.warehouse_id_list.append(warehouse_id)
        self.warehouse_id_list_lock.release()
        self.logger.info("Monitor Load of Warehouse" + str(warehouse_id) + " Complete")

    def get_warehouse_id(self):
        self.warehouse_id_list_lock.acquire()
        num_id = rand.number(0, len(self.warehouse_id_list) - 1)
        num_id2 = num_id
        if len(self.warehouse_id_list) > 2:
            num_id2 = rand.numberExcluding(0, len(self.warehouse_id_list) - 1, num_id)
        warehouse_id = self.warehouse_id_list[num_id]
        warehouse_id_remote = self.warehouse_id_list[num_id2]
        self.warehouse_id_list_lock.release()
        return warehouse_id, warehouse_id_remote

    def delete_all(self):
        self.driver.delete_all(config.TABLE_NAME_ORDER_LINE)
        self.driver.delete_all(config.TABLE_NAME_NEW_ORDER)
        self.driver.delete_all(config.TABLE_NAME_ORDERS)
        self.driver.delete_all(config.TABLE_NAME_HISTORY)
        self.driver.delete_all(config.TABLE_NAME_CUSTOMER)
        self.driver.delete_all(config.TABLE_NAME_DISTRICT)
        self.driver.delete_all(config.TABLE_NAME_STOCK)
        self.driver.delete_all(config.TABLE_NAME_WAREHOUSE)
        self.driver.delete_all(config.TABLE_NAME_ITEM)

    def init_warehouse_item(self):
        self.delete_all()
        self.load_item()
        self.load_warehouse()

    def transaction_workload(self, t_id, alive):
        self.logger.info("Workload of transaction" + str(t_id) + " has been Started.")
        while alive.value is 1:
            prob = rand.number(1, 100)
            if prob <= 4:  # 4% stock-level
                warehouse_id, warehouse_id_remote = self.get_warehouse_id()
                self.transaction.stock_level(warehouse_id)
            elif prob <= 4 + 4:  # 4% delivery
                warehouse_id, warehouse_id_remote = self.get_warehouse_id()
                self.transaction.delivery(warehouse_id)
            elif prob <= 4 + 4 + 4:  # 4% order_status
                warehouse_id, warehouse_id_remote = self.get_warehouse_id()
                self.transaction.order_status(warehouse_id)
            elif prob <= 43 + 4 + 4 + 4:  # 43% payment
                warehouse_id, warehouse_id_remote = self.get_warehouse_id()
                self.transaction.payment(warehouse_id, warehouse_id_remote)
            else:  # 45% new_order
                warehouse_id, warehouse_id_remote = self.get_warehouse_id()
                self.transaction.new_order(warehouse_id, warehouse_id_remote)
            with self.counter.get_lock():
                self.counter.value = self.counter.value + 1
        self.logger.info("Workload of transaction" + str(t_id) + " has been terminated.")

    def get_counter(self):
        with self.counter.get_lock():
            ret = self.counter.value
            self.counter.value = 0
        return ret

    def monitor_process(self):
        # TODO: check load

        # TODO: do load

        # transaction process
        transaction_alive = multiprocessing.Value('i', 1)
        transaction_process_num = 2
        transaction_process_list = []
        start_transaction_time = time.time()
        for i in range(1, transaction_process_num + 1):
            temp_process = multiprocessing.Process(target=self.transaction_workload, args=(i, transaction_alive, ))
            transaction_process_list.append(temp_process)
            temp_process.start()

        # Daemon process
        while True:
            signal = input()
            if signal is 'c':
                self.logger.info('Get end transaction signal! Please wait.')
                transaction_alive.value = 0
                break
            else:
                self.logger.info('Wrong signal. The end signal is c.')

        for item in transaction_process_list:
            item.join()
        end_transaction_time = time.time()
        total_transaction_time = end_transaction_time - start_transaction_time
        transaction_counter = self.get_counter()
        self.logger.info(
            "All Transaction Terminated. During " + str(total_transaction_time) + " of transaction time, " + str(
                transaction_counter) + " of transactions has been submit. Average " + str(
                transaction_counter / total_transaction_time) + " TPS.")
