from TPC.loader.loader import Loader
from TPC.probe.mysqlprobe import Probe
from TPC.loader.transaction import Transaction
from TPC.utils.myLogger import get_cmd_logger
from random import shuffle
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
        self.warehouse_id_list = []
        # warehouse-list的锁
        self.warehouse_id_list_lock = multiprocessing.Lock()

        self.logger = get_cmd_logger()
        driver_class = self.create_driver_class(name)
        self.driver = driver_class()
        self.transaction = Transaction(self.driver)
        self.loader = Loader(self.driver)
        self.probe = Probe(self.driver)
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

    def load_warehouse(self, alive):
        while alive.value is 1:
            # 分配warehouse_id并更新id值
            self.warehouse_id_lock.acquire()
            warehouse_id = self.max_warehouse_id
            self.max_warehouse_id = self.max_warehouse_id + 1
            self.warehouse_id_lock.release()
            # 进行load
            self.monitor_warehouse(warehouse_id, alive)
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

    def delete_warehouse(self, warehouse_id):
        sql_order_line = "delete from ORDER_LINE where OL_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_order_line)
        sql_new_order = "delete from NEW_ORDER where NO_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_new_order)
        sql_orders = "delete from ORDERS where O_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_orders)
        sql_history = "delete from HISTORY where H_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_history)
        sql_customer = "delete from CUSTOMER where C_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_customer)
        sql_district = "delete from DISTRICT where D_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_district)
        sql_stock = "delete from STOCK where S_W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_stock)
        sql_warehouse = "delete from WAREHOUSE where W_ID = " + str(warehouse_id) + ";"
        self.driver.delete(sql_warehouse)

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

    def monitor_warehouse(self, warehouse_id, alive):
        # 添加一个warehouse
        self.loader.load_warehouse(warehouse_id)
        # Stock
        for item_id in range(1, config.NUM_ITEMS + 1):
            if alive.value is 0:
                break
            self.loader.load_stock(warehouse_id, item_id)
            if item_id % 1000 == 0:
                self.logger.info("Load of Stock of Warehouse" + str(warehouse_id) + ": " + str(item_id) + "/" + str(
                    config.NUM_ITEMS))

        # District/customer/history/order
        for district_id in range(1, config.DIST_PER_WARE + 1):
            if alive.value is 0:
                break
            self.loader.load_district(warehouse_id, district_id)

            # customer和history
            c_id_permutation = []
            for customer_id in range(1, config.CUST_PER_DIST + 1):
                if alive.value is 0:
                    break
                self.loader.load_customer(warehouse_id, district_id, customer_id)
                self.loader.load_history(warehouse_id, district_id, customer_id)
                c_id_permutation.append(customer_id)
                if customer_id % 1000 == 0:
                    self.logger.info("Load of Customer of District" + str(district_id) + " of Warehouse" + str(
                        warehouse_id) + ": " + str(customer_id) + "/" + str(config.CUST_PER_DIST))

            self.logger.info("Load of Customer of District" + str(district_id) + " of Warehouse" + str(
                warehouse_id) + " has been done!")

            shuffle(c_id_permutation)

            # Order、Order-line以及New-Order
            for order_id in range(1, config.CUST_PER_DIST + 1):
                if alive.value is 0:
                    break
                new_order = ((config.CUST_PER_DIST - config.NEW_ORDERS_PER_DISTRICT) < order_id)
                order_line_count = rand.number(config.MIN_OL_CNT, config.MAX_OL_CNT)
                self.loader.load_order(warehouse_id, district_id, order_id, c_id_permutation[order_id - 1],
                                       order_line_count, new_order)
                for order_line_number in range(0, order_line_count):
                    if alive.value is 0:
                        break
                    self.loader.load_order_line(warehouse_id, district_id, order_id, order_line_number,
                                                config.NUM_ITEMS, new_order)
                if new_order:
                    self.loader.load_new_order(order_id, district_id, warehouse_id)

                if order_id % 500 == 0:
                    self.logger.info("Load of Order of District" + str(district_id) + " of Warehouse" + str(
                        warehouse_id) + ": " + str(order_id) + "/" + str(config.CUST_PER_DIST))
            if alive.value is 0:
                break
            self.logger.info(
                "Load of District" + str(district_id) + " of Warehouse" + str(warehouse_id) + " has been done!")
        if alive.value is 0:
            self.delete_warehouse(warehouse_id)
            self.logger.info("Load of Warehouse" + str(warehouse_id) + " has been canceled!")
            return
        self.logger.info("Load of Warehouse" + str(warehouse_id) + " has been done!")

    def monitor_process(self):
        process_list = []
        # check load
        check_warehouse_sql = "SELECT w_id from WAREHOUSE;"
        warehouse_dict = self.driver.fetch_all(check_warehouse_sql)
        for item in warehouse_dict:
            self.warehouse_id_list.append(item['w_id'])
            if item['w_id'] >= self.max_warehouse_id:
                self.max_warehouse_id = item['w_id'] + 1

        # load process
        load_alive = multiprocessing.Value('i', 1)
        temp_process = multiprocessing.Process(target=self.load_warehouse, args=(load_alive,))
        process_list.append(temp_process)
        temp_process.start()

        # transaction process
        transaction_alive = multiprocessing.Value('i', 1)
        transaction_process_num = 2
        start_transaction_time = time.time()
        for i in range(1, transaction_process_num + 1):
            temp_process = multiprocessing.Process(target=self.transaction_workload, args=(i, transaction_alive,))
            process_list.append(temp_process)
            temp_process.start()

        # probe process
        probe_alive = multiprocessing.Value('i', 1)
        queue = multiprocessing.Queue()
        process_w = multiprocessing.Process(target=self.probe.save_result, args=(queue, probe_alive,))
        process_w.start()
        process_list.append(process_w)
        for i in range(len(self.probe.PROBE_SQL_LIST)):
            self.probe.file_init(i)
            process = multiprocessing.Process(target=self.probe.workflow_probe, args=(i, queue, probe_alive,))
            process.start()
            process_list.append(process)
            self.logger.info("Probe of Query " + str(i) + " started!")
            time.sleep(config.PROBE_TIME_BETWEEN_SQL)

        # Daemon process
        while True:
            signal = input()
            if signal is 'c':
                self.logger.info('Get end signal! Please wait.')
                transaction_alive.value = 0
                probe_alive.value = 0
                load_alive.value = 0
                break
            else:
                self.logger.info('Wrong signal. The end signal is c.')
        queue.put(None)
        for item in process_list:
            item.join()
        end_transaction_time = time.time()
        total_transaction_time = end_transaction_time - start_transaction_time
        transaction_counter = self.get_counter()
        queue.close()
        self.logger.info(
            "All Transaction Terminated. During " + str(total_transaction_time) + " of transaction time, " + str(
                transaction_counter) + " of transactions has been submit. Average " + str(
                transaction_counter / total_transaction_time) + " TPS.")
        self.logger.warn("Probe Monitor Terminated!!")
