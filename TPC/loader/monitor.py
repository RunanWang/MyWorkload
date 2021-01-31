from TPC.loader.loader import Loader
from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import multiprocessing


class Monitor(object):
    def __init__(self):
        # 待分配的下一个warehouse-id
        self.max_warehouse_id = 1
        # warehouse-list的锁
        self.warehouse_id_lock = multiprocessing.Lock()
        # 所有完成的warehouse-id
        self.warehouse_id_list = []
        # warehouse-list的锁
        self.warehouse_id_list_lock = multiprocessing.Lock()
        self.logger = get_cmd_logger()

    def load_item(self):
        loader = Loader("mysql")
        loader.monitor_item()
        self.logger.info("Monitor Load of Item Complete")

    def load_warehouse(self):
        loader = Loader("mysql")
        # 分配warehouse_id并更新id值
        self.warehouse_id_lock.acquire()
        warehouse_id = self.max_warehouse_id
        self.max_warehouse_id = self.max_warehouse_id + 1
        self.warehouse_id_lock.release()
        # 进行load
        loader.monitor_warehouse(warehouse_id)
        # 把更新之后的内容加入id_list中
        self.warehouse_id_list_lock.acquire()
        self.warehouse_id_list.append(warehouse_id)
        self.warehouse_id_list_lock.release()

    def get_warehouse_id(self):
        self.warehouse_id_list_lock.acquire()
        num_id = rand.number(1, len(self.warehouse_id_list))
        warehouse_id = self.warehouse_id_list[num_id]
        self.warehouse_id_list_lock.release()
        return warehouse_id
