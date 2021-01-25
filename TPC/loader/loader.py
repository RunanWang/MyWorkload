from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config

from datetime import datetime
import multiprocessing


class Loader(object):
    def __init__(self, name):
        self.logger = get_cmd_logger()
        self.item_counter = multiprocessing.Value("i", 0, lock=True)
        self.warehouse_counter = multiprocessing.Value("i", 0, lock=True)
        driver_class = self.create_driver_class(name)
        self.driver = driver_class()

    # 寻找name对应的driver
    @staticmethod
    def create_driver_class(name):
        full_name = "%sDriver" % name.title()
        mod = __import__('TPC.drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
        klass = getattr(mod, full_name)
        return klass

    # 把Original标记填满
    @staticmethod
    def generate_original_string(data):
        original_length = len(config.ORIGINAL_STRING)
        position = rand.number(0, len(data) - original_length)
        out = data[:position] + config.ORIGINAL_STRING + data[position + original_length:]
        assert len(out) == len(data)
        return out

    # 产生tax，用于warehouse
    @staticmethod
    def generate_tax():
        return rand.fixedPoint(config.TAX_DECIMALS, config.MIN_TAX, config.MAX_TAX)

    # 产生邮编
    @staticmethod
    def generate_zip():
        length = config.ZIP_LENGTH - len(config.ZIP_SUFFIX)
        return rand.nstring(length, length) + config.ZIP_SUFFIX

    # 产生地址
    def generate_address(self):
        name = rand.astring(config.MIN_NAME, config.MAX_NAME)
        street1 = rand.astring(config.MIN_STREET, config.MAX_STREET)
        street2 = rand.astring(config.MIN_STREET, config.MAX_STREET)
        city = rand.astring(config.MIN_CITY, config.MAX_CITY)
        state = rand.astring(config.STATE, config.STATE)
        add_zip = self.generate_zip()
        return [name, street1, street2, city, state, add_zip]

    def load_item(self, item_id: int):
        """
        向数据库中插入1条item_id的item，其中有10%的概率i-data为ORIGINAL。

        :param item_id: item的ID
        """
        i_id = item_id
        i_im_id = rand.number(config.MIN_IM, config.MAX_IM)
        i_name = rand.astring(config.MIN_I_NAME, config.MAX_I_NAME)
        i_price = rand.fixedPoint(config.MONEY_DECIMALS, config.MIN_PRICE, config.MAX_PRICE)
        i_data = rand.astring(config.MIN_I_DATA, config.MAX_I_DATA)
        # Select 10% of the rows to be marked "original"
        if rand.rand_bool(config.I_ORIGINAL_RATE):
            i_data = self.generate_original_string(i_data)
        item_detail = [i_id, i_im_id, i_name, i_price, i_data]
        self.driver.insert(config.TABLE_NAME_ITEM, item_detail)

    def load_warehouse(self, warehouse_id: int):
        """
        向数据库中插入1条warehouse_id的WAREHOUSE。

        :param warehouse_id: warehouse的ID
        """
        w_tax = self.generate_tax()
        w_ytd = config.INITIAL_W_YTD
        w_address = self.generate_address()
        warehouse_detail = [[warehouse_id] + w_address + [w_tax, w_ytd]]
        self.driver.insert(config.TABLE_NAME_WAREHOUSE, warehouse_detail)

    def load_district(self, d_w_id: int, d_id: int, d_next_o_id: int):
        """
        插入一条District

        :param d_w_id: Warehouse-ID
        :param d_id: District-ID
        :param d_next_o_id: Next_Order_ID, Default: CustomerPerDistrict
        """
        d_tax = self.generate_tax()
        d_ytd = config.D_INITIAL_YTD
        d_address = self.generate_address()
        district_detail = [[d_id, d_w_id] + d_address + [d_tax, d_ytd, d_next_o_id]]
        self.driver.insert(config.TABLE_NAME_DISTRICT, district_detail)

    def load_customer(self, c_w_id: int, c_d_id: int, c_id: int, bad_credit: bool):
        """
        插入一条 Customer

        :param c_w_id: WarehouseID of Customer
        :param c_d_id: DistrictID of Customer
        :param c_id: CustomerID
        :param bad_credit: Whether Customer is bad-credit
        """
        c_first = rand.astring(config.MIN_FIRST, config.MAX_FIRST)
        c_middle = config.MIDDLE
        # 一部分是随机，一部分是伪随机
        if c_id <= 1000:
            c_last = rand.makeLastName(c_id - 1)
        else:
            c_last = rand.makeRandomLastName(config.CUSTOMERS_PER_DISTRICT)
        # 详细信息
        c_phone = rand.nstring(config.PHONE, config.PHONE)
        c_since = datetime.now()
        c_credit = config.BAD_CREDIT if bad_credit else config.GOOD_CREDIT
        c_credit_lim = config.INITIAL_CREDIT_LIM
        c_discount = rand.fixedPoint(config.DISCOUNT_DECIMALS, config.MIN_DISCOUNT, config.MAX_DISCOUNT)
        c_balance = config.INITIAL_BALANCE
        c_ytd_payment = config.INITIAL_YTD_PAYMENT
        c_payment_cnt = config.INITIAL_PAYMENT_CNT
        c_delivery_cnt = config.INITIAL_DELIVERY_CNT
        c_data = rand.astring(config.MIN_C_DATA, config.MAX_C_DATA)
        # 地址信息
        c_street1 = rand.astring(config.MIN_STREET, config.MAX_STREET)
        c_street2 = rand.astring(config.MIN_STREET, config.MAX_STREET)
        c_city = rand.astring(config.MIN_CITY, config.MAX_CITY)
        c_state = rand.astring(config.STATE, config.STATE)
        c_zip = self.generate_zip()
        # 生成并插入
        c_detail = [c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street1, c_street2, c_city, c_state, c_zip,
                    c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt,
                    c_delivery_cnt, c_data]
        self.driver.insert(config.TABLE_NAME_CUSTOMER, c_detail)

    def monitor(self):
        self.load_item(self.item_counter.value)
        self.item_counter.value += config.NUM_ITEMS
        self.load_warehouse(self.warehouse_counter.value)
        self.warehouse_counter.value += config.NUM_WAREHOUSE


if __name__ == "__main__":
    loader = Loader("mysql")
    loader.monitor()
