from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config

from datetime import datetime
from random import shuffle


class Loader(object):
    def __init__(self, driver):
        self.logger = get_cmd_logger()
        self.driver = driver

    @staticmethod
    def generate_original_string(data):
        original_length = len(config.ORIGINAL_STRING)
        position = rand.number(0, len(data) - original_length)
        out = data[:position] + config.ORIGINAL_STRING + data[position + original_length:]
        assert len(out) == len(data)
        return out

    @staticmethod
    def generate_tax():
        return rand.fixedPoint(config.TAX_DECIMALS, config.MIN_TAX, config.MAX_TAX)

    @staticmethod
    def generate_zip():
        length = config.ZIP_LENGTH - len(config.ZIP_SUFFIX)
        return rand.nstring(length, length) + config.ZIP_SUFFIX

    def generate_address(self):
        """
        产生地址
        :return:
        """
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
        warehouse_detail = [warehouse_id] + w_address + [w_tax, w_ytd]
        self.driver.insert(config.TABLE_NAME_WAREHOUSE, warehouse_detail)

    def load_district(self, d_w_id: int, d_id: int):
        """
        插入一条District

        :param d_w_id: Warehouse-ID
        :param d_id: District-ID
        """
        d_next_o_id = config.CUST_PER_DIST + 1
        d_tax = self.generate_tax()
        d_ytd = config.D_INITIAL_YTD
        d_address = self.generate_address()
        district_detail = [d_id, d_w_id] + d_address + [d_tax, d_ytd, d_next_o_id]
        self.driver.insert(config.TABLE_NAME_DISTRICT, district_detail)

    def load_customer(self, c_w_id: int, c_d_id: int, c_id: int):
        """
        插入一条 Customer

        :param c_w_id: WarehouseID of Customer
        :param c_d_id: DistrictID of Customer
        :param c_id: CustomerID
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
        c_credit = config.BAD_CREDIT if rand.rand_bool(config.C_BAD_CREDIT_RATE) else config.GOOD_CREDIT
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

    def load_history(self, h_c_w_id: int, h_c_d_id: int, h_c_id: int):
        """
        插入一条 History

        :param h_c_w_id: warehouse-ID
        :param h_c_d_id: district-ID
        :param h_c_id: customer-ID
        """
        h_w_id = h_c_w_id
        h_d_id = h_c_d_id
        h_date = datetime.now()
        h_amount = config.H_INITIAL_AMOUNT
        h_data = rand.astring(config.H_MIN_DATA, config.H_MAX_DATA)
        h_detail = [h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data]
        self.driver.insert(config.TABLE_NAME_HISTORY, h_detail)

    def load_stock(self, s_w_id: int, s_i_id: int):
        """
        插入一条 Stock

        :param s_w_id: warehouse-ID
        :param s_i_id: Item_ID
        """
        s_quantity = rand.number(config.S_MIN_QUANTITY, config.S_MAX_QUANTITY)
        s_ytd = 0
        s_order_cnt = 0
        s_remote_cnt = 0
        s_data = rand.astring(config.MIN_I_DATA, config.MAX_I_DATA)

        # Select 10% of the stock to be marked "original"
        if rand.rand_bool(config.S_ORIGINAL_RATE):
            s_data = self.generate_original_string(s_data)

        s_dists = []
        for i in range(0, config.DIST_PER_WARE):
            s_dists.append(rand.astring(config.S_DIST, config.S_DIST))

        s_detail = [s_i_id, s_w_id, s_quantity] + s_dists + [s_ytd, s_order_cnt, s_remote_cnt, s_data]
        self.driver.insert(config.TABLE_NAME_STOCK, s_detail)

    def load_order(self, o_w_id: int, o_d_id: int, o_id: int, o_c_id: int, o_ol_cnt: int, new_order: bool):
        """

        :param o_w_id: warehouse-ID
        :param o_d_id: District-ID
        :param o_id: Order-ID
        :param o_c_id: Customer-ID
        :param o_ol_cnt: Orderline-Count
        :param new_order:
        """
        o_entry_d = datetime.now()
        if new_order:
            o_carrier_id = config.NULL_CARRIER_ID
        else:
            o_carrier_id = rand.number(config.MIN_CARRIER_ID, config.MAX_CARRIER_ID)
        o_all_local = config.INITIAL_ALL_LOCAL
        o_detail = [o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local]
        self.driver.insert(config.TABLE_NAME_ORDERS, o_detail)

    def load_order_line(self, ol_w_id, ol_d_id, ol_o_id, ol_number, max_items, new_order):
        ol_i_id = rand.number(1, max_items)
        ol_supply_w_id = ol_w_id
        ol_delivery_d = datetime.now()
        ol_quantity = config.INITIAL_QUANTITY

        # 1% of items are from a remote warehouse
        remote = (rand.number(1, 100) == 1)
        if remote:
            if ol_w_id == 1:
                ol_supply_w_id = 1
            else:
                ol_supply_w_id = rand.numberExcluding(1, ol_w_id, ol_w_id)

        if not new_order:
            ol_amount = 0.00
        else:
            ol_amount = rand.fixedPoint(config.MONEY_DECIMALS, config.MIN_AMOUNT,
                                        config.MAX_PRICE * config.MAX_OL_QUANTITY)
            # ol_delivery_d = None
        ol_dist_info = rand.astring(config.S_DIST, config.S_DIST)

        ol_detail = [ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity,
                     ol_amount, ol_dist_info]
        self.driver.insert(config.TABLE_NAME_ORDER_LINE, ol_detail)

    def load_new_order(self, o_id, d_id, w_id):
        no_detail = [o_id, d_id, w_id]
        self.driver.insert(config.TABLE_NAME_NEW_ORDER, no_detail)

    def monitor_item(self):
        for item_id in range(1, config.NUM_ITEMS + 1):
            self.load_item(item_id)
            if item_id % 1000 == 0:
                self.logger.info("Load num of items: " + str(item_id))
        self.logger.info("Load of items finished!")

    def monitor_warehouse(self, warehouse_id):
        # 添加一个warehouse
        self.load_warehouse(warehouse_id)
        # Stock
        for item_id in range(1, config.NUM_ITEMS + 1):
            self.load_stock(warehouse_id, item_id)
            if item_id % 1000 == 0:
                self.logger.info("Load of Stock of Warehouse" + str(warehouse_id) + ": " + str(item_id) + "/" + str(
                    config.NUM_ITEMS))

        # District/customer/history/order
        for district_id in range(1, config.DIST_PER_WARE + 1):
            self.load_district(warehouse_id, district_id)

            # customer和history
            c_id_permutation = []
            for customer_id in range(1, config.CUST_PER_DIST + 1):
                self.load_customer(warehouse_id, district_id, customer_id)
                self.load_history(warehouse_id, district_id, customer_id)
                c_id_permutation.append(customer_id)
                if customer_id % 1000 == 0:
                    self.logger.info("Load of Customer of District" + str(district_id) + " of Warehouse" + str(
                        warehouse_id) + ": " + str(customer_id) + "/" + str(config.CUST_PER_DIST))

            self.logger.info("Load of Customer of District" + str(district_id) + " of Warehouse" + str(
                warehouse_id) + " has been done!")

            shuffle(c_id_permutation)

            # Order、Order-line以及New-Order
            for order_id in range(1, config.CUST_PER_DIST + 1):
                new_order = ((config.CUST_PER_DIST - config.NEW_ORDERS_PER_DISTRICT) < order_id)
                order_line_count = rand.number(config.MIN_OL_CNT, config.MAX_OL_CNT)
                self.load_order(warehouse_id, district_id, order_id, c_id_permutation[order_id - 1], order_line_count,
                                new_order)
                for order_line_number in range(0, order_line_count):
                    self.load_order_line(warehouse_id, district_id, order_id, order_line_number, config.NUM_ITEMS,
                                         new_order)
                if new_order:
                    self.load_new_order(order_id, district_id, warehouse_id)

                if order_id % 500 == 0:
                    self.logger.info("Load of Order of District" + str(district_id) + " of Warehouse" + str(
                        warehouse_id) + ": " + str(order_id) + "/" + str(config.CUST_PER_DIST))

            self.logger.info(
                "Load of District" + str(district_id) + " of Warehouse" + str(warehouse_id) + " has been done!")
        self.logger.info("Load of Warehouse" + str(warehouse_id) + " has been done!")


if __name__ == "__main__":
    loader = Loader("mysql")
