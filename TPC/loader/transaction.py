from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config

from datetime import datetime

TXN_QUERIES = {
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
        # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST",
        # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1",
        # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?",
        # w_id, d_id, o_id
    },

    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?",
        # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?",  # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
        # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?",
        # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
        # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST",
        # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
        # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
        # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    },

    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """,
    },
}


class Transaction(object):
    def __init__(self, name):
        self.logger = get_cmd_logger()
        driver_class = self.create_driver_class(name)
        self.driver = driver_class()

    # 寻找name对应的driver
    @staticmethod
    def create_driver_class(name):
        full_name = "%sDriver" % name.title()
        mod = __import__('TPC.drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
        klass = getattr(mod, full_name)
        return klass

    def delivery(self, warehouse_id):
        o_carrier_id = rand.number(config.MIN_CARRIER_ID, config.MAX_CARRIER_ID)
        ol_delivery_d = datetime.now()
        result = []
        for district_id in range(1, config.DIST_PER_WARE + 1):
            cursor, conn = self.driver.get_conn()
            # get New-Order
            sql = "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = " + str(district_id) + " AND NO_W_ID = " + str(
                warehouse_id) + " AND NO_O_ID > -1 LIMIT 1"
            try:
                new_order = self.driver.transaction_fetchone(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                continue
            if new_order is None:
                # No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            no_order_id = new_order['NO_O_ID']

            # get Customer ID
            sql = "SELECT O_C_ID FROM ORDERS WHERE O_ID = " + str(no_order_id) + " AND O_D_ID = " + str(
                district_id) + " AND O_W_ID = " + str(warehouse_id)
            cursor.execute(sql)
            try:
                self.driver.transaction_exec(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                continue
            customer_id = cursor.fetchone()['O_C_ID']

            # sum OrderLine Amount
            sql = "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = " + str(no_order_id) + " AND OL_D_ID = " + str(
                district_id) + " AND OL_W_ID = " + str(warehouse_id)
            try:
                order_line_total = self.driver.transaction_fetchone(cursor, conn, sql)['SUM(OL_AMOUNT)']
            except Exception as e:
                self.logger.warn(e)
                continue

            # delete New-Order
            sql = "DELETE FROM NEW_ORDER WHERE NO_D_ID = " + str(district_id) + " AND NO_W_ID = " + str(
                warehouse_id) + " AND NO_O_ID = " + str(no_order_id)
            try:
                self.driver.transaction_exec(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                continue

            # update Orders
            sql = "UPDATE ORDERS SET O_CARRIER_ID = " + str(o_carrier_id) + " WHERE O_ID = " + str(
                no_order_id) + " AND O_D_ID = " + str(district_id) + " AND O_W_ID = " + str(warehouse_id)
            try:
                self.driver.transaction_exec(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                continue

            # update OrderLine
            sql = "UPDATE ORDER_LINE SET OL_DELIVERY_D = %s" + " WHERE OL_O_ID = " + str(
                no_order_id) + " AND OL_D_ID = " + str(district_id) + " AND OL_W_ID = " + str(warehouse_id)
            try:
                self.driver.transaction_exec(cursor, conn, sql, [ol_delivery_d])
            except Exception as e:
                self.logger.warn(e)
                continue

            # update Customer
            sql = "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + " + str(order_line_total) + " WHERE C_ID = " + str(
                customer_id) + " AND C_D_ID = " + str(district_id) + " AND C_W_ID = " + str(warehouse_id)
            try:
                self.driver.transaction_exec(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                continue

            result.append((district_id, no_order_id))
            self.driver.transaction_commit(cursor, conn)
        return result

    def new_order(self, warehouse_id, remote_warehouse_id):
        cursor, conn = self.driver.get_conn()
        district_id = rand.number(1, config.DIST_PER_WARE)
        customer_id = rand.NURand(1023, 1, config.CUST_PER_DIST)
        order_line_count = rand.number(config.MIN_OL_CNT, config.MAX_OL_CNT)
        o_entry_d = datetime.now()
        all_local = True

        # 1% of transactions roll back
        rollback = rand.rand_bool(1)

        i_ids = []
        items = []
        i_w_ids = []
        # get items
        for i in range(0, order_line_count):
            i_id = rand.NURand(8191, 1, config.NUM_ITEMS)
            while i_id in i_ids:
                i_id = rand.NURand(8191, 1, config.NUM_ITEMS)
            i_ids.append(i_id)

            # 1% of items are from a remote warehouse
            remote = rand.rand_bool(1)
            if remote and warehouse_id != remote_warehouse_id:
                all_local = False
                i_w_ids.append(remote_warehouse_id)
            else:
                i_w_ids.append(warehouse_id)

            # get item
            sql = "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = " + str(i_id)
            try:
                item_info = self.driver.transaction_fetchone(cursor, conn, sql)
                items.append(item_info)
            except Exception as e:
                self.logger.warn(e)
                continue

        # get Warehouse Tax Rate
        sql = "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = " + str(warehouse_id)
        try:
            warehouse_tax = self.driver.transaction_fetchone(cursor, conn, sql)['W_TAX']
        except Exception as e:
            self.logger.warn(e)
            return

        # get District
        sql = "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = " + str(district_id) + " AND D_W_ID = " + str(
            warehouse_id)
        try:
            district_info = self.driver.transaction_fetchone(cursor, conn, sql)
        except Exception as e:
            self.logger.warn(e)
            return

        # get Customer
        sql = "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = " + str(
            warehouse_id) + " AND C_D_ID = " + str(district_id) + " AND C_ID = " + str(customer_id)
        try:
            customer_info = self.driver.transaction_fetchone(cursor, conn, sql)
        except Exception as e:
            self.logger.warn(e)
            return

        # increment Next Order-ID
        sql = "UPDATE DISTRICT SET D_NEXT_O_ID = " + str(district_info['D_NEXT_O_ID'] + 1) + " WHERE D_ID = " + str(
            district_id) + " AND D_W_ID = " + str(warehouse_id)
        try:
            customer_info = self.driver.transaction_fetchone(cursor, conn, sql)
        except Exception as e:
            self.logger.warn(e)
            return

        # create Order
        o_detail = [district_info['D_NEXT_O_ID'], district_id, warehouse_id, customer_id, o_entry_d,
                    config.NULL_CARRIER_ID, order_line_count, all_local]
        try:
            self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_ORDERS, o_detail)
        except Exception as e:
            self.logger.warn(e)
            return

        # create NewOrder
        no_detail = [district_info['D_NEXT_O_ID'], district_id, warehouse_id]
        try:
            self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_NEW_ORDER, no_detail)
        except Exception as e:
            self.logger.warn(e)
            return

        total = 0
        #
        for i in range(len(i_ids)):
            order_line_num = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = rand.number(1, config.MAX_OL_QUANTITY)

            item_info = items[i]
            i_name = item_info["I_NAME"]
            i_data = item_info["I_DATA"]
            i_price = item_info["I_PRICE"]

            # get StockInfo
            sql = "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = " % district_id + str(
                ol_i_id) + " AND S_W_ID = " + str(warehouse_id)
            try:
                stock_info = self.driver.transaction_fetchone(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                return

            s_quantity = stock_info['S_QUANTITY']
            s_ytd = stock_info['S_YTD']
            s_order_cnt = stock_info['S_ORDER_CNT']
            s_remote_cnt = stock_info['S_REMOTE_CNT']
            s_data = stock_info['S_DATA']
            s_temp = "S_DIST_%02d" % district_id
            s_dist = stock_info[s_temp]

            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1

            if ol_supply_w_id != warehouse_id:
                s_remote_cnt += 1

            # update Stock
            sql = "UPDATE STOCK SET S_QUANTITY = " + str(s_quantity) + ", S_YTD = " + str(
                s_ytd) + ", S_ORDER_CNT = " + str(s_order_cnt) + ", S_REMOTE_CNT = " + str(
                s_remote_cnt) + " WHERE S_I_ID = " + str(ol_i_id) + " AND S_W_ID = " + str(ol_supply_w_id)
            try:
                self.driver.transaction_exec(cursor, conn, sql)
            except Exception as e:
                self.logger.warn(e)
                return

            if i_data.find(config.ORIGINAL_STRING) != -1 and s_data.find(config.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ol_amount = ol_quantity * i_price
            total += ol_amount

            # create OrderLine
            ol_detail = [district_info['D_NEXT_O_ID'], district_id, warehouse_id, order_line_num, ol_i_id,
                         ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist]
            try:
                self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_ORDER_LINE, ol_detail)
            except Exception as e:
                self.logger.warn(e)
                return

        if rollback:
            self.driver.transaction_rollback(cursor, conn)
        else:
            self.driver.transaction_commit(cursor, conn)
            total *= (1 - customer_info['C_DISCOUNT']) * (1 + warehouse_tax + district_info['D_TAX'])
            self.logger.debug(
                "Customer " + str(customer_id) + " of District " + str(district_id) + " of Warehouse " + str(
                    warehouse_id) + " generate new order " + str(
                    district_info['D_NEXT_O_ID']) + " with total of " + str(total) + ".")
