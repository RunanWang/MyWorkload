from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config

from datetime import datetime

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1",  #
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?",
        # d_id, w_id, no_o_id
        "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?",  # no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?",
        # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?",
        # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?",
        # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?",
        # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?",  # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?",  # d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?",
        # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
        # w_id, d_id, c_id
        "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)",  # o_id, d_id, w_id
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?",  # ol_i_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
        # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?",
        # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
    },

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
        cursor, conn = self.driver.get_conn()
        for district_id in range(1, config.DIST_PER_WARE + 1):
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
            sql = "UPDATE ORDER_LINE SET OL_DELIVERY_D = %s"  + " WHERE OL_O_ID = " + str(
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
            self.driver.transaction_commit()
        return result
