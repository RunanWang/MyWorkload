from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config

from datetime import datetime


class Transaction(object):
    def __init__(self, driver):
        self.logger = get_cmd_logger()
        self.driver = driver

    def delivery(self, warehouse_id):
        cursor, conn = self.driver.get_conn()
        try:
            o_carrier_id = rand.number(config.MIN_CARRIER_ID, config.MAX_CARRIER_ID)
            ol_delivery_d = datetime.now()
            result = []
            for district_id in range(1, config.DIST_PER_WARE + 1):
                # get New-Order
                sql = "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = " + str(district_id) + " AND NO_W_ID = " + str(
                    warehouse_id) + " AND NO_O_ID > -1 LIMIT 1"
                new_order = self.driver.transaction_fetchone(cursor, conn, sql)

                if new_order is None:
                    # No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                no_order_id = new_order['NO_O_ID']

                # get Customer ID
                sql = "SELECT O_C_ID FROM ORDERS WHERE O_ID = " + str(no_order_id) + " AND O_D_ID = " + str(
                    district_id) + " AND O_W_ID = " + str(warehouse_id)
                cursor.execute(sql)
                self.driver.transaction_exec(cursor, conn, sql)
                customer_id = cursor.fetchone()['O_C_ID']

                # sum OrderLine Amount
                sql = "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = " + str(
                    no_order_id) + " AND OL_D_ID = " + str(
                    district_id) + " AND OL_W_ID = " + str(warehouse_id)
                order_line_total = self.driver.transaction_fetchone(cursor, conn, sql)['SUM(OL_AMOUNT)']

                # delete New-Order
                sql = "DELETE FROM NEW_ORDER WHERE NO_D_ID = " + str(district_id) + " AND NO_W_ID = " + str(
                    warehouse_id) + " AND NO_O_ID = " + str(no_order_id)
                self.driver.transaction_exec(cursor, conn, sql)

                # update Orders
                sql = "UPDATE ORDERS SET O_CARRIER_ID = " + str(o_carrier_id) + " WHERE O_ID = " + str(
                    no_order_id) + " AND O_D_ID = " + str(district_id) + " AND O_W_ID = " + str(warehouse_id)
                self.driver.transaction_exec(cursor, conn, sql)

                # update OrderLine
                sql = "UPDATE ORDER_LINE SET OL_DELIVERY_D = %s" + " WHERE OL_O_ID = " + str(
                    no_order_id) + " AND OL_D_ID = " + str(district_id) + " AND OL_W_ID = " + str(warehouse_id)
                self.driver.transaction_exec(cursor, conn, sql, [ol_delivery_d])

                # update Customer
                sql = "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + " + str(order_line_total) + " WHERE C_ID = " + str(
                    customer_id) + " AND C_D_ID = " + str(district_id) + " AND C_W_ID = " + str(warehouse_id)
                self.driver.transaction_exec(cursor, conn, sql)
                result.append((district_id, no_order_id))

                self.driver.transaction_commit(cursor, conn)
            self.logger.debug("Transaction Delivery complete:" + str(result))

        except KeyboardInterrupt:
            self.driver.transaction_rollback(cursor, conn)
            self.logger.info("Transaction Delivery has been rollback.")

        except Exception as e:
            self.logger.warn(e)

    def new_order(self, warehouse_id, remote_warehouse_id):
        cursor, conn = self.driver.get_conn()
        try:
            district_id = rand.number(1, config.DIST_PER_WARE)
            customer_id = rand.NURand(1023, 1, config.CUST_PER_DIST)
            order_line_count = rand.number(config.MIN_OL_CNT, config.MAX_OL_CNT)
            o_entry_d = datetime.now()
            all_local = 1

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
                    all_local = 0
                    i_w_ids.append(remote_warehouse_id)
                else:
                    i_w_ids.append(warehouse_id)

                # get item
                sql = "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = " + str(i_id)
                item_info = self.driver.transaction_fetchone(cursor, conn, sql)
                items.append(item_info)

            # get Warehouse Tax Rate
            sql = "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = " + str(warehouse_id)
            warehouse_tax = self.driver.transaction_fetchone(cursor, conn, sql)['W_TAX']

            # get District
            sql = "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = " + str(district_id) + " AND D_W_ID = " + str(
                warehouse_id)
            district_info = self.driver.transaction_fetchone(cursor, conn, sql)

            # get Customer
            sql = "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = " + str(
                warehouse_id) + " AND C_D_ID = " + str(district_id) + " AND C_ID = " + str(customer_id)
            customer_info = self.driver.transaction_fetchone(cursor, conn, sql)

            # increment Next Order-ID
            sql = "UPDATE DISTRICT SET D_NEXT_O_ID = " + str(district_info['D_NEXT_O_ID'] + 1) + " WHERE D_ID = " + str(
                district_id) + " AND D_W_ID = " + str(warehouse_id)
            self.driver.transaction_exec(cursor, conn, sql)

            # create Order
            o_detail = [district_info['D_NEXT_O_ID'], district_id, warehouse_id, customer_id, o_entry_d,
                        config.NULL_CARRIER_ID, order_line_count, all_local]
            self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_ORDERS, o_detail)

            # create NewOrder
            no_detail = [district_info['D_NEXT_O_ID'], district_id, warehouse_id]
            self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_NEW_ORDER, no_detail)

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
                stock_info = self.driver.transaction_fetchone(cursor, conn, sql)

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
                self.driver.transaction_exec(cursor, conn, sql)

                # if i_data.find(config.ORIGINAL_STRING) != -1 and s_data.find(config.ORIGINAL_STRING) != -1:
                #     brand_generic = 'B'
                # else:
                #     brand_generic = 'G'

                ol_amount = ol_quantity * i_price
                total += ol_amount

                # create OrderLine
                ol_detail = [district_info['D_NEXT_O_ID'], district_id, warehouse_id, order_line_num, ol_i_id,
                             ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist]
                self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_ORDER_LINE, ol_detail)

            if rollback:
                self.driver.transaction_rollback(cursor, conn)
            else:
                self.driver.transaction_commit(cursor, conn)
                total *= (1 - customer_info['C_DISCOUNT']) * (1 + warehouse_tax + district_info['D_TAX'])
                self.logger.debug(
                    "Transaction New-Order complete: Customer " + str(customer_id) + " of District " + str(
                        district_id) + " of Warehouse " + str(
                        warehouse_id) + " generate new order " + str(
                        district_info['D_NEXT_O_ID']) + " with total of " + str(total) + ".")

        except KeyboardInterrupt:
            self.driver.transaction_rollback(cursor, conn)

        except Exception as e:
            self.logger.warn(e)

    def payment(self, warehouse_id, remote_warehouse_id):
        cursor, conn = self.driver.get_conn()
        try:
            district_id = rand.number(1, config.DIST_PER_WARE)

            # 85%: paying through own warehouse (or there is only 1 warehouse)
            if warehouse_id == remote_warehouse_id or rand.rand_bool(85):
                customer_warehouse_id = warehouse_id
                customer_district_id = district_id
            # 15%: paying through another warehouse:
            else:
                customer_warehouse_id = remote_warehouse_id
                customer_district_id = rand.number(1, config.DIST_PER_WARE)

            # 60%: payment by last name
            if rand.rand_bool(60):
                customer_last = rand.makeRandomLastName(config.CUST_PER_DIST)
                sql = "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, " \
                      "C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM " \
                      "CUSTOMER WHERE C_W_ID = " + str(customer_warehouse_id) + " AND C_D_ID = " + str(customer_district_id) \
                      + " AND C_LAST = '" + str(customer_last) + "' ORDER BY C_FIRST"
                customer_info = self.driver.transaction_fetchone(cursor, conn, sql)

            # 40%: payment by id
            else:
                customer_id = rand.NURand(1023, 1, config.CUST_PER_DIST)
                sql = "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, " \
                      "C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM " \
                      "CUSTOMER WHERE C_W_ID = " + str(customer_warehouse_id) + " AND C_D_ID = " + str(customer_district_id) \
                      + " AND C_ID = " + str(customer_id)
                customer_info = self.driver.transaction_fetchone(cursor, conn, sql)

            h_amount = rand.fixedPoint(2, config.MIN_PAYMENT, config.MAX_PAYMENT)
            customer_id = customer_info["C_ID"]
            c_balance = customer_info["C_BALANCE"] - h_amount
            c_ytd_payment = customer_info["C_YTD_PAYMENT"] + h_amount
            c_payment_cnt = customer_info["C_PAYMENT_CNT"] + 1
            c_data = customer_info["C_DATA"]

            # get warehouse
            sql = "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = " + str(
                warehouse_id)
            warehouse_info = self.driver.transaction_fetchone(cursor, conn, sql)

            # get district
            sql = "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = " + str(
                warehouse_id) + " AND D_ID = " + str(district_id)
            district_info = self.driver.transaction_fetchone(cursor, conn, sql)

            # update warehouse balance
            sql = "UPDATE WAREHOUSE SET W_YTD = W_YTD + " + str(h_amount) + " WHERE W_ID = " + str(warehouse_id)
            self.driver.transaction_exec(cursor, conn, sql)

            # update district balance
            sql = "UPDATE DISTRICT SET D_YTD = D_YTD + " + str(h_amount) + " WHERE D_W_ID  = " + str(
                warehouse_id) + " AND D_ID = " + str(district_id)
            self.driver.transaction_exec(cursor, conn, sql)

            # update Customer Credit Information
            if customer_info['C_CREDIT'] == config.BAD_CREDIT:
                new_data = " ".join(map(str, [customer_id, customer_district_id, customer_warehouse_id, district_id,
                                              warehouse_id, h_amount]))
                c_data = (new_data + "|" + c_data)
                if len(c_data) > config.MAX_C_DATA:
                    c_data = c_data[:config.MAX_C_DATA]
                sql = "UPDATE CUSTOMER SET C_BALANCE = " + str(c_balance) + ", C_YTD_PAYMENT = " + str(
                    c_ytd_payment) + ", C_PAYMENT_CNT = " + str(c_payment_cnt) + ", C_DATA = " + str(
                    c_data) + " WHERE C_W_ID = " + str(customer_warehouse_id) + " AND C_D_ID = " + str(
                    customer_district_id) + " AND C_ID = " + str(customer_id)
                self.driver.transaction_exec(cursor, conn, sql)

            else:
                c_data = ""
                sql = "UPDATE CUSTOMER SET C_BALANCE = " + str(c_balance) + ", C_YTD_PAYMENT = " + str(
                    c_ytd_payment) + ", C_PAYMENT_CNT = " + str(c_payment_cnt) + " WHERE C_W_ID = " + str(
                    customer_warehouse_id) + " AND C_D_ID = " + str(customer_district_id) + " AND C_ID = " + str(
                    customer_id)
                self.driver.transaction_exec(cursor, conn, sql)

            # Concatenate w_name, four spaces, d_name
            h_data = "%s    %s" % (warehouse_info["W_NAME"], district_info["D_NAME"])
            h_date = datetime.now()
            h_detail = [customer_id, customer_district_id, customer_warehouse_id, district_id, warehouse_id, h_date,
                        h_amount, h_data]
            self.driver.transaction_insert(cursor, conn, config.TABLE_NAME_HISTORY, h_detail)

            self.driver.transaction_commit(cursor, conn)
            self.logger.debug(
                "Transaction Payment complete: Customer " + str(customer_info['C_ID']) + " of Warehouse" + str(
                    customer_warehouse_id) + " in District" + str(customer_district_id) + " generate payment of " + str(
                    h_amount) + ".")

        except KeyboardInterrupt:
            self.driver.transaction_rollback(cursor, conn)

        except Exception as e:
            self.logger.warn(e)

    def stock_level(self, warehouse_id):
        cursor, conn = self.driver.get_conn()
        try:
            district_id = rand.number(1, config.DIST_PER_WARE)
            threshold = rand.number(config.MIN_STOCK_LEVEL_THRESHOLD, config.MAX_STOCK_LEVEL_THRESHOLD)

            sql = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = " + str(warehouse_id) + " AND D_ID = " + str(district_id)
            district_info = self.driver.transaction_fetchone(cursor, conn, sql)

            o_id = district_info["D_NEXT_O_ID"]

            sql = "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = " + str(
                warehouse_id) + " AND OL_D_ID = " + str(district_id) + " AND OL_O_ID < " + str(
                o_id) + " AND OL_O_ID >= " + str((o_id - 20)) + " AND S_W_ID = " + str(
                warehouse_id) + " AND S_I_ID = OL_I_ID AND S_QUANTITY < " + str(threshold)
            count_info = self.driver.transaction_fetchone(cursor, conn, sql)

            self.driver.transaction_commit(cursor, conn)
            self.logger.debug("Transaction stock_level complete: Warehouse" + str(warehouse_id) + " in District" + str(
                district_id) + " ans " + str(count_info))

        except KeyboardInterrupt:
            self.driver.transaction_rollback(cursor, conn)

        except Exception as e:
            self.logger.warn(e)

    def order_status(self, warehouse_id):
        cursor, conn = self.driver.get_conn()
        try:
            district_id = rand.number(1, config.DIST_PER_WARE)

            # 60%: order status by last name
            if rand.rand_bool(60):
                c_last = rand.makeRandomLastName(config.CUST_PER_DIST)
                sql = "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = " + str(
                    warehouse_id) + " AND C_D_ID = " + str(district_id) + " AND C_LAST = '" + str(
                    c_last) + "' ORDER BY C_FIRST"
                customer_info = self.driver.transaction_fetchone(cursor, conn, sql)

            # 40%: order status by id
            else:
                c_id = rand.NURand(1023, 1, config.CUST_PER_DIST)
                sql = "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = " + str(
                    warehouse_id) + " AND C_D_ID = " + str(district_id) + " AND C_ID = " + str(c_id)
                customer_info = self.driver.transaction_fetchone(cursor, conn, sql)

            c_id = customer_info['C_ID']
            sql = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = " + str(
                warehouse_id) + " AND O_D_ID = " + str(district_id) + " AND O_C_ID = " + str(
                c_id) + " ORDER BY O_ID DESC LIMIT 1"
            order_info = self.driver.transaction_fetchone(cursor, conn, sql)

            ol_info = {}
            if order_info:
                sql = "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = " + str(
                    warehouse_id) + " AND OL_D_ID = " + str(district_id) + " AND OL_O_ID = " + str(order_info['O_ID'])
                ol_info = self.driver.transaction_fetchall(cursor, conn, sql)

            self.driver.transaction_commit(cursor, conn)
            self.logger.debug("Transaction Order_status Complete: Warehouse" + str(warehouse_id) + " District" + str(
                district_id) + " Customer" + str(c_id) + " gets order-line info " + str(ol_info[0]) + ".")

        except KeyboardInterrupt:
            self.driver.transaction_rollback(cursor, conn)

        except Exception as e:
            self.logger.warn(e)
