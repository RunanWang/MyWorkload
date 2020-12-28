import drivers
import utils.myLogger
import utils.rand as rand
import config.config as config
import multiprocessing


class Loader(object):
    def __init__(self, name):
        self.logger = utils.myLogger.getCMDLogger()
        self.item_counter = multiprocessing.Value("i", 0, lock=True)
        driverClass = self.createDriverClass(name)
        self.driver = driverClass()
        cursor, conn = self.driver.getconn()
        cursor.close()
        conn.close()

    # 寻找name对应的driver
    def createDriverClass(self, name):
        full_name = "%sDriver" % name.title()
        mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
        klass = getattr(mod, full_name)
        return klass

    # 把Original标记填满
    def fillOriginal(self, data):
        originalLength = len(config.ORIGINAL_STRING)
        position = rand.number(0, len(data) - originalLength)
        out = data[:position] + config.ORIGINAL_STRING + data[position + originalLength:]
        assert len(out) == len(data)
        return out

    # 生成Item插入内容
    def generateItem(self, id, original):
        i_id = id
        i_im_id = rand.number(config.MIN_IM, config.MAX_IM)
        i_name = rand.astring(config.MIN_I_NAME, config.MAX_I_NAME)
        i_price = rand.fixedPoint(config.MONEY_DECIMALS, config.MIN_PRICE, config.MAX_PRICE)
        i_data = rand.astring(config.MIN_I_DATA, config.MAX_I_DATA)
        if original: i_data = self.fillOriginal(i_data)

        return [i_id, i_im_id, i_name, i_price, i_data]

    # 把Item插进db
    def loadItems(self):
        ## Select 10% of the rows to be marked "original"
        originalRows = rand.selectUniqueIds(int(config.NUM_ITEMS / 10), 1, config.NUM_ITEMS)
        ## Load all of the items
        for i in range(1, config.NUM_ITEMS + 1):
            original = (i in originalRows)
            input = self.generateItem(i + self.item_counter.value, original)
            if i % 100 == 0 or i == config.NUM_ITEMS:
                self.logger.debug("LOAD - %s: %d / %d" % (config.TABLENAME_ITEM, i, config.NUM_ITEMS))
            self.driver.insert(config.TABLENAME_ITEM, input)
        self.item_counter.value += config.NUM_ITEMS
    
    def loadWarehouse(self, w_id):
        # self.logger.debug("LOAD - %s: %d / %d" % (constants.TABLENAME_WAREHOUSE, w_id, len(self.w_ids)))
        
        ## WAREHOUSE
        w_tuples = [ self.generateWarehouse(w_id) ]
        self.handle.loadTuples(constants.TABLENAME_WAREHOUSE, w_tuples)

        ## DISTRICT
        d_tuples = [ ]
        for d_id in range(1, self.scaleParameters.districtsPerWarehouse+1):
            d_next_o_id = self.scaleParameters.customersPerDistrict + 1
            d_tuples = [ self.generateDistrict(w_id, d_id, d_next_o_id) ]
            
            c_tuples = [ ]
            h_tuples = [ ]
            
            ## Select 10% of the customers to have bad credit
            selectedRows = rand.selectUniqueIds(self.scaleParameters.customersPerDistrict / 10, 1, self.scaleParameters.customersPerDistrict)
            
            ## TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
            ## is a c_id field, it seems to make sense to have it be a permutation of the
            ## customers. For the "real" thing this will be equivalent
            cIdPermutation = [ ]

            for c_id in range(1, self.scaleParameters.customersPerDistrict+1):
                badCredit = (c_id in selectedRows)
                c_tuples.append(self.generateCustomer(w_id, d_id, c_id, badCredit, True))
                h_tuples.append(self.generateHistory(w_id, d_id, c_id))
                cIdPermutation.append(c_id)
            ## FOR
            assert cIdPermutation[0] == 1
            assert cIdPermutation[self.scaleParameters.customersPerDistrict - 1] == self.scaleParameters.customersPerDistrict
            shuffle(cIdPermutation)
            
            o_tuples = [ ]
            ol_tuples = [ ]
            no_tuples = [ ]
            
            for o_id in range(1, self.scaleParameters.customersPerDistrict+1):
                o_ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
                
                ## The last newOrdersPerDistrict are new orders
                newOrder = ((self.scaleParameters.customersPerDistrict - self.scaleParameters.newOrdersPerDistrict) < o_id)
                o_tuples.append(self.generateOrder(w_id, d_id, o_id, cIdPermutation[o_id - 1], o_ol_cnt, newOrder))

                ## Generate each OrderLine for the order
                for ol_number in range(0, o_ol_cnt):
                    ol_tuples.append(self.generateOrderLine(w_id, d_id, o_id, ol_number, self.scaleParameters.items, newOrder))
                ## FOR

                ## This is a new order: make one for it
                if newOrder: no_tuples.append([o_id, d_id, w_id])
            ## FOR
            
            self.handle.loadTuples(constants.TABLENAME_DISTRICT, d_tuples)
            self.handle.loadTuples(constants.TABLENAME_CUSTOMER, c_tuples)
            self.handle.loadTuples(constants.TABLENAME_ORDERS, o_tuples)
            self.handle.loadTuples(constants.TABLENAME_ORDER_LINE, ol_tuples)
            self.handle.loadTuples(constants.TABLENAME_NEW_ORDER, no_tuples)
            self.handle.loadTuples(constants.TABLENAME_HISTORY, h_tuples)
            self.handle.loadFinishDistrict(w_id, d_id)
        ## FOR
        
        ## Select 10% of the stock to be marked "original"
        s_tuples = [ ]
        selectedRows = rand.selectUniqueIds(self.scaleParameters.items / 10, 1, self.scaleParameters.items)
        total_tuples = 0
        for i_id in range(1, self.scaleParameters.items+1):
            original = (i_id in selectedRows)
            s_tuples.append(self.generateStock(w_id, i_id, original))
            if len(s_tuples) >= self.batch_size:
                logging.debug("LOAD - %s [W_ID=%d]: %5d / %d" % (constants.TABLENAME_STOCK, w_id, total_tuples, self.scaleParameters.items))
                self.handle.loadTuples(constants.TABLENAME_STOCK, s_tuples)
                s_tuples = [ ]
            total_tuples += 1
        ## FOR
        if len(s_tuples) > 0:
            logging.debug("LOAD - %s [W_ID=%d]: %5d / %d" % (constants.TABLENAME_STOCK, w_id, total_tuples, self.scaleParameters.items))
            self.handle.loadTuples(constants.TABLENAME_STOCK, s_tuples)

    def monitor(self):
        self.loadItems()
        self.loadItems()


if __name__ == "__main__":
    loader = Loader("mysql")
    loader.monitor()
