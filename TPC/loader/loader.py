from TPC.utils.myLogger import get_cmd_logger
import TPC.utils.rand as rand
import TPC.config.config as config
import multiprocessing


class Loader(object):
    def __init__(self, name):
        self.logger = get_cmd_logger()
        self.item_counter = multiprocessing.Value("i", 0, lock=True)
        driver_class = self.create_driver_class(name)
        self.driver = driver_class()

    # 寻找name对应的driver
    @staticmethod
    def create_driver_class(name):
        full_name = "%sDriver" % name.title()
        mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
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

    # 生成Item插入内容
    def generate_item(self, item_id, original):
        i_id = item_id
        i_im_id = rand.number(config.MIN_IM, config.MAX_IM)
        i_name = rand.astring(config.MIN_I_NAME, config.MAX_I_NAME)
        i_price = rand.fixedPoint(config.MONEY_DECIMALS, config.MIN_PRICE, config.MAX_PRICE)
        i_data = rand.astring(config.MIN_I_DATA, config.MAX_I_DATA)
        if original:
            i_data = self.generate_original_string(i_data)
        return [i_id, i_im_id, i_name, i_price, i_data]

    # 把Item插进db
    def load_items(self):
        # Select 10% of the rows to be marked "original"
        original_rows = rand.selectUniqueIds(int(config.NUM_ITEMS / 10), 1, config.NUM_ITEMS)
        # Load all of the items
        for i in range(1, config.NUM_ITEMS + 1):
            original = (i in original_rows)
            item_detail = self.generate_item(i + self.item_counter.value, original)
            if i % 100 == 0 or i == config.NUM_ITEMS:
                self.logger.debug("LOAD - %s: %d / %d" % (config.TABLE_NAME_ITEM, i, config.NUM_ITEMS))
            self.driver.insert(config.TABLE_NAME_ITEM, item_detail)
        self.item_counter.value += config.NUM_ITEMS

    def monitor(self):
        self.load_items()
        self.load_items()


if __name__ == "__main__":
    loader = Loader("mysql")
    loader.monitor()
