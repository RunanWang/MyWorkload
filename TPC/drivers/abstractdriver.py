class AbstractDriver(object):
    def __init__(self, name):
        self.name = name
        self.driver_name = "%sDriver" % self.name.title()

    def __str__(self):
        return self.driver_name

    def init_db(self):
        """初始化连接池"""
        raise NotImplementedError("%s does not implement initDB!" % (self.__str__()))

    def init_table(self, init_table_input):
        """初始化表"""
        raise NotImplementedError("%s does not implement initTable!" % (self.__str__()))

    def get_cursor(self):
        """连接数据库"""
        raise NotImplementedError("%s does not implement get_cursor!" % (self.__str__()))

    def insert(self, table_name, insert_input):
        """向数据库中插入一条信息，信息包含在input中"""
        return None

    def delete(self, delete_input):
        """从数据库中删除一条信息，信息包含在input中"""
        return None

    def update(self, update_input):
        """从数据库中更改一条信息，信息包含在input中"""
        return None

    def fetch_all(self, get_all_input):
        """从数据库中取全部信息，信息包含在input中"""
        return None

    def exec(self, exec_input):
        """从数据库中更改一条信息，信息包含在input中"""
        return None
