class AbstractDriver(object):
    def __init__(self, name):
        self.name = name
        self.driver_name = "%sDriver" % self.name.title()
        
    def __str__(self):
        return self.driver_name

    def initDB(self):
        """初始化连接池"""
        raise NotImplementedError("%s does not implement initDB!" % (self.driver_name))

    def initTable(self, input):
        """初始化表"""
        raise NotImplementedError("%s does not implement initTable!" % (self.driver_name))

    def getCursor(self):
        """连接数据库"""
        raise NotImplementedError("%s does not implement connect!" % (self.driver_name))

    def insert(self, input):
        """向数据库中插入一条信息，信息包含在input中"""
        return None
    
    def delete(self, input):
        """从数据库中删除一条信息，信息包含在input中"""
        return None
    
    def update(self, input):
        """从数据库中更改一条信息，信息包含在input中"""
        return None
    
    def getall(self, input):
        """从数据库中取全部信息，信息包含在input中"""
        return None