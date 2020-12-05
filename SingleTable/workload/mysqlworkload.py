import utils.myRand as myRand
import config.config as constant

class MysqlWorkload(object):
    def __init__(self):
        self.workload = {}
        ################################################
        # 下面配置每一种workload在每隔多少个时间单位执行1次
        ################################################
        self.workload['workload_insert'] = 1
        self.workload['workload_updateByID'] = 1
        self.workload['workload_countTotal'] = 1

    def get_workload(self):
        return self.workload

    ####################################
    # 下面配置每一种Workload的具体生成函数
    ####################################
    # 插入一条随机数据
    def workload_insert(self):
        insertSQL = """INSERT INTO TEST (FIELD_01,FIELD_02,FIELD_03,FIELD_04,FIELD_05,FIELD_06,FIELD_07,FIELD_08,FIELD_09,FIELD_10) VALUES"""
        insertContent = "("
        for i in range(1,5):
            insertContent = insertContent + "\'"+ myRand.randomString(constant.MIN_SHORT_STRING_LEN,constant.MAX_SHORT_STRING_LEN,'A',26) + "\', "
        for i in range(5,7):
            insertContent = insertContent + "\'"+ myRand.randomString(constant.MIN_LONG_STRING_LEN,constant.MAX_LONG_STRING_LEN,'A',26) + "\', "
        insertContent = insertContent + "\'"+ myRand.randomCity(constant.CITY) + "\', "
        for i in range(8,10):
            insertContent = insertContent + str(myRand.number(constant.MIN_BIG_INT,constant.MAX_BIG_INT)) + ","
        insertContent = insertContent + str(myRand.number(constant.MIN_SMALL_INT,constant.MAX_SMALL_INT)) + ")"
        insertSQL = insertSQL + insertContent
        return insertSQL

    # 更改一条记录(FIELD_09)
    def workload_updateByID(self):
        sql = """UPDATE TEST SET FIELD_09 = FIELD_09 + 1 where T_ID = (select a.count from (SELECT count(*) as count from TEST) as a)""" 
        return sql

    # 记录总数
    def workload_countTotal(self):
        sql = "select count(*) from TEST"
        return sql

    # 按ID查找(唯一的索引)
    def workload_searchByID(self):
        sql = "select * from TEST where T_ID = " + str(myRand.number(1,100))
        return sql

    # 查城市
    def workload_searchCountPerCity(self):
        sql = "select FIELD_07, sum(1) from TEST group by FIELD_07"
        return sql

    # 查字符串-like
    def workload_searchF1(self):
        sql = "select * from TEST where FIELD_01 LIKE '%A%'"
        return sql

    # 查字符串-distinct
    def workload_searchF2(self):
        sql = "select distinct FIELD_02 from TEST"
        return sql
