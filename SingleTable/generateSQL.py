import logging
import myRand
import constant

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# 插入一条随机数据
def insertOne():
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
def updateByID():
    sql = """UPDATE TEST SET FIELD_09 = FIELD_09 + 1 where T_ID = (select a.count from (SELECT count(*) as count from TEST) as a)""" 
    return sql

# 记录总数
def countTotal():
    sql = "select count(*) from TEST"
    return sql

# 全表查询
def searchAll():
    sql = "select * from TEST"
    return sql

# 按ID查找(唯一的索引)
def searchByID():
    sql = "select * from TEST where T_ID = " + str(myRand.number(1,100))
    return sql

# 查城市
def searchCountPerCity():
    sql = "select FIELD_07, sum(1) from TEST group by FIELD_07"
    return sql

# 查字符串-like
def searchF1():
    sql = "select * from TEST where FIELD_01 LIKE '%A%'"
    return sql

# 查字符串-distinct
def searchF2():
    sql = "select distinct FIELD_02 from TEST"
    return sql
