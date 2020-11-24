import pymysql
import logging
import myRand
import constant
import threading

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# 数据库链接
def makeConnect():
    conn = pymysql.connect(host="127.0.0.1", port=36036, user="root", password="1", database="test", charset="utf8")
    return conn

# 初始化TEST表
def initialization():
    conn = makeConnect()
    logger.debug("Start Initialization!")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS TEST")
    logger.debug("TEST table dropped!")
    createTableSQL = """CREATE TABLE TEST (
        T_ID INTEGER AUTO_INCREMENT,
        FIELD_01 VARCHAR(32) DEFAULT NULL,
        FIELD_02 VARCHAR(32) DEFAULT NULL,
        FIELD_03 VARCHAR(32) DEFAULT NULL,
        FIELD_04 VARCHAR(32) DEFAULT NULL,
        FIELD_05 VARCHAR(32) DEFAULT NULL,
        FIELD_06 VARCHAR(32) DEFAULT NULL,
        FIELD_07 VARCHAR(32) DEFAULT NULL,
        FIELD_08 INTEGER DEFAULT '0' NOT NULL,
        FIELD_09 INTEGER DEFAULT '0' NOT NULL,
        FIELD_10 INTEGER DEFAULT '0' NOT NULL,
        PRIMARY KEY (T_ID)
        )"""
    cursor.execute(createTableSQL)
    logger.debug("TEST table established!")
    conn.close()

# 插入一条随机数据
def insertOne():
    conn = makeConnect()
    cursor = conn.cursor()
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
    logger.debug(insertSQL)
    try:
        cursor.execute(insertSQL)
        conn.commit()
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 更改一条记录(FIELD_09)
def updateByID(id):
    conn = makeConnect()
    cursor = conn.cursor()
    sql = """UPDATE TEST SET FIELD_09 = FIELD_09 + 1 where T_ID = %s""" %(str(id))
    logger.debug(sql)
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 记录总数
def countTotal():
    conn = makeConnect()
    cursor = conn.cursor()
    sql = "select count(*) from TEST"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        total = results[0][0]
        logger.debug(total)
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()
    return total

# 全表查询
def searchAll():
    conn = makeConnect()
    cursor = conn.cursor()
    sql = "select * from TEST"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.debug(results)
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 按ID查找(唯一的索引)
def searchByID(id):
    conn = makeConnect()
    cursor = conn.cursor()
    sql = "select * from TEST where T_ID = " + str(id)
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.debug(results)
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 查城市
def searchCountPerCity():
    conn = makeConnect()
    cursor = conn.cursor()
    sql = "select FIELD_07, sum(1) from TEST group by FIELD_07"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.debug(results)
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 查字符串-like
def searchF1():
    conn = makeConnect()
    cursor = conn.cursor()
    sql = "select * from TEST where FIELD_01 LIKE '%A%'"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.debug(results)
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 查字符串-distinct
def searchF2():
    conn = makeConnect()
    cursor = conn.cursor()
    sql = "select distinct FIELD_02 from TEST"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.debug(results)
    except Exception:
        logger.warn(Exception)
        conn.rollback()
    conn.close()

# 加载loadnum组数据，loader的log编号为c
def loadData(c, loadNum):
    for i in range(1, loadNum):
        if i%100==0:
            logmsg = str(c)+ "loadData: loaded num = " + str(i)
            logger.info(logmsg)
        insertOne()

# 20线程加载
def multiThreadLoad(loadNum):
    for i in range(20):
        t = threading.Thread(target=loadData, args=(i,int(loadNum/20),))
        t.start()
    logger.info("load finish!")


if __name__ == "__main__":
    # initialization()
    # insertOne()
    # searchAll()
    searchByID(countTotal()+1)
    searchF1()
    # updateByID(countTotal()-1)
    # multiThreadLoad(1000)