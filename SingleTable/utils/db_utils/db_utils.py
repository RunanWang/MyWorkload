from dbutils.pooled_db import PooledDB
import db_config as config
import pymysql

class MyConnectionPool(object):
    __pool = None

    # 创建数据库连接conn和游标cursor
    def __init__(self):
        self.conn = self.__getconn()
        self.cursor = self.conn.cursor()
    
    # def __enter__(self):
    #     self.conn = self.__getconn()
    #     self.cursor = self.conn.cursor()

    # 创建数据库连接池
    def __getconn(self):
        if self.__pool is None:
            self.__pool = PooledDB(
                creator=config.DB_CREATOR,
                mincached=config.DB_MIN_CACHED,
                maxcached=config.DB_MAX_CACHED,
                maxshared=config.DB_MAX_SHARED,
                maxconnections=config.DB_MAX_CONNECYIONS,
                blocking=config.DB_BLOCKING,
                maxusage=config.DB_MAX_USAGE,
                setsession=config.DB_SET_SESSION,
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                passwd=config.DB_PASSWORD,
                db=config.DB_DBNAME,
                use_unicode=False,
                charset=config.DB_CHARSET
            )
        return self.__pool.connection()

    # 释放连接池资源
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    # 从连接池中取出一个连接
    def getconn(self):
        conn = self.__getconn()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        return cursor, conn


# 获取连接池,实例化
def get_connection():
    return MyConnectionPool()

