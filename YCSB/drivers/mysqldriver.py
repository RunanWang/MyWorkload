from drivers.abstractdriver import *
from config.mysql_config import *
from utils.myLogger import getCMDLogger
from dbutils.pooled_db import PooledDB

import pymysql

class MysqlDriver(AbstractDriver):
    def __init__(self):
        super(MysqlDriver,self).__init__("MySQL")
        self.__pool = None
        self.logger = getCMDLogger()
    
    # 获取连接，无连接则创建连接池
    def __getconn(self):
        if self.__pool is None:
            self.__pool = PooledDB(
                creator=DB_CREATOR,
                mincached=DB_MIN_CACHED,
                maxcached=DB_MAX_CACHED,
                maxshared=DB_MAX_SHARED,
                maxconnections=DB_MAX_CONNECYIONS,
                blocking=DB_BLOCKING,
                maxusage=DB_MAX_USAGE,
                setsession=DB_SET_SESSION,
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                passwd=DB_PASSWORD,
                db=DB_DBNAME,
                use_unicode=False,
                charset=DB_CHARSET
            )
            self.logger.info("Driver Pool Established!")
        return self.__pool.connection(shareable = False)

    # 释放连接池资源
    def close(self):
        self.logger.info("Driver Closed!")
        self.__pool.close()
    
    # 从连接池中取出一个连接
    def getconn(self):
        conn = self.__getconn()
        cursor = conn.cursor(DB_CURSOR_TYPE)
        return cursor, conn

    # 从连接池外获取单个链接
    def getCursor(self):
        conn = pymysql.connect(
            host=DB_HOST, 
            port=DB_PORT, 
            user=DB_USER, 
            password=DB_PASSWORD, 
            database=DB_DBNAME, 
            charset=DB_CHARSET
            )
        cursor = conn.cursor(DB_CURSOR_TYPE)
        return conn, cursor
    
    def initDB(self):
        """初始化数据库"""
        try:
            cursor, conn = self.getconn()
            cursor.close()
            conn.close()
        except Exception as e:
            raise(e)

    def initTable(self, input):
        """初始化表"""
        self.logger.info("Start Database Initialization!")
        conn, cursor = self.getCursor()
        try:
            for sql in input:
                cursor.execute(sql)
            self.logger.info("Database Initialization Successed!")
        except Exception as e:
            self.logger.warn("Database Initialization Failed!")
            self.logger.warn(e)
        finally:
            cursor.close()
            conn.close()

    # 封装执行命令
    def execute(self, sql):
        """
        【主要判断是否有参数和是否执行完就释放连接】
        :param sql: 字符串类型，sql语句
        :return: 返回连接conn和游标cursor
        """
        cursor, conn = self.getconn()  # 从连接池获取连接
        count = 0
        try:
            # count : 为改变的数据条数
            count = cursor.execute(sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
        return cursor, conn, count

    def closeConn(self, cursor, conn):
        cursor.close()
        conn.close()

    # get所有
    def getall(self, input):
        try:
            cursor, conn, count = self.execute(input)
            res = cursor.fetchall()
            return res
        except Exception as e:
            self.logger.warn(e)
            return count
        finally:
            self.closeConn(cursor, conn)

    # 增加
    def insert(self, input):
        try:
            cursor, conn, count = self.execute(input)
            return count
        except Exception as e:
            self.logger.warn(e)
            return count
        finally:
            self.closeConn(cursor, conn)

    # 删除
    def delete(self, input):
        try:
            cursor, conn, count = self.execute(input)
            return count
        except Exception as e:
            self.logger.warn(e)
            return count
        finally:
            self.closeConn(cursor, conn)

    # 更新
    def update(self, input):
        try:
            cursor, conn, count = self.execute(input)
            return count
        except Exception as e:
            self.logger.warn(e)
            return count
        finally:
            self.closeConn(cursor, conn)

    # 执行
    def exec(self, input):
        try:
            cursor, conn, count = self.execute(input)
            return count
        except Exception as e:
            try:
                self.closeConn(cursor, conn)
            except:
                pass
            self.logger.warn(e.with_traceback())
            return -1

        