from TPC.drivers.abstractdriver import AbstractDriver
from TPC.utils.myLogger import get_cmd_logger
from TPC.config.mysql_config import *

from dbutils.pooled_db import PooledDB
import pymysql


class MysqlDriver(AbstractDriver):
    def __init__(self):
        super(MysqlDriver, self).__init__("MySQL")
        self.__pool = None
        self.logger = get_cmd_logger()
        conn, cursor = self.get_conn()
        conn.close()
        cursor.close()

    # 获取连接，无连接则创建连接池
    def __get_conn(self):
        if self.__pool is None:
            self.__pool = PooledDB(
                creator=DB_CREATOR,
                mincached=DB_MIN_CACHED,
                maxcached=DB_MAX_CACHED,
                maxshared=DB_MAX_SHARED,
                maxconnections=DB_MAX_CONNECTIONS,
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
        return self.__pool.connection(shareable=False)

    # 释放连接池资源
    def close(self):
        self.logger.info("Driver Closed!")
        self.__pool.close()

    # 从连接池中取出一个连接
    def get_conn(self):
        conn = self.__get_conn()
        cursor = conn.cursor(DB_CURSOR_TYPE)
        return cursor, conn

    # 从连接池中关闭连接
    @staticmethod
    def close_conn(cursor, conn):
        cursor.close()
        conn.close()

    # 从连接池外获取单个链接
    def get_cursor(self):
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

    # 初始化数据库
    def init_db(self):
        try:
            cursor, conn = self.get_conn()
            cursor.close()
            conn.close()
        except Exception as e:
            raise e

    # 初始化表
    def init_table(self, init_ddl):
        self.logger.info("Start Database Initialization!")
        conn, cursor = self.get_cursor()
        try:
            for sql in init_ddl:
                cursor.execute(sql)
            self.logger.info("Database Initialization Successes!")
        except Exception as e:
            self.logger.warn("Database Initialization Failed!")
            self.logger.warn(e)
        finally:
            cursor.close()
            conn.close()

    # 封装执行命令
    def execute(self, sql):
        cursor, conn = self.get_conn()  # 从连接池获取连接
        count = 0
        try:
            # count : 为改变的数据条数
            count = cursor.execute(sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.warn(e)
        return cursor, conn, count

    # get所有
    def fetch_all(self, fetch_input):
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(fetch_input)
            res = cursor.fetchall()
            return res
        except Exception as e:
            self.logger.warn(e)
            return -1
        finally:
            self.close_conn(cursor, conn)

    # get一条
    def fetch_one(self, fetch_input):
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(fetch_input)
            res = cursor.fetchone()
            return res
        except Exception as e:
            self.logger.warn(e)
            return -1
        finally:
            self.close_conn(cursor, conn)

    # 增加
    def insert(self, table_name, insert_input):
        sql = "INSERT INTO " + table_name + " VALUES (\"" + str(insert_input[0]) + '"'
        for i in range(1, len(insert_input)):
            sql = sql + ", \"" + str(insert_input[i]) + '"'
        sql = sql + ");"
        # self.logger.debug(sql)
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(sql)
            return count
        except Exception as e:
            self.logger.warn(e.with_traceback)
            return -1
        finally:
            self.close_conn(cursor, conn)

    # 删除
    def delete(self, delete_input):
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(delete_input)
            return count
        except Exception as e:
            self.logger.warn(e)
            return -1
        finally:
            self.close_conn(cursor, conn)

    # 删除全部
    def delete_all(self, table_name):
        sql = "delete from " + str(table_name) + ";"
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(sql)
            return count
        except Exception as e:
            self.logger.warn(e)
            return -1
        finally:
            self.close_conn(cursor, conn)

    # 更新
    def update(self, update_input):
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(update_input)
            return count
        except Exception as e:
            self.logger.warn(e)
            return -1
        finally:
            self.close_conn(cursor, conn)

    # 执行
    def exec(self, exec_input):
        cursor, conn = None, None
        try:
            cursor, conn, count = self.execute(exec_input)
            return count
        except Exception as e:
            self.close_conn(cursor, conn)
            self.logger.warn(e.with_traceback)
        return -1

    def transaction_exec(self, cursor, conn, sql, para=None):
        try:
            if para is not None:
                cursor.execute(sql, para)
            else:
                cursor.execute(sql)
        except Exception as e:
            self.logger.warn(e)
            conn.rollback()
            self.close_conn(cursor, conn)
            raise e

    def transaction_fetchone(self, cursor, conn, sql):
        try:
            cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            self.logger.warn(e)
            conn.rollback()
            self.close_conn(cursor, conn)
            raise e

    def transaction_fetchall(self, cursor, conn, sql):
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            self.logger.warn(e)
            conn.rollback()
            self.close_conn(cursor, conn)
            raise e

    def transaction_commit(self, cursor, conn):
        conn.commit()
        self.close_conn(cursor, conn)

    def transaction_rollback(self, cursor, conn):
        conn.rollback()
        self.close_conn(cursor, conn)

    def transaction_insert(self, cursor, conn, table_name, insert_input):
        sql = "INSERT INTO " + table_name + " VALUES (\"" + str(insert_input[0]) + '"'
        for i in range(1, len(insert_input)):
            sql = sql + ", \"" + str(insert_input[i]) + '"'
        sql = sql + ");"
        try:
            count = cursor.execute(sql)
            return count
        except Exception as e:
            self.logger.warn(e.with_traceback)
            conn.rollback()
            self.close_conn(cursor, conn)
            raise e
