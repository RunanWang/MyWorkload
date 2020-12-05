from db_utils import get_connection
import logging
import pymysql

class MySqLExec(object):

    def __init__(self):
        self.db = get_connection()  # 从数据池中获取连接
        logging.basicConfig(level = logging.DEBUG,format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()


    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'inst'):  # 单例
            cls.inst = super(MySqLExec, cls).__new__(cls, *args, **kwargs)
        return cls.inst

    # 封装执行命令
    def execute(self, sql, param=None, autoclose=False):
        """
        【主要判断是否有参数和是否执行完就释放连接】
        :param sql: 字符串类型，sql语句
        :param param: sql语句中要替换的参数"select %s from tab where id=%s" 其中的%s就是参数
        :param autoclose: 是否关闭连接
        :return: 返回连接conn和游标cursor
        """
        cursor, conn = self.db.getconn()  # 从连接池获取连接
        count = 0
        try:
            # count : 为改变的数据条数
            if param:
                count = cursor.execute(sql, param)
            else:
                count = cursor.execute(sql)
            conn.commit()
            if autoclose:
                self.close(cursor, conn)
        except Exception as e:
            pass
        return cursor, conn, count
    
    def close(self, cursor, conn):
        """释放连接归还给连接池"""
        cursor.close()
        conn.close()
    
    # fetch所有
    def fetchall(self, sql, param=None):
        try:
            cursor, conn, count = self.execute(sql, param)
            res = cursor.fetchall()
            return res
        except Exception as e:
            self.logger.warn(e)
            self.close(cursor, conn)
            return count

    # 增加
    def insert(self, sql, param):
        try:
            cursor, conn, count = self.execute(sql, param)
            conn.commit()
            self.close(cursor, conn)
            return count
        except Exception as e:
            self.logger.warn(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 删除
    def delete(self, sql, param=None):
        try:
            cursor, conn, count = self.execute(sql, param)
            self.close(cursor, conn)
            return count
        except Exception as e:
            self.logger.warn(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 更新
    def update(self, sql, param=None):
        try:
            cursor, conn, count = self.execute(sql, param)
            conn.commit()
            self.close(cursor, conn)
            return count
        except Exception as e:
            self.logger.warn(e)
            conn.rollback()
            self.close(cursor, conn)
            return count



if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG,format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    db = MySqLExec()
    sql1 = 'select count(*) from TEST;'
    ret = db.fetchall(sql=sql1)
    logger.debug(ret)
    
    
    