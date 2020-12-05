import os 
import time
import shutil
import config.config
from utils.myLogger import getCMDLogger
from utils.db_utils import mysqlDriver

def mkdir_job(dir_path):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path) #存在会报错

def check_dir():
    mkdir_job("./result") # 存当次收集到的csv数据和log文件
    mkdir_job("./archive/result") # 存往次收集到的csv数据和log文件

def mv_old_result():
    timeLabel = time.strftime("%m%d_%H%M", time.localtime())
    newDirName = "./archive/result/result_" + timeLabel
    if os.path.exists("./result"):
        shutil.move("./result",newDirName)
    check_dir()

def init_db_table():
    mysql = mysqlDriver.MySQLDriver()
    mysql.initDB()
    mysql.initTable(config.config.CREATE_TABLE_SQL)


if __name__ == "__main__":
    # getCMDLogger().info("Checking Dir!")
    # check_dir()
    # getCMDLogger().info("Moving Old Versions!")
    # mv_old_result()
    # init_db_table()