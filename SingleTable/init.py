import os 
import time
import shutil
import config.config
from utils.myLogger import getCMDLogger

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

def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass

def init_db_table(name):
    driverClass = createDriverClass(name)
    driver = driverClass()
    driver.initDB()
    driver.initTable(config.config.CREATE_TABLE_SQL)
