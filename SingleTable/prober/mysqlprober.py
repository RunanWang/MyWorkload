import utils.myLogger
import drivers.mysqldriver as mysql
import config.config as constant
import threading
import pandas as pd
import time
import os
import signal

PROBE_SQL_LIST=[
    "select FIELD_07, sum(1) from TEST group by FIELD_07;",
    "select * from (select * from (select * from (select * from TEST order by FIELD_09)as K order by FIELD_08) as a join (select FIELD_07 as FFIELD_07, FIELD_08 as FFIELD_08, FIELD_09 as FFIELD_09 from TEST) as b where a.FIELD_08=b.FFIELD_08) as c ORDER BY FIELD_09 LIMIT 10;"
]

logger = utils.myLogger.getCMDLogger()

# 分析当前数据库环境，执行sql并生成相应的sql分析
# 生成结果字典返回，k为对应名称，v为对应值
def probe(sql):

    return result


# 对字典进行保存
def save_result(result):


# 根据probe-list
def probe_monitor():
