import utils.myLogger
import drivers.mysqldriver as mysql
import config.config as constant
import threading
import pandas as pd
import time
import os
import signal

logger = utils.myLogger.getCMDLogger()

# 把explain语句中的信息解析出来
# 输出扫描行数和结果行数
def parseExplain(results):
    rowsScan = 0
    rowsOut = 0
    for i in range(0,len(results)):
        dictExplain = results[i]
        rowsScan = rowsScan + dictExplain.get('rows')
        rowsOut = rowsOut + dictExplain.get('rows') * dictExplain.get('filtered')/100
    logger.debug("rowScan: "+ str(rowsScan))
    logger.debug("rowOut: "+ str(rowsOut))
    return rowsScan, rowsOut

# 把Profile中的信息解析成字典输出
def parseProfile(results):
    dictProfile = {}
    total = 0
    for i in range(0,len(results)):
        try:
            dictProfile[results[i]['Status']] = results[i]['Duration'] + dictProfile[results[i]['Status']]
        except: 
            dictProfile[results[i]['Status']] = results[i]['Duration']
        total = total + results[i]['Duration']
    dictProfile['total'] = total
    return dictProfile

# 把profile信息汇总
def profile2dict(dictProfile, totalDict):
    if len(totalDict)==0:
        for k, v in dictProfile.items():
            totalDict[k] = []
    for k, v in dictProfile.items():
        try:
            totalDict[k].append(v)
        except Exception:
            pass
            # logger.warn(Exception.with_traceback())
    return totalDict

# init totalDict
def initTotalDict(filename):
    if os.path.exists(filename):
        csv_data = pd.read_csv(filename)
        totalDict = csv_data.to_dict(orient="list")
    else:
        totalDict = {}
    return totalDict

# 信息汇总
def summaryProfile(statusResult, explainResults, profileResults):
    rowsScan, rowsOut = parseExplain(explainResults)
    dictProfile = parseProfile(profileResults)
    dictProfile['rowScan'] = rowsScan
    dictProfile['rowOut'] = rowsOut
    dictProfile['Timestamp'] = time.time()
    return dictProfile


def getStatus(cursor):
    status = {}
    cursor.execute('''show global status like "%threads_run%";''')
    result = cursor.fetchall()
    for item in result:
        status[item['Variable_name']] = item['Value']
    logger.debug(result)
    return status

# 一个探针。
# 执行SQL并输出执行语句的相关信息，存入csv中。
def probe(lock, sql, filename):
    totalDict = initTotalDict(filename)
    mysqlD = mysql.MysqlDriver()
    conn, cursor = mysqlD.getCursor()
    explainSql = "explain " + sql
    try:
        # 记录status信息
        cursor.execute("show status;")
        statusResults = cursor.fetchall()

        # 记录explain结果
        cursor.execute(explainSql)
        explainResults = cursor.fetchall()

        # 记录profile结果
        cursor.execute("SET profiling=1;")
        cursor.execute(sql)
        cursor.execute("show profile;")
        profileResults = cursor.fetchall()

        # 整合结果
        dictProfile = summaryProfile(statusResults, explainResults, profileResults)
        totalDict = profile2dict(dictProfile, totalDict)

        # 输出
        df = pd.DataFrame(totalDict)
        lock.acquire()
        df.to_csv(filename, index=False, sep=',')
        lock.release()

    except Exception:
        logger.warn(Exception.with_traceback())
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

# terminate处理
def sigintHandler(signum, frame):
    logger.warn("probe" + "terminate!")
    exit()

# 定期进行probe
def cronProbe(i):
    count = 0
    lock = threading.Lock()
    signal.signal(signal.SIGTERM, sigintHandler)
    while True:
        sql = constant.PROBE_SQL_LIST[i]
        filename = constant.PROBE_FILE_PREFIX + str(i) + constant.PROBE_FILE_SUFFIX
        thread_sql = threading.Thread(target=probe,args=(lock,sql,filename,))
        thread_sql.start()
        count = count + 1
        logger.info("probe" + str(i) + "has been executed for:" + str(count) + "times.")
        time.sleep(constant.PROBE_INTERNAL_TIME)
