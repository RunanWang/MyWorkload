import utils.myLogger
import drivers.mysqldriver as mysql
import config.config
import multiprocessing
import csv
import time
import signal

PROBE_SQL_LIST=[
    "select FIELD_07, sum(1) from TEST group by FIELD_07;",
    "select * from (select * from (select * from (select * from TEST order by FIELD_09)as K order by FIELD_08) as a join (select FIELD_07 as FFIELD_07, FIELD_08 as FFIELD_08, FIELD_09 as FFIELD_09 from TEST) as b where a.FIELD_08=b.FFIELD_08) as c ORDER BY FIELD_09 LIMIT 10;"
]

headers = [
    'plan', 'rowScan', 'rowOut', 'startTime', 'oTableTime', 'cTableTime', 'transHookTime', 
    'lockTime', 'initTime','optimizeTime', 'statTime', 'prepareTime', 'execTime', 'endTime',
    'qEndTime', 'commitTime', 'crTmpTime', 'rmTmpTime', 'freeTime', 'cleanTime', 'totalTime',
    'timestamp'
    ]
mysql = mysql.MysqlDriver()
logger = utils.myLogger.getCMDLogger()

# 把explain语句中的信息解析出来
# 输出扫描行数和结果行数
def parseExplain(resp):
    dictExplain = {}
    rowsScan = 0
    rowsOut = 0
    plan = ''
    # logger.debug(resp)
    for i in range(0,len(resp)):
        dictExplain = resp[i]
        for k, v in dictExplain.items():
            if v == None:
                dictExplain[k] = 'None'
        rowsScan = rowsScan + dictExplain.get('rows')
        rowsOut = rowsOut + dictExplain.get('rows') * dictExplain.get('filtered')/100
        if plan!='':
            plan = plan + "+" + str(dictExplain.get('id')) + "-" + dictExplain.get('select_type') + "-" + dictExplain.get('table') + "-" + dictExplain.get('type') + "-" + dictExplain.get('key') + "-" + dictExplain.get('Extra').replace(' ','')
        else:
            plan = str(dictExplain.get('id')) + "-" + dictExplain.get('select_type') + "-" + dictExplain.get('table') + "-" + dictExplain.get('type') + "-" + dictExplain.get('key') + "-" + dictExplain.get('Extra').replace(' ','')
    dictExplain['rowsScan'] = rowsScan
    dictExplain['rowsOut'] = rowsOut
    dictExplain['plan'] = plan
    return dictExplain


# 把Profile中的信息解析成字典输出
def parseProfile(resp):
    dictProfile = {}
    total = 0
    for i in range(0,len(resp)):
        try:
            dictProfile[resp[i]['Status']] = resp[i]['Duration'] + dictProfile[resp[i]['Status']]
        except: 
            dictProfile[resp[i]['Status']] = resp[i]['Duration']
        total = total + resp[i]['Duration']
    dictProfile['totalTime'] = total
    return dictProfile


# 分析当前数据库环境，执行sql并生成相应的sql分析
# 生成结果压入队列，k为对应名称，v为对应值
def probe(i, queue):
    sql = PROBE_SQL_LIST[i]
    conn, cursor = mysql.getCursor()
    try:
        # 记录explain结果
        explainSql = "explain " + sql
        cursor.execute(explainSql)
        explainResp = cursor.fetchall()

        # 记录profile结果
        cursor.execute("SET profiling=1;")
        cursor.execute(sql)
        cursor.execute("show profile;")
        profileResp = cursor.fetchall()

        # 打包所有结果
        result_packer(i, explainResp, profileResp, queue)
    except Exception:
        logger.debug(Exception.with_traceback())
        conn.rollback()
    finally:
        conn.close()
        cursor.close()


# 打包器
def result_packer(i, explainResp, profileResp, queue):
    result = {}
    content = {}
    dictExplain = parseExplain(explainResp)
    content['timestamp'] = time.time()
    content['plan'] = dictExplain['plan']
    content['rowScan'] = dictExplain['rowsScan']
    content['rowOut'] = dictExplain['rowsOut']
    
    dictProfile = parseProfile(profileResp)
    content['totalTime'] = dictProfile['totalTime']
    content['lockTime'] = dictProfile['System lock']
    content['startTime'] = dictProfile['starting']
    content['transHookTime'] = dictProfile['Executing hook on transaction ']
    content['oTableTime'] = dictProfile['Opening tables']
    content['initTime'] = dictProfile['init']

    content['optimizeTime'] = dictProfile['optimizing']
    content['statTime'] = dictProfile['statistics']
    content['prepareTime'] = dictProfile['preparing']
    content['execTime'] = dictProfile['executing']
    content['endTime'] = dictProfile['end']
    content['qEndTime'] = dictProfile['query end']
    content['commitTime'] = dictProfile['waiting for handler commit']
    content['rmTmpTime'] = dictProfile['removing tmp table']
    content['crTmpTime'] = dictProfile['Creating tmp table']
    content['cTableTime'] = dictProfile['closing tables']
    content['freeTime'] = dictProfile['freeing items']
    content['cleanTime'] = dictProfile['cleaning up']

    result['id'] = i
    result['content'] = content
    queue.put(result)


# 把result字典保存至num对应的文件/数据库中
def save_result(queue):
    logger.info("Probe Writer Started!!")
    while True:
        try:
            result = queue.get()
            cont_id = result['id']
            cont = result['content']
            filename = config.config.PROBE_FILE_PREFIX + str(cont_id) + config.config.PROBE_FILE_SUFFIX
            cont_in = []
            for rowname in headers:
                try:
                    temp = cont[rowname]
                except KeyError:
                    temp = ''
                cont_in.append(temp)
            # logger.debug(cont_in)
            with open(filename, 'a') as f:
                f_csv = csv.writer(f)
                f_csv.writerow(cont_in)
        except KeyboardInterrupt:
            logger.warn("Probe Writer Terminated!!")
            break


# 定期执行相应的
def workflow_probe(i, queue):
    counter = 0
    while True:
        try:
            process = multiprocessing.Process(target=probe, args=(i, queue, ))
            process.start()
            counter = counter + 1
            logger.info("Probe of query " + str(i) + " has been executed for " + str(counter) + " times.")
            time.sleep(config.config.PROBE_INTERNAL_TIME)
        except KeyboardInterrupt:
            process.terminate()
            logger.warn("Probe of Query "+ str(i) +" Terminated!!")
            break


# 根据probe-list，调用相应的work-flow-probe，并管理各个workflow优雅退出。
def probe_monitor():
    record = []
    flag = True
    queue = multiprocessing.Queue()
    processW = multiprocessing.Process(target=save_result, args=(queue, ))
    processW.start()
    record.append(processW)
    for i in range(len(PROBE_SQL_LIST)):
        # 初始化对应的csv文件
        filename = config.config.PROBE_FILE_PREFIX + str(i) + config.config.PROBE_FILE_SUFFIX
        with open(filename, 'w') as f:
            f_csv = csv.writer(f)
            f_csv.writerow(headers)
        # 启动workflow
        process = multiprocessing.Process(target=workflow_probe, args=(i, queue, ))
        process.start()
        record.append(process)
        logger.info("Prober of Query "+ str(i) + " started!")
        try:
            time.sleep(config.config.PROBE_TIME_BETWEEN_SQL)
        except KeyboardInterrupt:
            queue.close()
            for p in record:
                p.terminate()
                p.join()
                time.sleep(0.01)
            logger.warn("Probe Monitor Terminated!!")
            flag = False
            break
    while flag:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            queue.close()
            for p in record:
                p.terminate()
                p.join()
                time.sleep(0.01)
            break
            
    logger.warn("Probe Monitor Terminated!!")
