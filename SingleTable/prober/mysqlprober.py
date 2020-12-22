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
    'plan', 'rowScan', 'rowOut', 
    
    'startTime', 'oTableTime', 'cTableTime', 'transHookTime', 'lockTime', 'initTime',
    'optimizeTime', 'statTime', 'prepareTime', 'execTime', 'endTime','qEndTime', 
    'commitTime', 'crTmpTime', 'rmTmpTime', 'freeTime', 'cleanTime', 

    'Aborted_clients', 'Aborted_connects','Bytes_received','Bytes_sent','Connections',
    'Com_insert','Com_rollback','Com_select','Com_set_option', 'Com_update','Com_delete',
    'Binlog_cache_use','Binlog_cache_disk_use','Created_tmp_files','Created_tmp_tables','Error_log_buffered_bytes','Flush_commands',

    'Handler_commit', 'Handler_delete', 'Handler_external_lock', 'Handler_mrr_init', 
    'Handler_prepare', 'Handler_read_first', 'Handler_read_key', 'Handler_read_last',
    'Handler_read_next', 'Handler_read_rnd_next', 'Handler_rollback', 'Handler_update',
    'Handler_write',     

    'Innodb_buffer_pool_pages_data', 'Innodb_buffer_pool_pages_dirty', 'Innodb_buffer_pool_pages_flushed', 'Innodb_buffer_pool_pages_free', 'Innodb_buffer_pool_pages_misc', 'Innodb_buffer_pool_pages_total', 'Innodb_buffer_pool_read_requests', 'Innodb_buffer_pool_reads', 'Innodb_buffer_pool_read_ahead_rnd', 'Innodb_buffer_pool_read_ahead', 'Innodb_buffer_pool_read_ahead_evicted', 

    'Innodb_data_fsyncs', 'Innodb_data_read', 'Innodb_data_reads', 'Innodb_data_writes', 'Innodb_data_written', 'Innodb_data_writes', 'Innodb_dblwr_pages_written', 'Innodb_dblwr_writes', 

    'Innodb_log_write_requests','Innodb_log_writes','Innodb_os_log_written','Innodb_pages_created','Innodb_pages_read','Innodb_page_size','Innodb_pages_written', 
    
    'Innodb_row_lock_time','Innodb_row_lock_time_avg','Innodb_row_lock_time_max','Innodb_row_lock_waits',

    'Innodb_rows_deleted', 'Innodb_rows_inserted', 'Innodb_rows_read', 'Innodb_rows_updated',   

    'Open_tables', 'Opened_tables', 'Queries', 'Questions',   

    'Select_full_join', 'Select_full_range_join', 'Select_range', 'Select_scan', 'Sort_rows', 'Sort_range', 'Sort_scan', 

    'Table_open_cache_hits', 'Table_open_cache_misses', 'Table_open_cache_overflows', 'Threads_cached', 'Threads_connected', 'Threads_created',

    'status_time', 'totalTime','timestamp'
    ]
mysql = mysql.MysqlDriver()
logger = utils.myLogger.getCMDLogger()

# 解析两次status形成输出
def parseStatus(resp1, resp2, time):
    dictTemp1 = {}
    dictTemp2 = {}
    dictStatus = {}
    for i in range(0,len(resp1)):
        dictTemp1[resp1[i]['Variable_name']] = resp1[i]['Value']
    for i in range(0,len(resp2)):
        dictTemp2[resp2[i]['Variable_name']] = resp2[i]['Value']
    
    dictStatus['Aborted_clients'] = int(dictTemp2['Aborted_clients']) - int(dictTemp1['Aborted_clients'])
    dictStatus['Aborted_connects'] = int(dictTemp2['Aborted_connects']) - int(dictTemp1['Aborted_connects'])
    dictStatus['Bytes_received'] = int(dictTemp2['Bytes_received']) - int(dictTemp1['Bytes_received'])
    dictStatus['Bytes_sent'] = int(dictTemp2['Bytes_sent']) - int(dictTemp1['Bytes_sent'])
    dictStatus['Connections'] = int(dictTemp2['Connections']) - int(dictTemp1['Connections'])
    
    dictStatus['Com_insert'] = int(dictTemp2['Com_insert']) - int(dictTemp1['Com_insert'])
    dictStatus['Com_rollback'] = int(dictTemp2['Com_rollback']) - int(dictTemp1['Com_rollback'])
    dictStatus['Com_select'] = int(dictTemp2['Com_select']) - int(dictTemp1['Com_select'])
    dictStatus['Com_set_option'] = int(dictTemp2['Com_set_option']) - int(dictTemp1['Com_set_option'])
    dictStatus['Com_update'] = int(dictTemp2['Com_update']) - int(dictTemp1['Com_update'])
    dictStatus['Com_delete'] = int(dictTemp2['Com_delete']) - int(dictTemp1['Com_delete'])
    
    dictStatus['Binlog_cache_use'] = int(dictTemp2['Binlog_cache_use']) - int(dictTemp1['Binlog_cache_use'])
    dictStatus['Binlog_cache_disk_use'] = int(dictTemp2['Binlog_cache_disk_use']) - int(dictTemp1['Binlog_cache_disk_use'])
    dictStatus['Created_tmp_files'] = int(dictTemp2['Created_tmp_files']) - int(dictTemp1['Created_tmp_files'])
    dictStatus['Created_tmp_tables'] = int(dictTemp2['Created_tmp_tables']) - int(dictTemp1['Created_tmp_tables'])
    dictStatus['Error_log_buffered_bytes'] = int(dictTemp2['Error_log_buffered_bytes']) - int(dictTemp1['Error_log_buffered_bytes'])
    dictStatus['Flush_commands'] = int(dictTemp2['Flush_commands']) - int(dictTemp1['Flush_commands'])
    
    dictStatus['Handler_commit'] = int(dictTemp2['Handler_commit']) - int(dictTemp1['Handler_commit'])
    dictStatus['Handler_delete'] = int(dictTemp2['Handler_delete']) - int(dictTemp1['Handler_delete'])
    dictStatus['Handler_external_lock'] = int(dictTemp2['Handler_external_lock']) - int(dictTemp1['Handler_external_lock'])
    dictStatus['Handler_mrr_init'] = int(dictTemp2['Handler_mrr_init']) - int(dictTemp1['Handler_mrr_init'])
    dictStatus['Handler_prepare'] = int(dictTemp2['Handler_prepare']) - int(dictTemp1['Handler_prepare'])
    dictStatus['Handler_read_first'] = int(dictTemp2['Handler_read_first']) - int(dictTemp1['Handler_read_first'])
    dictStatus['Handler_read_key'] = int(dictTemp2['Handler_read_key']) - int(dictTemp1['Handler_read_key'])
    dictStatus['Handler_read_last'] = int(dictTemp2['Handler_read_last']) - int(dictTemp1['Handler_read_last'])
    dictStatus['Handler_read_next'] = int(dictTemp2['Handler_read_next']) - int(dictTemp1['Handler_read_next'])
    dictStatus['Handler_read_rnd_next'] = int(dictTemp2['Handler_read_rnd_next']) - int(dictTemp1['Handler_read_rnd_next'])
    dictStatus['Handler_rollback'] = int(dictTemp2['Handler_rollback']) - int(dictTemp1['Handler_rollback'])
    dictStatus['Handler_update'] = int(dictTemp2['Handler_update']) - int(dictTemp1['Handler_update'])
    dictStatus['Handler_write'] = int(dictTemp2['Handler_write']) - int(dictTemp1['Handler_write'])
    
    dictStatus['Innodb_buffer_pool_pages_data'] = int(dictTemp2['Innodb_buffer_pool_pages_data']) - int(dictTemp1['Innodb_buffer_pool_pages_data'])
    dictStatus['Innodb_buffer_pool_pages_dirty'] = int(dictTemp2['Innodb_buffer_pool_pages_dirty']) - int(dictTemp1['Innodb_buffer_pool_pages_dirty'])
    dictStatus['Innodb_buffer_pool_pages_flushed'] = int(dictTemp2['Innodb_buffer_pool_pages_flushed']) - int(dictTemp1['Innodb_buffer_pool_pages_flushed'])
    dictStatus['Innodb_buffer_pool_pages_free'] = int(dictTemp2['Innodb_buffer_pool_pages_free']) - int(dictTemp1['Innodb_buffer_pool_pages_free'])
    dictStatus['Innodb_buffer_pool_pages_misc'] = int(dictTemp2['Innodb_buffer_pool_pages_misc']) - int(dictTemp1['Innodb_buffer_pool_pages_misc'])
    dictStatus['Innodb_buffer_pool_pages_total'] = int(dictTemp2['Innodb_buffer_pool_pages_total'])
    dictStatus['Innodb_buffer_pool_read_requests'] = int(dictTemp2['Innodb_buffer_pool_read_requests']) - int(dictTemp1['Innodb_buffer_pool_read_requests'])
    dictStatus['Innodb_buffer_pool_reads'] = int(dictTemp2['Innodb_buffer_pool_reads']) - int(dictTemp1['Innodb_buffer_pool_reads'])
    dictStatus['Innodb_buffer_pool_read_ahead_rnd'] = int(dictTemp2['Innodb_buffer_pool_read_ahead_rnd']) - int(dictTemp1['Innodb_buffer_pool_read_ahead_rnd'])
    dictStatus['Innodb_buffer_pool_read_ahead'] = int(dictTemp2['Innodb_buffer_pool_read_ahead']) - int(dictTemp1['Innodb_buffer_pool_read_ahead'])
    dictStatus['Innodb_buffer_pool_read_ahead_evicted'] = int(dictTemp2['Innodb_buffer_pool_read_ahead_evicted']) - int(dictTemp1['Innodb_buffer_pool_read_ahead_evicted'])
    
    dictStatus['Innodb_data_fsyncs'] = int(dictTemp2['Innodb_data_fsyncs']) - int(dictTemp1['Innodb_data_fsyncs'])
    dictStatus['Innodb_data_read'] = int(dictTemp2['Innodb_data_read']) - int(dictTemp1['Innodb_data_read'])
    dictStatus['Innodb_data_reads'] = int(dictTemp2['Innodb_data_reads']) - int(dictTemp1['Innodb_data_reads'])
    dictStatus['Innodb_data_writes'] = int(dictTemp2['Innodb_data_writes']) - int(dictTemp1['Innodb_data_writes'])
    dictStatus['Innodb_data_written'] = int(dictTemp2['Innodb_data_written']) - int(dictTemp1['Innodb_data_written'])
    dictStatus['Innodb_data_writes'] = int(dictTemp2['Innodb_data_writes']) - int(dictTemp1['Innodb_data_writes'])
    dictStatus['Innodb_dblwr_pages_written'] = int(dictTemp2['Innodb_dblwr_pages_written']) - int(dictTemp1['Innodb_dblwr_pages_written'])
    dictStatus['Innodb_dblwr_writes'] = int(dictTemp2['Innodb_dblwr_writes']) - int(dictTemp1['Innodb_dblwr_writes'])
    
    dictStatus['Innodb_log_write_requests'] = int(dictTemp2['Innodb_log_write_requests']) - int(dictTemp1['Innodb_log_write_requests'])
    dictStatus['Innodb_log_writes'] = int(dictTemp2['Innodb_log_writes']) - int(dictTemp1['Innodb_log_writes'])
    dictStatus['Innodb_os_log_written'] = int(dictTemp2['Innodb_os_log_written']) - int(dictTemp1['Innodb_os_log_written'])
    dictStatus['Innodb_pages_created'] = int(dictTemp2['Innodb_pages_created']) - int(dictTemp1['Innodb_pages_created'])
    dictStatus['Innodb_pages_read'] = int(dictTemp2['Innodb_pages_read']) - int(dictTemp1['Innodb_pages_read'])
    dictStatus['Innodb_page_size'] = int(dictTemp2['Innodb_page_size'])
    dictStatus['Innodb_pages_written'] = int(dictTemp2['Innodb_pages_written']) - int(dictTemp1['Innodb_pages_written'])

    dictStatus['Innodb_row_lock_time'] = int(dictTemp2['Innodb_row_lock_time']) - int(dictTemp1['Innodb_row_lock_time'])
    dictStatus['Innodb_row_lock_time_avg'] = int(dictTemp2['Innodb_row_lock_time_avg'])
    dictStatus['Innodb_row_lock_time_max'] = int(dictTemp2['Innodb_row_lock_time_max'])
    dictStatus['Innodb_row_lock_waits'] = int(dictTemp2['Innodb_row_lock_waits']) - int(dictTemp1['Innodb_row_lock_waits'])
    
    dictStatus['Innodb_rows_deleted'] = int(dictTemp2['Innodb_rows_deleted']) - int(dictTemp1['Innodb_rows_deleted'])
    dictStatus['Innodb_rows_inserted'] = int(dictTemp2['Innodb_rows_inserted']) - int(dictTemp1['Innodb_rows_inserted'])
    dictStatus['Innodb_rows_read'] = int(dictTemp2['Innodb_rows_read']) - int(dictTemp1['Innodb_rows_read'])
    dictStatus['Innodb_rows_updated'] = int(dictTemp2['Innodb_rows_updated']) - int(dictTemp1['Innodb_rows_updated'])
    
    dictStatus['Open_tables'] = int(dictTemp2['Open_tables']) - int(dictTemp1['Open_tables'])
    dictStatus['Opened_tables'] = int(dictTemp2['Opened_tables']) - int(dictTemp1['Opened_tables'])
    
    dictStatus['Queries'] = int(dictTemp2['Queries']) - int(dictTemp1['Queries'])
    dictStatus['Questions'] = int(dictTemp2['Questions']) - int(dictTemp1['Questions'])

    dictStatus['Select_full_join'] = int(dictTemp2['Select_full_join']) - int(dictTemp1['Select_full_join'])
    dictStatus['Select_full_range_join'] = int(dictTemp2['Select_full_range_join']) - int(dictTemp1['Select_full_range_join'])
    dictStatus['Select_range'] = int(dictTemp2['Select_range']) - int(dictTemp1['Select_range'])
    dictStatus['Select_scan'] = int(dictTemp2['Select_scan']) - int(dictTemp1['Select_scan'])
    dictStatus['Sort_rows'] = int(dictTemp2['Sort_rows']) - int(dictTemp1['Sort_rows'])
    dictStatus['Sort_range'] = int(dictTemp2['Sort_range']) - int(dictTemp1['Sort_range'])
    dictStatus['Sort_scan'] = int(dictTemp2['Sort_scan']) - int(dictTemp1['Sort_scan'])
    
    dictStatus['Table_open_cache_hits'] = int(dictTemp2['Table_open_cache_hits']) - int(dictTemp1['Table_open_cache_hits'])
    dictStatus['Table_open_cache_misses'] = int(dictTemp2['Table_open_cache_misses']) - int(dictTemp1['Table_open_cache_misses'])
    dictStatus['Table_open_cache_overflows'] = int(dictTemp2['Table_open_cache_overflows']) - int(dictTemp1['Table_open_cache_overflows'])
    
    dictStatus['Threads_cached'] = int(dictTemp2['Threads_cached']) - int(dictTemp1['Threads_cached'])
    dictStatus['Threads_connected'] = int(dictTemp2['Threads_connected']) - int(dictTemp1['Threads_connected'])
    dictStatus['Threads_created'] = int(dictTemp2['Threads_created']) - int(dictTemp1['Threads_created'])

    dictStatus['status_time'] = time
    return dictStatus


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

        cursor.execute("show global status")
        statusResp1 = cursor.fetchall()
        statusTimeBegin = time.time()

        # 记录profile结果
        cursor.execute("SET profiling=1;")
        cursor.execute(sql)
        cursor.execute("show profile;")
        profileResp = cursor.fetchall()

        statusTimeEnd = time.time()
        cursor.execute("show global status")
        statusResp2 = cursor.fetchall()
        

        statusTime = statusTimeEnd - statusTimeBegin

        # 打包所有结果
        result_packer(i, statusResp1, statusResp2, explainResp, profileResp, statusTime, queue)
    except Exception:
        logger.debug(Exception.with_traceback())
        conn.rollback()
    finally:
        conn.close()
        cursor.close()


# 打包器
def result_packer(i, statusResp1, statusResp2, explainResp, profileResp, statusTime, queue):
    result = {}
    content = {}

    dictStatus = parseStatus(statusResp1, statusResp2, statusTime)
    content = dictStatus

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
