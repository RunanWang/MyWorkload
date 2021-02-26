from TPC.utils.myLogger import get_cmd_logger
import TPC.config.config as config
import multiprocessing
import csv
import time


class Probe(object):
    def __init__(self, driver):
        self.PROBE_SQL_LIST = [
            "select count(*) from ORDERS;"
        ]
        self.logger = get_cmd_logger()
        self.driver = driver
        self.header = [
            'plan', 'rowScan', 'rowOut',

            'startTime', 'oTableTime', 'cTableTime', 'transHookTime', 'lockTime', 'initTime',
            'optimizeTime', 'statTime', 'prepareTime', 'execTime', 'endTime', 'qEndTime',
            'commitTime', 'crTmpTime', 'rmTmpTime', 'freeTime', 'cleanTime',

            'Aborted_clients', 'Aborted_connects', 'Bytes_received', 'Bytes_sent', 'Connections',
            'Com_insert', 'Com_rollback', 'Com_select', 'Com_set_option', 'Com_update', 'Com_delete',
            'Binlog_cache_use', 'Binlog_cache_disk_use', 'Created_tmp_files', 'Created_tmp_tables',
            'Error_log_buffered_bytes',
            'Flush_commands',

            'Handler_commit', 'Handler_delete', 'Handler_external_lock', 'Handler_mrr_init',
            'Handler_prepare', 'Handler_read_first', 'Handler_read_key', 'Handler_read_last',
            'Handler_read_next', 'Handler_read_rnd_next', 'Handler_rollback', 'Handler_update',
            'Handler_write',

            'Innodb_buffer_pool_pages_data', 'Innodb_buffer_pool_pages_dirty', 'Innodb_buffer_pool_pages_flushed',
            'Innodb_buffer_pool_pages_free', 'Innodb_buffer_pool_pages_misc', 'Innodb_buffer_pool_pages_total',
            'Innodb_buffer_pool_read_requests', 'Innodb_buffer_pool_reads', 'Innodb_buffer_pool_read_ahead_rnd',
            'Innodb_buffer_pool_read_ahead', 'Innodb_buffer_pool_read_ahead_evicted',

            'Innodb_data_fsyncs', 'Innodb_data_read', 'Innodb_data_reads', 'Innodb_data_writes', 'Innodb_data_written',
            'Innodb_data_writes', 'Innodb_dblwr_pages_written', 'Innodb_dblwr_writes',

            'Innodb_log_write_requests', 'Innodb_log_writes', 'Innodb_os_log_written', 'Innodb_pages_created',
            'Innodb_pages_read', 'Innodb_page_size', 'Innodb_pages_written',

            'Innodb_row_lock_time', 'Innodb_row_lock_time_avg', 'Innodb_row_lock_time_max', 'Innodb_row_lock_waits',

            'Innodb_rows_deleted', 'Innodb_rows_inserted', 'Innodb_rows_read', 'Innodb_rows_updated',

            'Open_tables', 'Opened_tables', 'Queries', 'Questions',

            'Select_full_join', 'Select_full_range_join', 'Select_range', 'Select_scan', 'Sort_rows', 'Sort_range',
            'Sort_scan',

            'Table_open_cache_hits', 'Table_open_cache_misses', 'Table_open_cache_overflows', 'Threads_cached',
            'Threads_connected', 'Threads_created',

            'status_time', 'totalTime', 'timestamp'
        ]

    # 解析两次status形成输出
    @staticmethod
    def parse_status(resp1, resp2, status_time):
        dict_temp1 = {}
        dict_temp2 = {}
        dict_status = {}

        for i in range(0, len(resp1)):
            dict_temp1[resp1[i]['Variable_name']] = resp1[i]['Value']
        for i in range(0, len(resp2)):
            dict_temp2[resp2[i]['Variable_name']] = resp2[i]['Value']

        dict_status['Aborted_clients'] = int(dict_temp2['Aborted_clients']) - int(dict_temp1['Aborted_clients'])
        dict_status['Aborted_connects'] = int(dict_temp2['Aborted_connects']) - int(dict_temp1['Aborted_connects'])
        dict_status['Bytes_received'] = int(dict_temp2['Bytes_received']) - int(dict_temp1['Bytes_received'])
        dict_status['Bytes_sent'] = int(dict_temp2['Bytes_sent']) - int(dict_temp1['Bytes_sent'])
        dict_status['Connections'] = int(dict_temp2['Connections']) - int(dict_temp1['Connections'])

        dict_status['Com_insert'] = int(dict_temp2['Com_insert']) - int(dict_temp1['Com_insert'])
        dict_status['Com_rollback'] = int(dict_temp2['Com_rollback']) - int(dict_temp1['Com_rollback'])
        dict_status['Com_select'] = int(dict_temp2['Com_select']) - int(dict_temp1['Com_select'])
        dict_status['Com_set_option'] = int(dict_temp2['Com_set_option']) - int(dict_temp1['Com_set_option'])
        dict_status['Com_update'] = int(dict_temp2['Com_update']) - int(dict_temp1['Com_update'])
        dict_status['Com_delete'] = int(dict_temp2['Com_delete']) - int(dict_temp1['Com_delete'])

        dict_status['Binlog_cache_use'] = int(dict_temp2['Binlog_cache_use']) - int(dict_temp1['Binlog_cache_use'])
        dict_status['Binlog_cache_disk_use'] = int(dict_temp2['Binlog_cache_disk_use']) - int(
            dict_temp1['Binlog_cache_disk_use'])
        dict_status['Created_tmp_files'] = int(dict_temp2['Created_tmp_files']) - int(dict_temp1['Created_tmp_files'])
        dict_status['Created_tmp_tables'] = int(dict_temp2['Created_tmp_tables']) - int(
            dict_temp1['Created_tmp_tables'])
        dict_status['Error_log_buffered_bytes'] = int(dict_temp2['Error_log_buffered_bytes']) - int(
            dict_temp1['Error_log_buffered_bytes'])
        dict_status['Flush_commands'] = int(dict_temp2['Flush_commands']) - int(dict_temp1['Flush_commands'])

        dict_status['Handler_commit'] = int(dict_temp2['Handler_commit']) - int(dict_temp1['Handler_commit'])
        dict_status['Handler_delete'] = int(dict_temp2['Handler_delete']) - int(dict_temp1['Handler_delete'])
        dict_status['Handler_external_lock'] = int(dict_temp2['Handler_external_lock']) - int(
            dict_temp1['Handler_external_lock'])
        dict_status['Handler_mrr_init'] = int(dict_temp2['Handler_mrr_init']) - int(dict_temp1['Handler_mrr_init'])
        dict_status['Handler_prepare'] = int(dict_temp2['Handler_prepare']) - int(dict_temp1['Handler_prepare'])
        dict_status['Handler_read_first'] = int(dict_temp2['Handler_read_first']) - int(
            dict_temp1['Handler_read_first'])
        dict_status['Handler_read_key'] = int(dict_temp2['Handler_read_key']) - int(dict_temp1['Handler_read_key'])
        dict_status['Handler_read_last'] = int(dict_temp2['Handler_read_last']) - int(dict_temp1['Handler_read_last'])
        dict_status['Handler_read_next'] = int(dict_temp2['Handler_read_next']) - int(dict_temp1['Handler_read_next'])
        dict_status['Handler_read_rnd_next'] = int(dict_temp2['Handler_read_rnd_next']) - int(
            dict_temp1['Handler_read_rnd_next'])
        dict_status['Handler_rollback'] = int(dict_temp2['Handler_rollback']) - int(dict_temp1['Handler_rollback'])
        dict_status['Handler_update'] = int(dict_temp2['Handler_update']) - int(dict_temp1['Handler_update'])
        dict_status['Handler_write'] = int(dict_temp2['Handler_write']) - int(dict_temp1['Handler_write'])

        dict_status['Innodb_buffer_pool_pages_data'] = int(dict_temp2['Innodb_buffer_pool_pages_data']) - int(
            dict_temp1['Innodb_buffer_pool_pages_data'])
        dict_status['Innodb_buffer_pool_pages_dirty'] = int(dict_temp2['Innodb_buffer_pool_pages_dirty']) - int(
            dict_temp1['Innodb_buffer_pool_pages_dirty'])
        dict_status['Innodb_buffer_pool_pages_flushed'] = int(dict_temp2['Innodb_buffer_pool_pages_flushed']) - int(
            dict_temp1['Innodb_buffer_pool_pages_flushed'])
        dict_status['Innodb_buffer_pool_pages_free'] = int(dict_temp2['Innodb_buffer_pool_pages_free']) - int(
            dict_temp1['Innodb_buffer_pool_pages_free'])
        dict_status['Innodb_buffer_pool_pages_misc'] = int(dict_temp2['Innodb_buffer_pool_pages_misc']) - int(
            dict_temp1['Innodb_buffer_pool_pages_misc'])
        dict_status['Innodb_buffer_pool_pages_total'] = int(dict_temp2['Innodb_buffer_pool_pages_total'])
        dict_status['Innodb_buffer_pool_read_requests'] = int(dict_temp2['Innodb_buffer_pool_read_requests']) - int(
            dict_temp1['Innodb_buffer_pool_read_requests'])
        dict_status['Innodb_buffer_pool_reads'] = int(dict_temp2['Innodb_buffer_pool_reads']) - int(
            dict_temp1['Innodb_buffer_pool_reads'])
        dict_status['Innodb_buffer_pool_read_ahead_rnd'] = int(dict_temp2['Innodb_buffer_pool_read_ahead_rnd']) - int(
            dict_temp1['Innodb_buffer_pool_read_ahead_rnd'])
        dict_status['Innodb_buffer_pool_read_ahead'] = int(dict_temp2['Innodb_buffer_pool_read_ahead']) - int(
            dict_temp1['Innodb_buffer_pool_read_ahead'])
        dict_status['Innodb_buffer_pool_read_ahead_evicted'] = int(
            dict_temp2['Innodb_buffer_pool_read_ahead_evicted']) - int(
            dict_temp1['Innodb_buffer_pool_read_ahead_evicted'])

        dict_status['Innodb_data_fsyncs'] = int(dict_temp2['Innodb_data_fsyncs']) - int(
            dict_temp1['Innodb_data_fsyncs'])
        dict_status['Innodb_data_read'] = int(dict_temp2['Innodb_data_read']) - int(dict_temp1['Innodb_data_read'])
        dict_status['Innodb_data_reads'] = int(dict_temp2['Innodb_data_reads']) - int(dict_temp1['Innodb_data_reads'])
        dict_status['Innodb_data_writes'] = int(dict_temp2['Innodb_data_writes']) - int(
            dict_temp1['Innodb_data_writes'])
        dict_status['Innodb_data_written'] = int(dict_temp2['Innodb_data_written']) - int(
            dict_temp1['Innodb_data_written'])
        dict_status['Innodb_data_writes'] = int(dict_temp2['Innodb_data_writes']) - int(
            dict_temp1['Innodb_data_writes'])
        dict_status['Innodb_dblwr_pages_written'] = int(dict_temp2['Innodb_dblwr_pages_written']) - int(
            dict_temp1['Innodb_dblwr_pages_written'])
        dict_status['Innodb_dblwr_writes'] = int(dict_temp2['Innodb_dblwr_writes']) - int(
            dict_temp1['Innodb_dblwr_writes'])

        dict_status['Innodb_log_write_requests'] = int(dict_temp2['Innodb_log_write_requests']) - int(
            dict_temp1['Innodb_log_write_requests'])
        dict_status['Innodb_log_writes'] = int(dict_temp2['Innodb_log_writes']) - int(dict_temp1['Innodb_log_writes'])
        dict_status['Innodb_os_log_written'] = int(dict_temp2['Innodb_os_log_written']) - int(
            dict_temp1['Innodb_os_log_written'])
        dict_status['Innodb_pages_created'] = int(dict_temp2['Innodb_pages_created']) - int(
            dict_temp1['Innodb_pages_created'])
        dict_status['Innodb_pages_read'] = int(dict_temp2['Innodb_pages_read']) - int(dict_temp1['Innodb_pages_read'])
        dict_status['Innodb_page_size'] = int(dict_temp2['Innodb_page_size'])
        dict_status['Innodb_pages_written'] = int(dict_temp2['Innodb_pages_written']) - int(
            dict_temp1['Innodb_pages_written'])

        dict_status['Innodb_row_lock_time'] = int(dict_temp2['Innodb_row_lock_time']) - int(
            dict_temp1['Innodb_row_lock_time'])
        dict_status['Innodb_row_lock_time_avg'] = int(dict_temp2['Innodb_row_lock_time_avg'])
        dict_status['Innodb_row_lock_time_max'] = int(dict_temp2['Innodb_row_lock_time_max'])
        dict_status['Innodb_row_lock_waits'] = int(dict_temp2['Innodb_row_lock_waits']) - int(
            dict_temp1['Innodb_row_lock_waits'])

        dict_status['Innodb_rows_deleted'] = int(dict_temp2['Innodb_rows_deleted']) - int(
            dict_temp1['Innodb_rows_deleted'])
        dict_status['Innodb_rows_inserted'] = int(dict_temp2['Innodb_rows_inserted']) - int(
            dict_temp1['Innodb_rows_inserted'])
        dict_status['Innodb_rows_read'] = int(dict_temp2['Innodb_rows_read']) - int(dict_temp1['Innodb_rows_read'])
        dict_status['Innodb_rows_updated'] = int(dict_temp2['Innodb_rows_updated']) - int(
            dict_temp1['Innodb_rows_updated'])

        dict_status['Open_tables'] = int(dict_temp2['Open_tables']) - int(dict_temp1['Open_tables'])
        dict_status['Opened_tables'] = int(dict_temp2['Opened_tables']) - int(dict_temp1['Opened_tables'])

        dict_status['Queries'] = int(dict_temp2['Queries']) - int(dict_temp1['Queries'])
        dict_status['Questions'] = int(dict_temp2['Questions']) - int(dict_temp1['Questions'])

        dict_status['Select_full_join'] = int(dict_temp2['Select_full_join']) - int(dict_temp1['Select_full_join'])
        dict_status['Select_full_range_join'] = int(dict_temp2['Select_full_range_join']) - int(
            dict_temp1['Select_full_range_join'])
        dict_status['Select_range'] = int(dict_temp2['Select_range']) - int(dict_temp1['Select_range'])
        dict_status['Select_scan'] = int(dict_temp2['Select_scan']) - int(dict_temp1['Select_scan'])
        dict_status['Sort_rows'] = int(dict_temp2['Sort_rows']) - int(dict_temp1['Sort_rows'])
        dict_status['Sort_range'] = int(dict_temp2['Sort_range']) - int(dict_temp1['Sort_range'])
        dict_status['Sort_scan'] = int(dict_temp2['Sort_scan']) - int(dict_temp1['Sort_scan'])

        dict_status['Table_open_cache_hits'] = int(dict_temp2['Table_open_cache_hits']) - int(
            dict_temp1['Table_open_cache_hits'])
        dict_status['Table_open_cache_misses'] = int(dict_temp2['Table_open_cache_misses']) - int(
            dict_temp1['Table_open_cache_misses'])
        dict_status['Table_open_cache_overflows'] = int(dict_temp2['Table_open_cache_overflows']) - int(
            dict_temp1['Table_open_cache_overflows'])

        dict_status['Threads_cached'] = int(dict_temp2['Threads_cached']) - int(dict_temp1['Threads_cached'])
        dict_status['Threads_connected'] = int(dict_temp2['Threads_connected']) - int(dict_temp1['Threads_connected'])
        dict_status['Threads_created'] = int(dict_temp2['Threads_created']) - int(dict_temp1['Threads_created'])

        dict_status['status_time'] = status_time
        return dict_status

    # 把explain语句中的信息解析出来
    # 输出扫描行数和结果行数
    @staticmethod
    def parse_explain(resp):
        dict_explain = {}
        rows_scan = 0
        rows_out = 0
        plan = ''
        # logger.debug(resp)
        for i in range(0, len(resp)):
            dict_explain = resp[i]
            for k, v in dict_explain.items():
                if v is None:
                    dict_explain[k] = 'None'
            rows_scan = rows_scan + dict_explain.get('rows')
            rows_out = rows_out + dict_explain.get('rows') * dict_explain.get('filtered') / 100
            if plan != '':
                plan = plan + "+" + str(dict_explain.get('id')) + "-" + dict_explain.get(
                    'select_type') + "-" + dict_explain.get('table') + "-" + dict_explain.get(
                    'type') + "-" + dict_explain.get(
                    'key') + "-" + dict_explain.get('Extra').replace(' ', '')
            else:
                plan = str(dict_explain.get('id')) + "-" + dict_explain.get('select_type') + "-" + dict_explain.get(
                    'table') + "-" + dict_explain.get('type') + "-" + dict_explain.get('key') + "-" + dict_explain.get(
                    'Extra').replace(' ', '')
        dict_explain['rowsScan'] = rows_scan
        dict_explain['rowsOut'] = rows_out
        dict_explain['plan'] = plan
        return dict_explain

    # 把Profile中的信息解析成字典输出
    @staticmethod
    def parse_profile(resp):
        dict_profile = {}
        total = 0
        for i in range(0, len(resp)):
            try:
                dict_profile[resp[i]['Status']] = resp[i]['Duration'] + dict_profile[resp[i]['Status']]
            except:
                dict_profile[resp[i]['Status']] = resp[i]['Duration']
            total = total + resp[i]['Duration']
        dict_profile['totalTime'] = total
        return dict_profile

    # 分析当前数据库环境，执行sql并生成相应的sql分析
    # 生成结果压入队列，k为对应名称，v为对应值
    def probe(self, i, queue):
        sql = self.PROBE_SQL_LIST[i]
        explain_sql = "explain " + sql
        status_sql = "show global status"
        conn, cursor = self.driver.get_cursor()

        # 记录explain结果
        cursor.execute(explain_sql)
        explain_resp = cursor.fetchall()

        # 记录profile结果
        cursor.execute(status_sql)
        status_resp1 = cursor.fetchall()
        status_time_begin = time.time()
        cursor.execute("SET profiling=1;")
        cursor.execute(sql)
        cursor.execute("show profile;")
        profile_resp = cursor.fetchall()
        status_time_end = time.time()
        cursor.execute(status_sql)
        status_resp2 = cursor.fetchall()
        status_time = status_time_end - status_time_begin

        # 打包所有结果
        self.result_packer(i, status_resp1, status_resp2, explain_resp, profile_resp, status_time, queue)

    # 打包器
    def result_packer(self, i, status_resp1, status_resp2, explain_resp, profile_resp, status_time, queue):
        result = {}
        content = {}

        dict_status = self.parse_status(status_resp1, status_resp2, status_time)
        content = dict_status

        dict_explain = self.parse_explain(explain_resp)
        content['timestamp'] = time.time()
        content['plan'] = dict_explain['plan']
        content['rowScan'] = dict_explain['rowsScan']
        content['rowOut'] = dict_explain['rowsOut']

        dict_profile = self.parse_profile(profile_resp)
        content['totalTime'] = dict_profile['totalTime']
        content['lockTime'] = dict_profile['System lock']
        content['startTime'] = dict_profile['starting']
        content['transHookTime'] = dict_profile['Executing hook on transaction ']
        content['oTableTime'] = dict_profile['Opening tables']
        content['initTime'] = dict_profile['init']

        content['optimizeTime'] = dict_profile['optimizing']
        content['statTime'] = dict_profile['statistics']
        content['prepareTime'] = dict_profile['preparing']
        content['execTime'] = dict_profile['executing']
        content['endTime'] = dict_profile['end']
        content['qEndTime'] = dict_profile['query end']
        content['commitTime'] = dict_profile['waiting for handler commit']
        # content['rmTmpTime'] = 0 #dict_profile['removing tmp table']
        # content['crTmpTime'] = 0 #dict_profile['Creating tmp table']
        self.check_dict(dict_profile, content, 'removing tmp table', 'rmTmpTime')
        self.check_dict(dict_profile, content, 'Creating tmp table', 'crTmpTime')
        content['cTableTime'] = dict_profile['closing tables']
        content['freeTime'] = dict_profile['freeing items']
        content['cleanTime'] = dict_profile['cleaning up']

        result['id'] = i
        result['content'] = content
        queue.put(result)

    @staticmethod
    def check_dict(from_dict, to_dict, from_name, to_name):
        try:
            to_dict[to_name] = from_dict[from_name]
        except KeyError:
            to_dict[to_name] = 0

    # 把result字典保存至num对应的文件/数据库中
    def save_result(self, queue, alive):
        self.logger.info("Probe Writer Started!!")
        while alive.value is 1:
            result = queue.get()
            if result is None:
                break
            cont_id = result['id']
            cont = result['content']
            filename = config.PROBE_FILE_PREFIX + str(cont_id) + config.PROBE_FILE_SUFFIX
            cont_in = []
            for row_name in self.header:
                try:
                    temp = cont[row_name]
                except KeyError:
                    temp = ''
                cont_in.append(temp)
            # logger.debug(cont_in)
            with open(filename, 'a') as f:
                f_csv = csv.writer(f)
                f_csv.writerow(cont_in)
        self.logger.warn("Probe Writer Terminated!!")

    # 定期执行相应的
    def workflow_probe(self, i, queue, alive):
        counter = 0
        while alive.value is 1:
            process = multiprocessing.Process(target=self.probe, args=(i, queue,))
            process.start()
            counter = counter + 1
            self.logger.info("Probe of query " + str(i) + " has been executed for " + str(counter) + " times.")
            time.sleep(config.PROBE_INTERNAL_TIME)

        self.logger.warn("Probe of Query " + str(i) + " Terminated!!")

    def file_init(self, i):
        filename = config.PROBE_FILE_PREFIX + str(i) + config.PROBE_FILE_SUFFIX
        with open(filename, 'w') as f:
            f_csv = csv.writer(f)
            f_csv.writerow(self.header)

    # 根据probe-list，调用相应的work-flow-probe，并管理各个workflow优雅退出。
    def probe_monitor(self, alive):
        record = []
        queue = multiprocessing.Queue()
        process_w = multiprocessing.Process(target=self.save_result, args=(queue, alive,))
        process_w.start()
        record.append(process_w)
        for i in range(len(self.PROBE_SQL_LIST)):
            # 初始化对应的csv文件
            filename = config.PROBE_FILE_PREFIX + str(i) + config.PROBE_FILE_SUFFIX
            with open(filename, 'w') as f:
                f_csv = csv.writer(f)
                f_csv.writerow(self.header)
            # 启动workflow
            process = multiprocessing.Process(target=self.workflow_probe, args=(i, queue, alive,))
            process.start()
            record.append(process)
            self.logger.info("Probe of Query " + str(i) + " started!")
            time.sleep(config.PROBE_TIME_BETWEEN_SQL)

        while alive.value is 1:
            time.sleep(1)

        queue.close()
        for p in record:
            p.join()

        self.logger.warn("Probe Monitor Terminated!!")
