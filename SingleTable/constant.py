CITY = ['BJ', 'SH', 'GZ', 'SZ', 'HB', 'CC', 'SY', 'TJ']

# String
MAX_LONG_STRING_LEN = 30
MAX_SHORT_STRING_LEN = 10

MIN_LONG_STRING_LEN = 10
MIN_SHORT_STRING_LEN = 5

# Number
MIN_SMALL_INT = 1
MAX_SMALL_INT = 9

MIN_BIG_INT = 100
MAX_BIG_INT = 999

# Workload
RATE_INSERT = 1
RATE_UPDATE = 1
RATE_SIMPLE_SEARCH = 0.25
RATE_COMPLEX_SEARCH = 0.5

INTERNAL_SLEEP_TIME = 0.1

# probe
PROBE_SQL_LIST=[
    "select * from (select * from (select * from TEST where FIELD_08>500 order by FIELD_09)as K order by FIELD_08) as a join (select * from TEST where FIELD_09<200) as b where a.FIELD_08=b.FIELD_08;",
    "select FIELD_07, sum(1) from TEST group by FIELD_07;"
]
PROBE_FILE_PREFIX = "./result/sql_"
PROBE_FILE_SUFFIX = ".csv"
PROBE_INTERNAL_TIME = 60
PROBE_TIME_BETWEEN_SQL = 2
