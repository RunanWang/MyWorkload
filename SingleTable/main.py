import loader.monitor as monitor
import init

init.init_db_table("mysql")
monitor.monitor("mysql")