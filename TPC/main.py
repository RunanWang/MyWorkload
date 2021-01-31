from loader.loader import Loader
from loader.transaction import Transaction

# loader = Loader("mysql")
# loader.monitor_warehouse(1)
transaction = Transaction("mysql")
# transaction.delivery(1)
transaction.new_order(1, 1)
