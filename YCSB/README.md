# Explain

## config

DBMS部分相关，该模块负责配置信息。分为通用的系统信息和DB特有的信息。

## drivers

DBMS相关，该模块负责与DB之间的联系。目前已实现

- MySQL

需要按照抽象类实现各个接口，保证命名格式。

## loader

DBMS无关，DB通用的workload加载器。

分为

- consumer：按照DB对应的driver信息，从queue中取出workload向DB中执行。

- producer：按照DB对应的workload信息生成workload插入queue中。

- monitor： queue的管理和相关QPS等计数，以及优雅地终止程序。

## prober

DBMS部分相关，按DB区分的探针程序。目前已实现：

- MySQL

## utils

DBMS无关，一些工具，如logger和random等

## workload

DBMS相关，描述workload的模块，需要按照抽象类实现各个接口，保证命名格式。

按照每单位时间执行每条query多少次进行规定。