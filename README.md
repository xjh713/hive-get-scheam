# hive-get-scheam
执行hive -e 命令并且获取对应的select查询出来的值及其对应的scheam字段

需要在执行语句中前部添加 set hive.cli.print.header=true; 这个设置,如下语句:
hive -e "set hive.cli.print.header=true;use default;select * from students"

这样最后的结果中会返回查询出来的字段值及其对应的scheam
