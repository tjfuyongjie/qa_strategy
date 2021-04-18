qastocksim --user stock-user --password stock-user --eventmq_ip www.yutiansut.com --eventmq_port  5678

python sub_QAREALTIME_FIX.py

#qastocksim --user <youusername> --password <yourpassword> --eventmq_ip www.yutiansut.com --eventmq_port  5678

{datetime: {$gte:"2021-01-14 11:20:00",$lte:"2021-01-18 11:20:00"},code:"SZ002259",'frequence':'1min'}

安装：apt-get install cron
启动：service cron start
重启：service cron restart
停止：service cron stop
检查状态：service cron status
查询cron可用的命令：service cron
检查Cronta工具是否安装：crontab -l

SHELL=/bin/bash
PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
tz=asia/shanghai
30 17 * * 1-5 python /root/QUANTAXIS/config/update_all.py

更新 stock_block_list 报错的，环境或者docker中 运行 pip install xlrd -i https://pypi.doubanio.com/simple

30 09 * * * /opt/conda/bin/python /root/sim/stock_strategy/strategy.py > /root/sim/stock_strategy/strategy.log
29 09 * * * /opt/conda/bin/python /root/sim/sub_QAREALTIME_FIX.py


pip install QUANTAXIS -U
pip install QIFIAccount -Ux

pip install quantaxis_pubsub –U

-------
http://127.0.0.1:8029/testk