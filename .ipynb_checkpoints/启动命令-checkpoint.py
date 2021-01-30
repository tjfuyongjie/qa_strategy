qastocksim --user stock-user --password stock-user --eventmq_ip www.yutiansut.com --eventmq_port  5678

python sub_QAREALTIME_FIX.py

#qastocksim --user <youusername> --password <yourpassword> --eventmq_ip www.yutiansut.com --eventmq_port  5678

{datetime: {$gte:"2021-01-14 11:20:00",$lte:"2021-01-18 11:20:00"},code:"SZ002259",'frequence':'1min'}


30 09 * * * /opt/conda/bin/python /root/sim/stock_strategy/strategy.py
29 09 * * * /opt/conda/bin/python /root/sim/sub_QAREALTIME_FIX.py