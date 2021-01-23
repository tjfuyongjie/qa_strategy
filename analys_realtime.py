import pandas as pd


pwd =  "/data/2020/02/13"
code = 'SZ000652'
file = "{}/{}.csv".format(pwd, code)
res =  pd.read_csv(file, index_col=[0])

table= pd.concat([res[res.isbuy].loc[:,['buy_price',"buy_no", 'buy_vol', 'price', 'sell_vol', 'sell_no', 'isbuy']].rename(columns={
    "buy_price": "ask_price", "buy_vol": "ask_vol", "price": "DealPrice", "sell_vol": "RawVol", "sell_no": "RawNo", "buy_no": "AskNo"
}), res[~res.isbuy].loc[:,['sell_price', 'sell_vol',"sell_no", 'price', 'buy_vol','buy_no', 'isbuy']].rename(columns={
    "sell_price": "ask_price", "sell_vol": "ask_vol", "price": "DealPrice", "buy_vol": "RawVol", "buy_no": "RawNo","sell_no": "AskNo",
})], sort=False).sort_index()

table = table.assign(bias = abs(table.DealPrice - table.ask_price))
