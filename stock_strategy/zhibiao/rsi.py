from QUANTAXIS.QAIndicator.base import *
def RSI(data):
    N1=7
    LC=REF(data.close,1)
    RSI=SMA(MAX(data.close-LC,0),N1,1)/SMA(ABS(data.close-LC),N1,1)*100
    RSI1 = REF(RSI,1)
    sell=CROSS(78,RSI1)
    buy=CROSS(RSI1,20)
  
    
        
    return pd.DataFrame({
            'rsi': RSI1,
            'buy' :buy,
            'sell' :sell,
            
        })
