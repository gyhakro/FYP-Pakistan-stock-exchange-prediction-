import http.client
import pandas as pd
import time
symbols=['ENGRO','LUCK', 'OGDC','FFC','HBL','HUBC','PPL', 'POL','EFERT', 'MCB','UBL','DGKC','PSO','SEARL','MLCF',
'BAHL','MARI','TRG','ATRL','UNITY','SYS','MEBL','GHNI','NML','PIOC','CHCC','PAEL','ISL','KAPCO','DAWH']
months=[1,2,3,4,5,6,7,8,9,10,11,12]
years=[2018,2019,2020,2021,2022]
table_data=""
all_in_one_df=pd.DataFrame()

conn = http.client.HTTPSConnection("dps.psx.com.pk")
headers = {
    'Accept': "text/html, */*; q=0.01",
    'Accept-Language': "en-US,en;q=0.9",
    'Connection': "keep-alive",
    'Content-Type': "application/x-www-form-urlencoded; charset=UTF-8",
    'Origin': "https://dps.psx.com.pk",
    'Referer': "https://dps.psx.com.pk/historical",
    'Sec-Fetch-Dest': "empty",
    'Sec-Fetch-Mode': "cors",
    'Sec-Fetch-Site': "same-origin",
    'Sec-GPC': "1",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
    'X-Requested-With': "XMLHttpRequest"
    }
for symbol in symbols:
    time.sleep(1)
    table_data=""
    for year in years:
        time.sleep(1)
        for month in months:
            time.sleep(2)
            payload = f"month={month}&year={year}&symbol={symbol}"    
            conn.request("POST", "/historical", payload, headers)
            res = conn.getresponse()
            data = res.read()
            data=data.decode("utf-8")
            if len(data)>600:
                table_data=table_data+data[523:-22]
                print(f"{symbol} {month} {year} Added to Table!")
            else:
                print("Error 404!")
            final=data[0:523]+table_data+data[-22:]
            df=pd.DataFrame(pd.read_html(final)[0])
            df.insert(loc=0, column="Symbol", value=symbol)
            all_in_one_df=pd.concat([all_in_one_df,df],ignore_index=True)
    final=data[0:523]+table_data+data[-22:]
    df=pd.DataFrame(pd.read_html(final)[0])
    name="Historical Data/"+symbol+".csv"
    df.to_csv(name)
all_in_one_df.to_csv("Historical Data/All in one.csv")
