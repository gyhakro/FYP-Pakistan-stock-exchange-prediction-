import naas
import pandas as pd
import urllib.request
import times
import os
import json
import requests
list_companies=['ENGRO','LUCK', 'OGDC','FFC','HBL','HUBC','PPL', 'POL','EFERT', 'MCB','UBL','DGKC','PSO',
'SEARL','MLCF','BAHL','MARI','TRG','ATRL','UNITY','SYS','MEBL','GHNI','NML','PIOC','CHCC','PAEL','ISL','KAPCO','DAWH']
for item in list_companies:
    cnt= urllib.request.urlopen("https://dps.psx.com.pk/timeseries/int/"+item).read()
    df=pd.DataFrame(eval(cnt))
    loc= f"Daily data/{item}/{time.strftime('%Y-%m-%d')}.csv"
    name= item+" "+time.strftime('%Y-%m-%d')
    mypath ="Daily data/"+item 
    if not os.path.isdir(mypath):
       os.makedirs(mypath)
    df.to_csv(loc)
    print(name," Added to Directory")
    time.sleep(2)
    
    headers = {"Authorization": "Bearer ###Access token###"}
    para = {
        "name": name,
        "parents":["@Directory Address/ID"]
    }
    files = {
        'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
        'file': open(loc, "rb")
    }
    r = requests.post(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
        headers=headers,
        files=files
    )
#     print(r.text)
    if len(r.text)>200:
        print("Error!")
        subject="Failled to store data!"
        content='''Data could not be saved to your google drive.
        please try to download manually!'''
    else:
        print(name," Added to Google Dive")
        subject="Data stored Successfully!"
        content='''The daily PSX data has been saved successfully to your google drive.
        Thank you!'''
    print("-"*50)
email_to="#email Address"
naas.notification.send(email_to,subject,content)
naas.scheduler.add(recurrence="0 6 * * 1,2,3,4,5")


















# //////////////////////////////////////////////////////////////////////////////////
# code for uploading files to google Drive
# //////////////////////////////////////////////////////////////////////////////////


# import json
# import requests
# headers = {"Authorization": "Bearer ###Access token###"}
# para = {
#     "name": "test.jpg",
# }
# files = {
#     'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
#     'file': open("pic.jpg", "rb")
# }
# r = requests.post(
#     "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
#     headers=headers,
#     files=files
# )
# print(r.text)
