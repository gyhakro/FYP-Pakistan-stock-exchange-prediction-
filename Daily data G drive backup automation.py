import os
import time
import json
import naas
import os.path
import requests
import pandas as pd
import urllib.request
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

scps=["https://www.googleapis.com/auth/drive"]
creds= None
folders={}
list_companies=['ENGRO','LUCK', 'OGDC','FFC','HBL','HUBC','PPL', 'POL','EFERT', 'MCB','UBL','DGKC','PSO',
'SEARL','MLCF','BAHL','MARI','TRG','ATRL','UNITY','SYS','MEBL','GHNI','NML','PIOC','CHCC','PAEL','ISL','KAPCO','DAWH']

if os.path.exists("token.json"):
    creds=Credentials.from_authorized_user_file("token.json",scps)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow= InstalledAppFlow.from_client_secrets_file("client_secret.json",scps)
        creds= flow.run_local_server(port=0)
    with open("token.json",'w')as token:
        token.write(creds.to_json())
try:
    service= build("drive","v3",credentials=creds)
    response=service.files().list(
        q="name='FYP PSX daily data' and mimeType='application/vnd.google-apps.folder'",
        spaces='drive'
    ).execute()
    if not response['files']:
        file_metadata={
            "name":"FYP PSX daily data",
            "mimeType":"application/vnd.google-apps.folder",
        }
        file = service.files().create(body=file_metadata,fields="id").execute()
        parent_folder_id=file.get('id')
    else:
        parent_folder_id=response['files'][0]['id']
    for item in list_companies:
        time.sleep(2)
        service= build("drive","v3",credentials=creds)
        response=service.files().list(
            q=f"name='{item}' and mimeType='application/vnd.google-apps.folder'",
            spaces='drive'
        ).execute()
        if not response['files']:
            file_metadata={
                "name":item,
                "mimeType":"application/vnd.google-apps.folder",
                "parents":[parent_folder_id]
            }
            file = service.files().create(body=file_metadata,fields="id").execute()
            folder_id=file.get('id')
            folders[item]=folder_id
        else:
            folder_id=response['files'][0]['id']
        cnt= urllib.request.urlopen("https://dps.psx.com.pk/timeseries/int/"+item).read()
        df=pd.DataFrame(eval(cnt))
        loc= f"Daily data/{item}/{item} {time.strftime('%Y-%m-%d')}.csv"
        name= f"{item} {time.strftime('%Y-%m-%d')}.csv"
        mypath ="Daily data/"+item 
        if not os.path.isdir(mypath):
            os.makedirs(mypath)
        df.to_csv(loc)
        print(name," Downloaded")
        file_metadata={
            "name": name,
            "parents":[folder_id]
        }
        media= MediaFileUpload(loc)
        upload_file= service.files().create(body=file_metadata, media_body=media,fields="id").execute()
        print(name," backed up")
        subject="Data stored Successfully!"
        content='''The daily PSX data has been saved successfully to your google drive. Thank you!'''
        print("-"*50)
except HttpError as e:
    print("Error"+str(e))
    subject="Failled to store data!"
    content='''Data could not be saved to your google drive. please try to download manually!'''
email_to="email_address"
naas.notification.send(email_to,subject,content)
naas.scheduler.add(recurrence="0 17 * * 1,2,3,4,5")