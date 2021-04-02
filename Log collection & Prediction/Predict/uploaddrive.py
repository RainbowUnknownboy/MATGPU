import os 
import sys
import subprocess
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime , timedelta

todaydata = datetime.today().utcnow().strftime('%Y.%m.%d')
yesterday = datetime.today().utcnow() - timedelta(1)
yesterdaydata = datetime.strftime(yesterday , '%Y.%m.%d')

command = f' -ho 158.108.38.66 -p 9200 -u elastic -pw 1q2w3e4r -i ganglia-metrics-{yesterdaydata} -f "@timestamp,@version,dmax,host,log_host,name,program,slope,tmax,type,units,val" -o 10m -b 5000 -pcs 2500000 -e /home/hpcnc/cronrun/csv/hpcnc-metrics-{yesterdaydata}.csv'
elasticpath = "/usr/local/bin/elasticsearch_tocsv"
command = str(elasticpath + command)
getcsv = os.system(command)
#getcsv = subprocess.run(command , shell = True)

os.chdir("/home/hpcnc/cronrun")
print("Start Upload "+ f"hpcnc-metrics-{yesterdaydata}.csv")
gauth = GoogleAuth()  
gauth.LocalWebserverAuth()        
drive = GoogleDrive(gauth) 
changepath = str(f"/home/hpcnc/cronrun/csv/hpcnc-metrics-{yesterdaydata}.csv")

with open(changepath,"r") as file:
    file_drive = drive.CreateFile({"parents":[{"id": "1POD82HG_7hKa8ur4CqEorFbP3tRxEHEG"}] ,"title":os.path.basename(file.name)})
    #file_drive = drive.CreateFile({'title':os.path.basename(file.name) })  
    file_drive.SetContentString(file.read()) 
    file_drive.Upload()
print("Upload " + f"hpcnc-metrics-{yesterdaydata}.csv" + " Successfully")
