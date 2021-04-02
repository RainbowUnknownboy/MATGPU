import csv
import json
import pandas
import datetime
import numpy as np
import pandas as pd
import dateutil.parser
import matplotlib.pyplot as plt
from es_pandas import es_pandas
from pandas import json_normalize
from datetime import datetime , timedelta
from sklearn import datasets, linear_model
from prediction_conf import dataname , hostnode
from elasticsearch import Elasticsearch ,helpers
from sklearn.linear_model import LinearRegression

pd.options.mode.chained_assignment = None
todaydata = datetime.today().utcnow().strftime('%Y.%m.%d')
yesterday = datetime.today().utcnow() - timedelta(1)
yesterdaydata = datetime.strftime(yesterday , '%Y.%m.%d')
beforeyesterday = datetime.today().utcnow() - timedelta(2)
beforeyesterdaydata = datetime.strftime(beforeyesterday , '%Y.%m.%d')


# create a client instance of the library
es = Elasticsearch(
    ['158.108.38.66'],
    http_auth = ('elastic' ,'1q2w3e4r'),
    port = 9200 , http_compress=True)

print("Connected to Database" ,datetime.now())


def call_data(host , name):
    res = es.search(index = ["ganglia-metrics-" + beforeyesterdaydata,"ganglia-metrics-" + yesterdaydata,"ganglia-metrics-" + todaydata] ,  body = {
    #res = es.search(index = ["ganglia-metrics-" + todaydata] ,  body = {
        "query" :{
            "bool" :{
                "must":[
                    {"match" :{"host" : host}},
                    {"match" :{"name" : name}},
                    ]
                    }
                } , 
                "sort": [
            {
            "@timestamp" : {
                "order" : "desc"
                        }
            }
                        ]
        }
        , size = 10000 
        , request_timeout = 60
                    )
    #return res 

    #print (res["hits"]["hits"])

    #print("Done")



    df_cpu = json_normalize(res['hits']['hits'])
    df_cpu = df_cpu.sort_values(by=["_source.@timestamp"] , ascending = True).reset_index()
    datareserve = df_cpu
    #print(datareserve)
    #print(df_cpu_filtered)
    df_cpu.shape[0]



    df_cpu["_source.@timestamp"] = list(map(dateutil.parser.parse, df_cpu["_source.@timestamp"]))
    df_cpu["minute"] = list(map(lambda v : v.minute, df_cpu["_source.@timestamp"]))
    df_cpu["hour"] = list(map(lambda v : v.hour, df_cpu["_source.@timestamp"])) 
    df_cpu["day"] = list(map(lambda v : v.day, df_cpu["_source.@timestamp"]))
    df_cpu["month"] = list(map(lambda v : v.month, df_cpu["_source.@timestamp"]))
    df_cpu["year"] = list(map(lambda v : v.year, df_cpu["_source.@timestamp"]))
    df_cpu["time"] = (pd.to_datetime(df_cpu["hour"].astype(str) + ':' + df_cpu["minute"].astype(str), format='%H:%M').dt.time)
    df_cpu["No"] = range(0,0+len(df_cpu))
    df_cpu["time"] = df_cpu["time"].astype(str)

    datafiltered = df_cpu[["year","month","day","time","_source.val"]].groupby(["year","month","day","time"]).agg(["mean"])
    datafiltered["No"] = range(0,0+len(datafiltered))
    datafiltered = datafiltered.reset_index()
    #print(datafiltered)


    modeldata = datafiltered["_source.val"]
    modelnum = datafiltered["No"]
    x = modeldata[(len(datafiltered)-400):(len(datafiltered)-200)]
    x = x.values.reshape((-1,1))
    y = modeldata[(len(datafiltered)-200):len(datafiltered)]
    y = y.values.reshape((-1,1))
    timetest = modelnum[(len(datafiltered)-200):len(datafiltered)]
    timetest = timetest.values.reshape((-1,1))
    #print(x,"\n",y)
    model = LinearRegression()
    model.fit(x,y)



    result = model.score(x,y)
    print('coefficient of determination:', result)
    print('intercept:', model.intercept_)
    print('slope:', model.coef_)



    predict_val = model.predict(y)
    #print('predicted response:', predict_val, sep='\n')

    '''
    plt.plot(timetest , model.predict(x), color='red',linewidth=2)
    plt.scatter(timetest , y,  color='black')
    plt.title('Test Data')
    plt.xlabel('Time')
    plt.ylabel('Percent')
    plt.xticks(())
    plt.yticks(())
    plt.show()
    '''




    dataedited = datareserve[(len(datareserve)-200):len(datareserve)]
    dataedited["val"] = predict_val.astype(float)
    #print(datafiltered)


    dataedited.drop(["_source.val" , "_source.@timestamp" , "index" , "minute" , "hour" ,"year","month", "day" , "time" , "No" , "_index" , "_type" , "_id" ,"sort"], 1 , inplace=True)
    def datetime_range(start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta

    dts = [dt.strftime("%Y-%m-%dT%H:%M:00.000Z") for dt in 
        datetime_range(datetime.utcnow(), datetime.utcnow() + timedelta(minutes = 199), 
        timedelta(minutes=1))]

    indexname =  "ganglia-metrics-prediction"
    for line in range (200):
        usename = indexname 
    dataedited["_source.@timestamp"] = dts
    #dataedited.columns = ["score", "type" ,"slope" , "name" , "@version" ,"log_host" , "tmax", "program" ,"units" ,"host" ,"val","@timestamp"]
    #dataedited.columns = ["score", "slope" ,"type" ,"dmax" , "name" , "program" , "@version" , "units" , "log_host" , "tmax" , "host" , "prediction" , "@timestamp"]
    dataedited.columns = ["score","name","slope", "type", "units" , "log_host", "host", "program", "@version", "tmax", "dmax", "prediction" , "@timestamp"]
    dataedited["prediction"].astype(float)
    #print(dataedited)

    return dataedited


#hostnode = [ "127.0.1.1" , "158.108.38.71" , "158.108.38.72"]

#dataname = ["cpu_user" , "cpu_system" , "cpu_idle" ,"disk_free" , "diskstat_sdb_write_bytes_per_sec" , "diskstat_sdb_read_bytes_per_sec" , "gpu0_util" , "gpu0_fb_memory" , "gpu0_temp" , "mem_buffers" , "mem_cached" , "mem_free" ,"bytes_in" , "bytes_out" ,"proc_total" ,"proc_run"]
#hostnode = [ "127.0.1.1" , "158.108.38.70" , "158.108.38.71" , "158.108.38.72" , "158.108.38.73" , "158.108.38.74" , "158.108.38.75" , "158.108.222.135" ,"158.108.222.136" , "158.108.222.137"]
#dataname = ["cpu_user" , "cpu_system" , "cpu_idle" ]
dataaggregate = pd.DataFrame()

for i in hostnode:
    for j in dataname:
        try:
            print(i,j)
            dataaggregate_new = call_data(i , j)
            #print(dataaggregate_new)
            dataaggregate_new = dataaggregate_new.reset_index(drop=True)
            dataaggregate = dataaggregate.append(dataaggregate_new)
            dataaggregate = dataaggregate.reset_index(drop=True)
            #print(dataaggregate) 
        except KeyError:
            pass
        except ValueError:
            pass


print(dataaggregate)
"""
dataaggregate = call_data("158.108.38.66" , "cpu_user")
print(dataaggregate)
dataaggregate = dataaggregate.reset_index(drop=True) 
dataaggregate_new = call_data("158.108.38.66" , "cpu_system")
#dataaggregate.append(call_data("cpu_system"))
dataaggregate_new = dataaggregate_new.reset_index(drop=True) 
dataaggregate = dataaggregate.append(dataaggregate_new)
dataaggregate = dataaggregate.reset_index(drop=True)
"""

es.delete_by_query(index = "ganglia-metrics-prediction" , body = {"query": {"match_all": {}}})
print("\n"+"Delete old data ", datetime.now())

es_host = "158.108.38.66:9200"
es_auth = 'elastic' 
password = '1q2w3e4r'
dataindex = "ganglia-metrics-prediction"
type = "_doc"
dtype = {"prediction" : "float"}
ep = es_pandas(es_host , http_auth = ("elastic","1q2w3e4r") , dtype = dtype)
ep.to_es(dataaggregate, dataindex, type)
print("\n"+"Success ", datetime.now())
