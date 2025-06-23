#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


# cd home/karanverma


# In[2]:


#!/usr/bin/env python
# coding: utf-8

print('Starting of Notification score update')
import logging
import datetime
import time
import os

import time
start_time = time.time()

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


log_path = '/home/DS_Live_Trials/Notification_score/notification_update_logs/'
print(os.getcwd())
if not os.path.exists(log_path):
    os.makedirs(log_path)


# In[3]:


# In[5]:


import datetime
import numpy as np
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, udf, when
from pyspark.sql.types import *
from pyspark.sql import DataFrameStatFunctions as stat
import numpy as np
import pandas as pd
from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, udf, when
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql import Window

import requests
from collections import defaultdict
from pymongo import MongoClient
from tqdm import tqdm
from pyspark.sql import DataFrameStatFunctions as stat
from pyspark.sql import Window
import pyspark.sql.functions as F
import os
import time
import json
import pickle

NIS_DATA_BASE_PATH = "gs://nis-segment-datasource-v3/processed/"
NIS_OLD_DATA_BASE_PATH = "gs://nis-localytics-datasource/processed/"

# conf = SparkConf().setAll([('spark.driver.memory', '50g'), ('spark.broadcast.blockSize', '20m'), ("spark.executor.instances", '30')])
conf = SparkConf().setAll([('spark.sql.broadcastTimeout',1000)
    # ('spark.driver.memory', '20g'), \
#                            ('spark.broadcast.blockSize', '10m'),\
#                            ('spark.dynamicAllocation.enabled','False'),\
#                            ('spark.executor.instances','9999')
# #                            ,("spark.sql.autoBroadcastJoinThreshold",-1)
                          ])
sc = SparkContext.getOrCreate(conf=conf)
#sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
#sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# In[6]:


# In[4]:


def get_path(dates, prefix, padding=None):
    dates_ = dates + []
    if padding:
        st_date, ed_date = sorted(dates)[0], sorted(dates)[-1]
        for i in range(1, 4):
            d = (datetime.datetime.strptime(ed_date, date_fmt) + datetime.timedelta(days=i)).strftime(date_fmt)
            if d < datetime.datetime.today().strftime(date_fmt):
                dates_.append(d)
    dates_ = list(set(dates_) - set(["2019/02/17", "2019/02/18", "2019/05/28", "2019/06/03", "2019/07/02", "2019/07/03", "2019/07/04", "2019/11/13", "2019/11/14", "2020/02/22", "2020/03/31", "2020/04/16", "2020/04/18", "2020/05/11", "2021/05/13"]))
    paths = []
    for date in dates_:
        base_path = NIS_DATA_BASE_PATH
        if date < "2018/06/26":
            base_path = NIS_OLD_DATA_BASE_PATH
        paths.append(base_path + date + "/" + prefix + "/*.parquet")
    return paths

date_fmt = "%Y/%m/%d"
month_fmt = "%Y/%m"

def millis2date(x):
    try:
        if x < 15000000000:
            return datetime.datetime.fromtimestamp(x).strftime(date_fmt)
        else:
            return datetime.datetime.fromtimestamp(x / 1000.).strftime(date_fmt)
    except:
        return "1970/01/01"

def millis2month(x):
    try:
        if x < 15000000000:
            return datetime.datetime.fromtimestamp(x).strftime(month_fmt)
        else:
            return datetime.datetime.fromtimestamp(x / 1000.).strftime(month_fmt)
    except:
        return "1970/01"

millis2date_udf = F.udf(millis2date, StringType())
millis2month_udf = F.udf(millis2month, StringType())

def divide_maps(d1, d2):
    keys = set(d1.keys()).intersection(set(d2.keys()))
    res = {}
    for k in keys:
        res[k] = d1[k] * 1. / (d2[k] + 1e-10)
    return res

def timediff(y, x, date_fmt="%Y/%m/%d"): 
    end = datetime.datetime.strptime(y, date_fmt)
    start = datetime.datetime.strptime(x, date_fmt)
    delta = (end - start).days
    return delta

def monthdiff(y, x, month_fmt="%Y/%m"): 
    millis = y - x
    delta = millis / (1000 * 3600 * 24 * 30)
    return delta

timediff_udf = udf(timediff, IntegerType())
monthdiff_udf = udf(monthdiff, IntegerType())

def filter_platform(data, platform=None):
    if platform == "ANDROID":
        data = data.filter(data.platform == "ANDROID")
    elif platform == "IOS":
        data = data.filter(data.platform != "ANDROID")
    return data

def filter_category(data, categories=None):
    if categories:
        data = data.filter(data.categoryWhenEventHappened.isin(categories))
    return data

def filter_tenant(data, tenant=None):
    if tenant in ['hi', 'HINDI']:
        data = data.filter(data.tenant.isin(['hi', 'HINDI', 'Hindi', 'hindi']))
    elif tenant in ['en', 'ENGLISH']:
        data = data.filter(~data.tenant.isin(['hi', 'HINDI', 'Hindi', 'hindi']))
    return data


def filter_app(data, app_name=None):
    if 'appName' in data.columns:
        if app_name:
            data = data.filter(data.appName == app_name)
        else:
            data = data.filter((data.appName != "mini") & (data.appName != "crux"))
    return data


# In[7]:


# In[5]:


def get_raw_path(date, hours=None):
    paths = []
    base_path = NIS_RAW_DATA_BASE_PATH + date
    if not hours:
        return base_path + "/*/*.gz"
    for hour in hours:
        paths.append(base_path + "/" + str(hour).zfill(2) + "/*.gz")
    return ",".join(paths)

def process_raw_data(paths):
    def view_data_filters(x):
        x = x['properties']
        deviceid_filter = ('deviceId' in x) and (x['deviceId'] != '')
        time_filter = ('timeSpent' in x) and (int(x['timeSpent']) <= 100) and (int(x['timeSpent']) >= 0)
        return deviceid_filter and time_filter

    try:
        rdd = sc.textFile(paths)             .map(json.loads)             .filter(lambda x: "batch" in x).flatMap(lambda x: x["batch"])             .filter(lambda x: ("event" in x) and (x["event"].lower() == "timespent-front"))             .filter(view_data_filters)             .map(lambda x: x['properties'])
        view_data = rdd.map(lambda x: (x['deviceId'], x['hashId'][:-2], x['timeSpent']))             .toDF(['deviceId', 'hashId', 'timeSpent'])
        
        view_data = view_data.filter(view_data.timeSpent.isNotNull())
        #view_data = view_data.filter((getHashBucketUDF(view_data.deviceId) >= 16) & (getHashBucketUDF(view_data.deviceId) <= 25))
        view_data = view_data.groupby(view_data.deviceId, view_data.hashId)                              .agg(F.max(view_data.timeSpent).alias('overallTimeSpent'))
        
        return view_data
    except Exception as e:
        logger.warning("Error processing data: " + str(e))


# In[8]:


# In[6]:


from Utils import *
def getNewsData(d1, d2):
    newsMap = getNewsInDates(d1, d2)
    hashIdList = list(newsMap.keys())

    hashIdsWithFilter = []
    for h in hashIdList:
        if 'newsLanguage' in newsMap[h] and newsMap[h]['newsLanguage'] == 'english' and newsMap[h]['publishGroupList'][0]['countryCode'] == 'IN':
            hashIdsWithFilter.append(h.split('-')[0])
    
    return hashIdsWithFilter, newsMap


# In[9]:


# In[7]:


import logging

logger = logging.getLogger(str(datetime.datetime.today().date()))
hdlr = logging.FileHandler(
    log_path + str(datetime.datetime.today().date()) + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


# In[10]:


# In[8]:


def Log(s, flag=True):
    if flag:
        logger.info(s)
        print(s)

Log('----', flag=True)


# In[9]:


# In[120]:


from pymongo import UpdateOne
import datetime
import logging
import os
import shutil
import sys
import json

import numpy as np
from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from Utils import *
from tqdm import tqdm


def getDeviceVectorsFromMongo(deviceIds):
    deviceVectorMap = {}
    cursor = deviceVectorCollection.find({"_id" : { "$in" : deviceIds }})
    
    for c in cursor:
        deviceVectorMap[c['_id']] = np.array(c['vector'])
    
    return deviceVectorMap
        
def insertDeviceVectorsInMongo(deviceVectors):
    
    collection = deviceVectorCollection
    ops = []
    results = []
    for deviceId in tqdm(deviceVectors):
        key = {"_id" : deviceId}
        devData = {"$set" : {"vector" : deviceVectors[deviceId]}}
        ops.append(UpdateOne(key, devData, upsert=True))
    
        if len(ops) == 100000:
            results.append(collection.bulk_write(ops,ordered=False))
            ops = []

    if len(ops) > 0:
        results.append(collection.bulk_write(ops,ordered=False))

oldClusters = {}
def getDeviceClustersFromMongo(deviceIds):
    Log("collection check: %s" %deviceClusterCollection)
    deviceClusterMap = {}
    if len(deviceIds) == 0:
        cursor = deviceClusterCollection.find()
    else:
        cursor = deviceClusterCollection.find({"_id" : { "$in" : deviceIds }})
    
    for c in cursor:
        deviceClusterMap[c['_id']] = c['cluster']
        oldClusters[c['_id']] = c['cluster']
    
    return deviceClusterMap

"""deprecated"""
# def insertDeviceClustersInMongo(deviceClusters):
#     collection = deviceClusterCollection
#     ops = []
#     results = []
    
#     for deviceId in deviceClusters:
#         key = {"_id" : deviceId}
        
#         devData = {"$set" : {"cluster" : int(deviceClusters[deviceId])}}
        
#         if deviceId not in oldClusters or int(deviceClusters[deviceId]) != oldClusters[deviceId]:
#             devData = {"$set" : {"cluster" : int(deviceClusters[deviceId]), "updatedAt" : datetime.datetime.now()}}
            
#         ops.append(UpdateOne(key, devData, upsert=True))
    
#         if len(ops) == 1000:
#             results.append(collection.bulk_write(ops,ordered=False))
#             ops = []

#     if len(ops) > 0:
#         results.append(collection.bulk_write(ops,ordered=False))
        
def getNewsVectorsFromMongo(hashIds):
    newsVectorMap = {}
    cursor = newsVectorCollection.find({"_id" : { "$in" : hashIds }})
    
    for c in cursor:
        newsVectorMap[c['_id']] = np.array(c['vector'])
    
    return newsVectorMap

def insertNewsVectorsInMongo(newsVectors):
    for hashId in newsVectors:
        key = {"_id" : hashId}
        newsData = {"$set" : {"vector" : list(newsVectors[hashId])}}
        newsVectorCollection.update_one(key, newsData, upsert=True)
        
def getNewsTSpentFromMongo(hashIds):
    newsTSpentMap = {}
    cursor = newsTSpentCollection.find({"_id" : {"$in" : hashIds }})
    
    for c in cursor:
        data = {}
        for cluster in c['tSpent']:
            data[int(cluster)] = c['tSpent'][cluster]
        newsTSpentMap[c['_id']] = data
        
    return newsTSpentMap

def insertNewsTSpentInMongo(newsTSpentMap):
    for hashId in newsTSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsTSpentMap[hashId]:
            data[str(cluster)] = newsTSpentMap[hashId][cluster]
        tSpentData = {"$set" : {"tSpent" : data}}
        newsTSpentCollection.update_one(key, tSpentData, upsert=True)

def getNewsTScoreInMongo(hashIds):
    newsTScoreMap = {}
    cursor = newsTScoreCollection.find({"_id" : {"$in" : hashIds }})
    
    for c in cursor:
        data = {}
        for cluster in c['tSpent']:
            data[int(cluster)] = c['tSpent'][cluster]
        newsTScoreMap[c['_id']] = data
        
    return newsTScoreMap

def insertNewsTScoreInMongo(newsTSpentMap):
    for hashId in newsTSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsTSpentMap[hashId]:
            if newsTSpentMap[hashId][cluster][1] > 1:
                data[str(cluster)] = newsTSpentMap[hashId][cluster][0] / newsTSpentMap[hashId][cluster][1]  
        
        scoreData = {"$set" : {"tSpent" : data, "updatedAt" : datetime.datetime.now()}}
        newsTScoreCollection.update_one(key, scoreData, upsert=True)
        
        
def getNewsNotiSpentFromMongo(hashIds):
    newsNotiSpentMap = {}
    cursor = newsNotiSpentCollection.find({"_id" : {"$in" : hashIds }})
    
    for c in cursor:
        data = {}
        for cluster in c['tSpent']:
            data[int(cluster)] = c['tSpent'][cluster]
        newsNotiSpentMap[c['_id']] = data
        
    return newsNotiSpentMap

def insertNewsNotiSpentInMongo(newsNotiSpentMap):
    for hashId in newsNotiSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsNotiSpentMap[hashId]:
            data[str(cluster)] = newsNotiSpentMap[hashId][cluster]
        NotiSpentData = {"$set" : {"tSpent" : data}}
        newsNotiSpentCollection.update_one(key, NotiSpentData, upsert=True)

def getNewsNotiScoreInMongo(hashIds):
    newsNotiScoreMap = {}
    cursor = newsNotiScoreCollection.find({"_id" : {"$in" : hashIds }})
    
    for c in cursor:
        data = {}
        for cluster in c['tSpent']:
            data[int(cluster)] = c['tSpent'][cluster]
        newsNotiScoreMap[c['_id']] = data
        
    return newsNotiScoreMap

def insertNewsNotiScoreInMongo(newsNotiSpentMap):
    for hashId in newsNotiSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsNotiSpentMap[hashId]:
            if newsNotiSpentMap[hashId][cluster][1] > 1:
                data[str(cluster)] = newsNotiSpentMap[hashId][cluster][0] / newsNotiSpentMap[hashId][cluster][1]  
        
        notiScoreData = {"$set" : {"tSpent" : data, "updatedAt" : datetime.datetime.now()}}
        newsNotiScoreCollection.update_one(key, notiScoreData, upsert=True)
        
        
def insertDeviceClustersInMongo(deviceClusters, oldClusters, Log):
    ops = []
    #results = []
    
    cntUpdated = 0
    for deviceId in deviceClusters:
        key = {"_id" : deviceId}
        
        devData = {"$set" : {"cluster" : int(deviceClusters[deviceId]), "updatedAt" : datetime.datetime.now()}}
        
        if deviceId in oldClusters and deviceClusters[deviceId] == oldClusters[deviceId]:
            continue
        
        cntUpdated += 1
        ops.append(UpdateOne(key, devData, upsert=True))
    
        if len(ops) == 100000:
            try:
                result = deviceNotiClustersCollection.bulk_write(ops,ordered=False)
                ops = []
            except Exception as e:
                ops = []
                Log("Failed to insert devices clusters, " + str(e))
        
        if cntUpdated % 100000 == 0:
            Log("Inserted " + str(cntUpdated) + " Device Clusters")
                
    if len(ops) > 0:
        deviceNotiClustersCollection.bulk_write(ops,ordered=False)
    
    Log("Updated " + str(cntUpdated / len(deviceClusters) * 100) + " device clusters")


# In[16]:


#from Colab_TF import *



#### Hash definition
def InternalSeqHash(deviceId):
    h = 0
    for c in list(deviceId):
        h = (31*h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000

def hashIdentifier(deviceId, M=100):
    hId = (abs(InternalSeqHash(deviceId)) % M) + 1
    return hId

### Initializer
def getDeviceVectors(deviceIds):
    
    deviceIdsSplit = np.array_split(deviceIds, 4)
    deviceVectorMap = {}
    resetCount = 0
    
    for devIds in deviceIdsSplit:
        devIdsList = list(devIds)
        deviceVectorMapProx = getDeviceVectorsFromMongo(devIdsList)
        for devId in devIdsList:
            if devId not in deviceVectorMapProx or not allVectorsOkay(deviceVectorMapProx[devId]) or len(deviceVectorMapProx[devId]) != NUM_DIMS:
                deviceVectorMap[devId] = np.random.randn(NUM_DIMS)
                resetCount += 1
            else:
                deviceVectorMap[devId] = deviceVectorMapProx[devId]

    logger.info("reset " + str(resetCount / len(deviceIds) * 100) + " % device vectors the db")
    
    return deviceVectorMap
        
def getNewsVectors(hashIds):
    newsVectorMap = getNewsVectorsFromMongo(hashIds)
    resetCount = 0
    for hashId in hashIds:
        """todo get it from mongo"""
        if hashId not in newsVectorMap or not allVectorsOkay(newsVectorMap[hashId]) or len(newsVectorMap[hashId]) != NUM_DIMS:
            newsVectorMap[hashId] = np.random.randn(NUM_DIMS)
            resetCount += 0
        
    logger.info("reset " + str(resetCount / len(hashIds) * 100) + " % news vectors the db")
    return newsVectorMap

def getNewsTSpentData(hashIds):
    newsTSpentMap = getNewsTSpentFromMongo(hashIds)
    for hashId in hashIds:
        if hashId not in newsTSpentMap:
            newsTSpentMap[hashId] = {}
        
        for cluster in range(NUM_CLUSTERS):
            if cluster not in newsTSpentMap[hashId]:
                newsTSpentMap[hashId][cluster] = (6, 1)
    
    return newsTSpentMap

def getNewsNotiSpentData(hashIds):
    newsNotiSpentMap = getNewsNotiSpentFromMongo(hashIds)
    for hashId in hashIds:
        if hashId not in newsNotiSpentMap:
            newsNotiSpentMap[hashId] = {}
        
        #base open_rate for all hashid's
        for cluster in range(NUM_CLUSTERS):
            if cluster not in newsNotiSpentMap[hashId]:
                newsNotiSpentMap[hashId][cluster] = (0,0)
    
    return newsNotiSpentMap

def getDeviceClusters(deviceIds):
    deviceIdsSplit = np.array_split(deviceIds, 4)
    deviceClusterMap = {}
    
    for devIds in deviceIdsSplit:
        devIdsList = list(devIds)
        deviceClusterMapProx = getDeviceClustersFromMongo(devIdsList)
        for devId in devIds:
            if devId not in deviceClusterMap or deviceClusterMapProx[devId] >= NUM_CLUSTERS:
                deviceClusterMap[devId] = hashIdentifier(devId, NUM_CLUSTERS) - 1
            else:
                deviceClusterMap[devId] = deviceClusterMapProx[devId]
        
    return deviceClusterMap


# In[10]:


notiType_filter = 'CLUSTER_TEST'
min_shown_event_threshold  = 30


# In[11]:


def get_raw_path_notification(date, hours=None):
    paths = []
    base_path = NIS_RAW_DATA_BASE_PATH + date
    if not hours:
        return base_path + "/*/*.gz"
    for hour in hours:
        paths.append(base_path + "/" + str(hour).zfill(2) + "/*.gz")
    return ",".join(paths)

def process_raw_data_notification(paths):
    def view_data_filters(x):
        x = x['properties']
        deviceid_filter = ('device_id' in x) and (x['device_id'] != '')
        noti_filter = ('notiOpened' in x) and (int(x['timeSpent']) <= 100)
        return deviceid_filter and time_filter

    try:
        rdd = sc.textFile(paths)             .map(json.loads)   .filter(lambda x: ("event_name" in x) and ( (x["event_name"].lower() == "notification opened") or (x["event_name"].lower() == "notification shown") ))            
        view_data = rdd.map(lambda x: (x['device_id'], x['hashId'][:-2], x['short_time'], x['noti_opened'], x['noti_shown'], x['event_name'], x['notification_type']))             .toDF(['deviceId', 'hashId', 'timeSpent', 'notiOpened', 'notiShown', 'event_name', 'notiType'])
        
        view_data = view_data.filter(view_data.notiType == notiType_filter)
        view_data = view_data.withColumn("notiOpened",F.when(view_data.event_name == 'Notification Opened', 1).otherwise(0))
        view_data = view_data.withColumn("notiShown",F.when(view_data.event_name == 'Notification Shown', 1).otherwise(0))
        view_data = view_data.select('deviceId', 'hashId', 'timeSpent', 'notiOpened', 'notiShown')  .groupby(view_data.deviceId, view_data.hashId)                              .agg(F.max(view_data.timeSpent).alias('timeSpent'),F.sum(view_data.notiOpened).alias('notiOpened'),F.sum(view_data.notiShown).alias('notiShown'))
        view_data = view_data.withColumn("notiShown",F.when(view_data.notiShown < view_data.notiOpened, 
                                                     view_data.notiOpened).otherwise(view_data.notiShown))
        return view_data
    except Exception as e:
        logger.warning("Error processing data: " + str(e))


# In[12]:


n_days = 7
st_date = datetime.datetime.now() - datetime.timedelta(days=n_days+1)

date_fmt = "%Y/%m/%d"

dates = [(st_date + datetime.timedelta(days=i)) for i in range(n_days+1)]
dates.sort()

dates_str = [date.strftime(date_fmt) for date in dates]
print(dates_str)
# millisMin = dates[0].timestamp() * 1000
# millisMax = (dates[-1] + datetime.timedelta(days=1)).timestamp() * 1000

hashIdsWithFilter, newsMap = getNewsData(dates[0], datetime.datetime.now())


# In[13]:


NIS_RAW_DATA_BASE_PATH = "gs://inshorts-minimal-event/data/inshorts-minimal-event-v1/"

days = 3
for d in range(days):
    raw_date = datetime.datetime.now() - datetime.timedelta(days=d)
    path = get_raw_path_notification(raw_date.strftime("%Y/%m/%d"))
    if d == 0:
        Log(path)
        today_data = process_raw_data_notification(path)
    else:
        Log(path)
        today_data.union(process_raw_data_notification(path))
today_data = today_data.filter(today_data.hashId.isin(hashIdsWithFilter))
Log("data loaded for above paths")
# In[14]:


# datestr = [d.strftime(date_fmt) for d in dates]
# paths = get_path(datestr, 'otherEvents')

# data = sqlContext.read.parquet(*paths)

# data = filter_app(data, app_name=None)
# data = filter_tenant(data, tenant='en')

# # data = data.filter((data.eventTimestamp > millisMin) & (data.eventTimestamp < millisMax))

# data = data.select(data.deviceId, (F.split(data.hashId, '-')[0]).alias('hashId'), data.overallTimeSpent, data.notificationOpened, data.notificationShown, data.notificationType,data.eventName, data.notificationType)
# data = data.filter( data.eventName.isin(['Notification Shown','Notification Opened']) )
# data = data.filter(data.notificationType == notiType_filter)

# data = data.filter(data.hashId.isin(hashIdsWithFilter))


# # In[15]:


# data = data.select('deviceId', 'hashId' ,'overallTimeSpent', 'notificationOpened', 'notificationShown')            .groupby('deviceId', 'hashId')            .agg(F.max('overallTimeSpent').alias('timeSpent'), 
#                  F.sum('notificationShown').alias('notiShown'),F.sum('notificationOpened').alias('notiOpened'))


# # In[16]:


# data = data.union(today_data)
# data.cache()
data = today_data
data = data.withColumn("notiShown",F.when(data.notiShown < data.notiOpened, 
                                                     data.notiOpened).otherwise(data.notiShown))


# Log("Data Loaded for %s days."%n_days)


# In[17]:


suffixes = ['CV_High', 'CV_Low', 'CV_Med', 'TF_IDF_High','TF_IDF_Low','TF_IDF_Med' ]
mongoClient = getMongoClient()
schema = StructType([
    StructField("deviceId", StringType(), True)
])
insert_start = time.time()

for suffix in suffixes:
    
    
    trialDB = mongoClient['trialDB_'+ suffix]
    deviceVectorCollection = trialDB['deviceVectors']
    deviceClusterCollection = trialDB['notiDeviceClusters']

    newsVectorCollection = trialDB['newsVectors']
    newsTSpentCollection = trialDB['newsSpent']
    newsTScoreCollection = trialDB['newsScores']
    newsNotiSpentCollection = trialDB['newsNotiSpent']
    newsNotiScoreCollection = trialDB['newsNotiScores']
    deviceNotiClustersCollection = trialDB['notiDeviceClusters']

    # clusterCentersCollection = trainDB['clusterCenters']
    Log(trialDB) 
    
    deviceClusterMapExisiting = getDeviceClustersFromMongo([])
    Log("device length in mongo: %s" %len(deviceClusterMapExisiting))
    
    
    devices = [d for d,c in deviceClusterMapExisiting.items()]
    devices = pd.DataFrame(devices, columns = ['deviceId'])

    devices = sqlContext.createDataFrame(devices, schema)
    
    data_devices = data.join(devices,["deviceId"],"inner")
    
    dataWithClusters = data_devices.select('hashId', 'notiOpened','notiShown', F.udf(lambda deviceId: deviceClusterMapExisiting[deviceId], IntegerType())('deviceId').alias('cluster'))
    newsClusterNotiMean = dataWithClusters.groupby('hashId', 'cluster').agg(F.sum('notiOpened'),F.sum('notiShown'),F.count('hashId'))
    newsClusterNotiMeanDf = newsClusterNotiMean.toPandas()
    Log(newsClusterNotiMeanDf.shape)
    
    newsNotiSpentMap = defaultdict(lambda: defaultdict(lambda: (7, 1)))
    for v in tqdm(newsClusterNotiMeanDf.values):
        newsNotiSpentMap[v[0]][v[1]] = (v[2], v[3])
        
        
    newsNotiSpentMap_filter = defaultdict(lambda: defaultdict(lambda: (7, 1)))
    for v in tqdm(newsClusterNotiMeanDf.values):
        if v[3]>=min_shown_event_threshold:
            newsNotiSpentMap_filter[v[0]][v[1]] = (v[2], v[3])
        
    Log("Inserting News Notification Score Data")
    print(len(newsNotiSpentMap_filter), len(newsNotiSpentMap) )
    
    trialDB.drop_collection('newsNotiSpent')
    trialDB.drop_collection('newsNotiScores')
    
    insertNewsNotiSpentInMongo(newsNotiSpentMap_filter)
    insertNewsNotiScoreInMongo(newsNotiSpentMap_filter)
    insert_time = (time.time() - insert_start)
    Log("Inserting News Notification Score took %s percent of time" % str(100 * insert_time / (time.time() - start_time)))

Log("Exectution finished successfully, in : %s minutes, %s seconds " % (int((time.time() - start_time) / 60), int((time.time() - start_time) % 60)), flag=True)

