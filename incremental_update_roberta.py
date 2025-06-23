#!/usr/bin/env python
# coding: utf-8

# In[9]:
print('Starting of NewsScore Incremental Update')
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

# In[10]:

print(os.getcwd())
if not os.path.exists('/home/karanverma/live_algo_logs/roberta_base_idf1/score_logs/'):
    os.makedirs('/home/karanverma/live_algo_logs/roberta_base_idf1/score_logs/')
    
from pymongo import MongoClient
from pymongo import UpdateOne

def getMongoClient():
    hosts = ['171.16.11.97','171.16.11.96','171.16.11.94']
    host = ','.join([i + ":27017" for i in hosts])
    conn_url = 'mongodb://root:superman@' + host
    client = MongoClient(conn_url)
    return client

def getMongoColl(db_name, coll_name, hosts=[]):
    host = ','.join([i + ":27017" for i in hosts])
    conn_url = 'mongodb://root:superman@' + host + '/' + db_name
    client = MongoClient(conn_url, minPoolSize=10, maxPoolSize=None)
    return client[db_name][coll_name]

def getNewsInHashIds(hashIds):
    news_coll = getMongoColl(db_name='nis-news', 
                           coll_name='News', 
                           hosts=['172.16.11.196', '172.16.11.195', '172.16.11.194'])
    
    newsMap = {}
    array = [t for t in news_coll.find({'_id' : {"$in" : hashIds}})]
    
    for i in range(len(array)):
        newsMap[array[i]['_id']] = array[i]
        
    return newsMap

def getNewsInDates(begin, end):
    news_coll = getMongoColl(db_name='nis-news', 
                           coll_name='News', 
                           hosts=['172.16.11.196', '172.16.11.195', '172.16.11.194'])
    
    newsMap = {}
    array = [t for t in news_coll.find({'createdAt' : {"$gt" : begin, "$lt" : end}})]
    
    for i in range(len(array)):
        newsMap[array[i]['_id']] = array[i]
        
    return newsMap


# In[11]:


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




NIS_RAW_DATA_BASE_PATH = "gs://inshorts-minimal-event/data/inshorts-minimal-event-v1/"

# fetching and processing data
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

def get_path(dates, prefix='timeSpentFrontEvents', padding=None):
    date_fmt = "%Y/%m/%d"
    dates_ = dates + []
    if padding:
        st_date, ed_date = sorted(dates)[0], sorted(dates)[-1]
        for i in range(1, 4):
            d = (datetime.datetime.strptime(ed_date, date_fmt) + datetime.timedelta(days=i)).strftime(date_fmt)
            if d < datetime.datetime.today().strftime(date_fmt):
                dates_.append(d)
    dates_ = list(set(dates_) - {"2019/02/17", "2019/02/18", "2019/05/28", "2019/06/03",
                                 "2019/07/02", "2019/07/03", "2019/07/04"})
    paths = []
    for date in dates_:
        base_path = NIS_DATA_BASE_PATH
        if date < "2018/06/26":
            base_path = NIS_OLD_DATA_BASE_PATH
        paths.append(base_path + date + "/" + prefix + "/*.parquet")
    return paths

def process_parquet_data(training_dates):
    try:
        paths = get_path(training_dates)
        data = sqlContext.read.parquet(*paths)
        data = data.filter(~data.appName.isin(['mini', 'crux']))
        view_data = data.filter(data.deviceId != '')
        view_data = view_data.filter((view_data.shortTime > 1) & (view_data.shortTime < 60))
        time_udf = F.udf(lambda x: max(min(20., x), 0.) / 4 if (1 < x < 60) else -1., FloatType())
        gid_udf = F.udf(lambda x: x[:-2] if x else "", StringType())
        view_data = view_data.withColumn("timeSpent", time_udf("shortTime"))
        view_data = view_data.select(view_data.deviceId, gid_udf(view_data.hashId).alias('hashId'),
                                     view_data.timeSpent)
        view_data = view_data.groupby(view_data.deviceId, view_data.hashId)                             .agg(F.max(view_data.timeSpent).alias('timeSpent'))
        return view_data
    except Exception as e:
        logger.warning("Error processing data: " + str(e))



# In[12]:





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
        deviceid_filter = ('device_id' in x) and (x['device_id'] != '')
        time_filter = ('timeSpent' in x) and (int(x['timeSpent']) <= 100)
        return deviceid_filter and time_filter

    try:
        rdd = sc.textFile(paths)             .map(json.loads)   .filter(lambda x: ("event_name" in x) and ( (x["event_name"].lower() == "timespent-front") or (x["event_name"].lower() == "notification opened") or (x["event_name"].lower() == "notification shown") ))            
        view_data = rdd.map(lambda x: (x['device_id'], x['hashId'][:-2], x['short_time'], x['noti_opened'], x['noti_shown'], x['event_name']))             .toDF(['deviceId', 'hashId', 'timeSpent', 'notiOpened', 'notiShown', 'event_name'])
        view_data = view_data.groupby(view_data.deviceId, view_data.hashId, view_data.event_name)                              .agg(F.max(view_data.timeSpent).alias('timeSpent'),F.sum(view_data.notiOpened).alias('notiOpened'),F.sum(view_data.notiShown).alias('notiShown'))
        return view_data
    except Exception as e:
        logger.warning("Error processing data: " + str(e))



# In[13]:





logger = logging.getLogger(str(datetime.datetime.today().date()))
hdlr = logging.FileHandler(
    '/home/karanverma/live_algo_logs/roberta_base_idf1/score_logs/' + str(datetime.datetime.today().date()) + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


def Log(s, flag=True):
    if flag:
        logger.info(s)
        print(s)

Log('----', flag=True)


# In[14]:



SPLITS = 1

## to do
workingDate = datetime.datetime.now() - datetime.timedelta(minutes=20)
time2 = datetime.datetime.now() - datetime.timedelta(minutes=80)
time3 = datetime.datetime.now() - datetime.timedelta(minutes=140)

hours = [time3.hour, time2.hour, workingDate.hour]

if time2.hour == 23 and workingDate.hour == 0:
    hours = [workingDate.hour]
if time2.hour == 0 and workingDate.hour == 1:
    hours = [time2.hour, workingDate.hour]

path = get_raw_path(workingDate.strftime("%Y/%m/%d"), hours)
print(path)        
view_data = process_raw_data(path)


abcset=sqlContext.read.parquet("gs://pvtrough_asia_south1/tf_idf/devices_fulfilling_criteria")
df_temp=abcset.filter(abcset.set_typ == 'roberta').select(['deviceid']).distinct()
df_temp=df_temp.withColumnRenamed("deviceid","deviceId")

view_data=view_data.join(df_temp,["deviceId"],"inner")

view_data = view_data.withColumn("notiOpened",F.when(view_data.event_name == 'Notification Opened', 1).otherwise(0))
view_data = view_data.withColumn("notiShown",F.when(view_data.event_name == 'Notification Shown', 1).otherwise(0))

# view_data=view_data.join(df_temp,["deviceId"],"inner")
view_data.cache()

data_chunks = view_data.randomSplit([1.] * SPLITS)










# In[15]:


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
from tqdm import tqdm

def getMongoClient():
    hosts = ['171.16.11.97','171.16.11.96','171.16.11.94']
    host = ','.join([i + ":27017" for i in hosts])
    conn_url = 'mongodb://root:superman@' + host
    client = MongoClient(conn_url)
    return client

trial_algo_suffix = '_roberta'
NUM_DIMS = 768
NUM_CLUSTERS = 1000

ALPHA = 1e-4
LAMMDA = 1e-5
CLUSTER_SHIFT_ALPHA = .9


mongoClient = getMongoClient()
trialDB = mongoClient['trialDB'+trial_algo_suffix]
deviceVectorCollection = trialDB['deviceVectors']
deviceClusterCollection = trialDB['notiDeviceClusters']

newsVectorCollection = trialDB['newsVectors']
newsTSpentCollection = trialDB['newsSpent']
newsTScoreCollection = trialDB['newsScores']
newsNotiSpentCollection = trialDB['newsNotiSpent']
newsNotiScoreCollection = trialDB['newsNotiScores']
deviceNotiClustersCollection = trialDB['notiDeviceClusters']

#clusterCentersCollection = trainDB['clusterCenters']
print(trialDB)
 
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

def processAndTrainDf(df):
    
    newsMap = getNewsInDates(datetime.datetime.now() - datetime.timedelta(days=3), datetime.datetime.now())
    hashIdList = list(newsMap.keys())
        
    hashIdsWithFilter = []
    for h in hashIdList:
        if 'newsLanguage' in newsMap[h] and newsMap[h]['newsLanguage'] == 'english' and newsMap[h]['publishGroupList'][0]['countryCode'] == 'IN':
            hashIdsWithFilter.append(h.split('-')[0])
            

    return train(df)

def allVectorsOkay(vector):
    return np.sum(np.isnan(vector)) == 0 and np.sum(np.isinf(vector)) == 0 and np.linalg.norm(vector) < vector.shape[0] * 10
    
def train(df):
    
    deviceIds = list(df['deviceId'].unique())
    hashIds = list(df['hashId'].unique())
    
    deviceClusters = getDeviceClusters(deviceIds)
    newsTSpentMap = getNewsTSpentData(hashIds)
    newsNotiSpentMap = getNewsNotiSpentData(hashIds)
    
    stream = df.to_numpy()
    
    cnt_fails = 0.
    cnt_newstspent_nan = 0.
    
    count1 = 0
    count2 = 0
    for s in stream:
        deviceId = s[0]
        hashId = s[1]
        event_name = s[2].lower()
        overallTimeSpent = max(min(s[3], 30), 0)
        notiOpened = s[4]
#         print(s)
#         break
        
        
        deviceCluster = deviceClusters[deviceId]
        timespent, t_count = newsTSpentMap[hashId][deviceCluster] 
        notiopened, noti_count = newsNotiSpentMap[hashId][deviceCluster] 
        
        
#         print(deviceId, timespent, notiopened )
        
            
        if event_name == 'timespent-front':
            if np.isnan(timespent) or timespent < 0:
                timespent = 6
                count = 1
                cnt_newstspent_nan += 1
            
            count2 += 1
            newsTSpentMap[hashId][deviceCluster] = (timespent + overallTimeSpent, t_count + 1)
            
        else:
            count1 += 1
            newsNotiSpentMap[hashId][deviceCluster] = (notiopened + notiOpened, noti_count + 1)
        
        
        """logging"""
        if deviceId not in devIdProcessMap:
            devIdProcessMap[deviceId] = {"processed" : 0}
        
        devIdProcessMap[deviceId]["processed"] += 1
        
    if cnt_fails > 0:
        logger.warning("Got nan or inf during training %age " + str(round(cnt_fails / len(stream) * 100, 3)))
    
    if cnt_newstspent_nan > 0:
        logger.warning("Got news timespent nan " + str(round(cnt_newstspent_nan / len(stream) * 100, 3)))
    
    logger.info("processed all " + str(len(stream)) + " entries. " + " Average updates per device " + str(round(len(stream) / len(devIdProcessMap), 3)))
    print("count: ", count1, count2)
    return newsTSpentMap , newsNotiSpentMap

def refreshNewsTSpentMap(newsTSpentMap):
    clusterSet = set(range(NUM_CLUSTERS)) 
    pass




import pandas as pd

devIdProcessMap = {}
for data_df in data_chunks:
    
    data_df = data_df.toPandas()

#     try:
    newsTSpentMap, newsNotiSpentMap = processAndTrainDf(data_df)
    refreshNewsTSpentMap(newsTSpentMap)
    #insertDeviceVectorsInMongo(deviceVectors)
    #insertDeviceClustersInMongo(deviceClusters)
    #insertClusterCentersInMongo(clusterCenters)
    #insertNewsVectorsInMongo(newsVectors)
    insertNewsTSpentInMongo(newsTSpentMap)
    insertNewsTScoreInMongo(newsTSpentMap)
    # insertNewsNotiSpentInMongo(newsNotiSpentMap)
    # insertNewsNotiScoreInMongo(newsNotiSpentMap)

    logger.info("inserted all data in the db, Num Clusters " + str(NUM_CLUSTERS))
    
#     except Exception as e:
#         logger.warning("Error processing data: " + str(e))




Log("Exectution finished successfully, in : %s minutes, %s seconds " % (int((time.time() - start_time) / 60), int((time.time() - start_time) % 60)), flag=True)







