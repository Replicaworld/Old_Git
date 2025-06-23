#!/usr/bin/env python
# coding: utf-8

# In[158]:
print('Starting of TF_IDF_Low Clustering Update')
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

print(os.getcwd())
if not os.path.exists('/home/Gourav/Live_Algorithms/TF_IDF_Low/Clustering_logs/'):
    os.makedirs('/home/Gourav/Live_Algorithms/TF_IDF_Low/Clustering_logs/')



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
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

NIS_DATA_BASE_PATH = "gs://nis-segment-datasource-v3/processed/"
NIS_OLD_DATA_BASE_PATH = "gs://nis-localytics-datasource/processed/"

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


# In[159]:


import time
start_time = time.time()



# In[160]:


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


# In[161]:


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


# In[162]:


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
def getNewsData(d1, d2):
    newsMap = getNewsInDates(d1, d2)
    hashIdList = list(newsMap.keys())

    hashIdsWithFilter = []
    for h in hashIdList:
        if 'newsLanguage' in newsMap[h] and newsMap[h]['newsLanguage'] == 'english' and newsMap[h]['publishGroupList'][0]['countryCode'] == 'IN':
            hashIdsWithFilter.append(h.split('-')[0])
    
    return hashIdsWithFilter, newsMap


# In[163]:


import logging

logger = logging.getLogger(str(datetime.datetime.today().date()))
hdlr = logging.FileHandler(
    '/home/Gourav/Live_Algorithms/TF_IDF_Low/Clustering_logs/' + str(datetime.datetime.today().date()) + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

def Log(s, flag=True):
    if flag:
        logger.info(s)
        print(s)

Log('----', flag=True)


# In[164]:


#keeping data of 3 days for training
n_days = 40
#st_date = datetime.datetime(2023, 2, 18)
#st_date = datetime.datetime(2023, 5, 18) - datetime.timedelta(days=n_days+1)

st_date = datetime.datetime.now() - datetime.timedelta(days=n_days+1)

date_fmt = "%Y/%m/%d"

dates = [(st_date + datetime.timedelta(days=i)) for i in range(n_days+1)]
dates.sort()
#print(dates)
dates_str = [date.strftime(date_fmt) for date in dates]

# millisMin = dates[0].timestamp() * 1000
# millisMax = (dates[-1] + datetime.timedelta(days=1)).timestamp() * 1000

hashIdsWithFilter, newsMap = getNewsData(dates[0], datetime.datetime.now())


# In[165]:


#need to comment this
NIS_RAW_DATA_BASE_PATH = "gs://inshorts-segment-raw/data/segment-raw-v5a/"
#path = get_raw_path(datetime.datetime(2023, 2, 22).strftime("%Y/%m/%d"))
path = get_raw_path(datetime.datetime.now().strftime("%Y/%m/%d"))
today_data = process_raw_data(path)
today_data = today_data.filter(today_data.hashId.isin(hashIdsWithFilter))


# In[166]:


datestr = [d.strftime(date_fmt) for d in dates]
paths = get_path(datestr, 'timeSpentFrontEvents')

data = sqlContext.read.parquet(*paths)

data = filter_app(data, app_name=None)
data = filter_tenant(data, tenant='en')

# data = data.filter((data.eventTimestamp > millisMin) & (data.eventTimestamp < millisMax))
data = data.select(data.deviceId, data.overallTimeSpent, (F.split(data.hashId, '-')[0]).alias('hashId'))        .groupby('deviceId', 'hashId')         .agg(F.max('overallTimeSpent').alias('overallTimeSpent'))

data = data.filter(data.hashId.isin(hashIdsWithFilter))


# In[167]:


data = data.union(today_data)
data=data.filter(data.overallTimeSpent<100)


# In[168]:


def popularity(data, newsMeanMap):
    Log("Computing news mean")
    newsMeandf = data.groupby(data.hashId).agg({'overallTimeSpent' : 'sum', 'hashId' : 'count'}).toPandas()

    for v in newsMeandf.values:
        newsMeanMap[v[0]] = newsMeanMap[v[0]][0] + v[1],  newsMeanMap[v[0]][1] + v[2]

newsMeanMap = defaultdict(lambda: (7, 1))
popularity(data, newsMeanMap)


# In[169]:


import redis
r_conn1 = redis.StrictRedis('172.16.11.60', port=6379, db=1)

lda_usrs=pd.DataFrame(list(map(lambda x:x.decode("utf-8"),list(r_conn1.smembers("TF_IDF_Low")))),columns=['deviceid'])
df_temp=sqlContext.createDataFrame(lda_usrs)
df_temp=df_temp.withColumnRenamed("deviceid","deviceId")
data_filtered = data.filter(F.udf(lambda hashId, overallTimeSpent: overallTimeSpent > (2.5 * newsMeanMap[hashId][0]/newsMeanMap[hashId][1]), BooleanType())('hashId', 'overallTimeSpent'))
data_filtered=data_filtered.join(df_temp,["deviceId"],"inner")


# #set_users= df_temp.toPandas().deviceid.values.tolist()
# #data_filtered=data_filtered.filter(data_filtered.deviceId.isin(set_users) == True)
# d1 = data.select('deviceId').distinct().collect()
# d2 = data_filtered.select('deviceId').distinct().collect()
# d3 = df_temp.select('deviceId').distinct().collect()

# left_out = set(d3) - set(d2)
# s = left_out
# list_s=list(s)
# df_left = pd.DataFrame(list(s))
# df_left.columns = ['deviceId']

# #path_leftout = 'gs://pvtrough_asia_south1/clustering/leftout_devices_tfidf'
# #df_left_s = sqlContext.createDataFrame(df_left)
# #df_left_s.write.csv(path_leftout , sep=',', header=True,mode = 'overwrite')

    

# In[170]:


import string
import re
import nltk
from nltk.stem.porter import PorterStemmer
from nltk.stem import WordNetLemmatizer

nltk.download('omw-1.4')
nltk.download('stopwords')
nltk.download('wordnet')
stopwords = nltk.corpus.stopwords.words('english')

#defining the object for stemming
porter_stemmer = PorterStemmer()

#defining the object for Lemmatization
wordnet_lemmatizer = WordNetLemmatizer()

#defining the function for lemmatization
def lemmatizer(text):
    lemm_text = [wordnet_lemmatizer.lemmatize(word) for word in text]
    return lemm_text

#defining a function for stemming
def stemming(text):
    stem_text = [porter_stemmer.stem(word) for word in text]
    return stem_text

def remove_stopwords(text):
    output= [i for i in text if i not in stopwords]
    return output

def tokenization(text):
    tokens = text.split()
    return tokens

def remove_punctuation(text):
    punctuationfree="".join([i for i in text if i not in string.punctuation])
    return punctuationfree

def preprocess(text):
    text = remove_punctuation(text)

    text = text.lower()

    tokens = tokenization(text)
    tokens = remove_stopwords(tokens)
    tokens = stemming(tokens)
    tokens = lemmatizer(tokens)
    
    return tokens

newsMapProcessed = {}
for hId in tqdm(newsMap):
    h = hId.split('-')[0]
    newsMapProcessed[h] = {}
    newsMapProcessed[h]['title'] = preprocess(newsMap[hId]['title'])
    newsMapProcessed[h]['content'] = preprocess(newsMap[hId]['content'])
    
    newsMapProcessed[h]['features'] = newsMapProcessed[h]['title'] + newsMapProcessed[h]['content']


# In[ ]:


from pyspark.ml.feature import CountVectorizer , IDF
from pyspark.ml.clustering import LDA
import time



training_start = time.time()

def newsToTokens(hashId, overallTimeSpent):
    tokens = newsMapProcessed[hashId]['features']
    #count = int(min(100, overallTimeSpent)/8) + 1
    return tokens

def toTokenCollection(tokensList):
    arr = []
    for tokens in tokensList:
        for token in tokens:
            arr.append(token)
    
    return arr

newsToTokensUdf = F.udf(newsToTokens, ArrayType(StringType()))

#data_filtered = data.filter(F.udf(lambda hashId, overallTimeSpent: overallTimeSpent > (2.5 * newsMeanMap[hashId][0]/newsMeanMap[hashId][1]), BooleanType())('hashId', 'overallTimeSpent'))
data_text = data_filtered.select('deviceId', newsToTokensUdf('hashId', 'overallTimeSpent').alias('tokens')).groupby('deviceId').agg(F.collect_list('tokens').alias('tokensAll'))
data_title = data_text.select('deviceId', F.udf(toTokenCollection, ArrayType(StringType()))('tokensAll').alias('tokensList'))
row = data_filtered.take(1)

#print(row)
cv = CountVectorizer()
cv.setInputCol('tokensList')
cv.setOutputCol("raw_features")

model = cv.fit(data_title)
#result_tfidf = model.transform(data_title)
data_vector = model.transform(data_title)
#data_vector=result_tfidf.select(['deviceId', 'Features'])


idf = IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(data_vector)
result_tfidf = idfModel.transform(data_vector) 

#result_tfidf.write.parquet("gs://pvtrough_asia_south1/clustering/TF_IDF_100/result_tfidf"+sampled_dates[k].strftime("%Y_%m_%d")+"/")

numClusters = 10
lda = LDA(k=numClusters, seed=1, optimizer="online")
lda.setFeaturesCol("features")

ldaModel = lda.fit(result_tfidf)

data_lda = ldaModel.transform(result_tfidf)

topicDf = data_lda.select('deviceId', 'topicDistribution').toPandas()

labels = {}

for v in tqdm(topicDf.values):
    labels[v[0]] = int(np.argmax(np.array(v[1])))

Log("Training took %s minutes"%(int((time.time() - training_start) / 60)))


# In[ ]:


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
#from Utils import *
from tqdm import tqdm

def getMongoClient():
    hosts = ['171.16.11.97','171.16.11.96','171.16.11.94']
    host = ','.join([i + ":27017" for i in hosts])
    conn_url = 'mongodb://root:superman@' + host
    client = MongoClient(conn_url)
    return client
trial_algo_suffix = 'TF_IDF_Low'

mongoClient = getMongoClient()
trialDB = mongoClient['trialDB_'+trial_algo_suffix]
deviceVectorCollection = trialDB['deviceVectors']
deviceClusterCollection = trialDB['notiDeviceClusters']

newsVectorCollection = trialDB['newsVectors']
newsTSpentCollection = trialDB['newsTSpent']
newsScoreCollection = trialDB['newsScores']
deviceNotiClustersCollection = trialDB['notiDeviceClusters']

#clusterCentersCollection = trainDB['clusterCenters']
 
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

def getNewsScoreInMongo(hashIds):
    newsScoreMap = {}
    cursor = newsScoreCollection.find({"_id" : {"$in" : hashIds }})
    
    for c in cursor:
        data = {}
        for cluster in c['tSpent']:
            data[int(cluster)] = c['tSpent'][cluster]
        newsScoreMap[c['_id']] = data
        
    return newsScoreMap

def insertNewsScoreInMongo(newsTSpentMap):
    for hashId in newsTSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsTSpentMap[hashId]:
            if newsTSpentMap[hashId][cluster][1] > 1:
                data[str(cluster)] = newsTSpentMap[hashId][cluster][0] / newsTSpentMap[hashId][cluster][1]  
        
        scoreData = {"$set" : {"tSpent" : data, "updatedAt" : datetime.datetime.now()}}
        newsScoreCollection.update_one(key, scoreData, upsert=True)

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
    
    Log("Updated " + str(round(cntUpdated / len(deviceClusters), 3) * 100) + " device clusters")


# In[ ]:


#from Colab_TF import *

deviceVectors = {}
for v in tqdm(topicDf.values):
    deviceVectors[v[0]] = [float(i) for i in v[1]]
insertDeviceVectorsInMongo(deviceVectors)

#df_left['cluster_number']=10
#labels_unknown = dfdump.select('deviceId','cluster').toPandas()
#labels_unknown = pd.Series(df_left.cluster_number.values,index=df_left.deviceId).to_dict()
#labels.update(labels_unknown)


# In[ ]:


newClusterWiseDevices = defaultdict(lambda: set())
for deviceId, cluster in tqdm(labels.items()):
    newClusterWiseDevices[cluster].add(deviceId)


# In[ ]:


deviceClusterMapExisiting = getDeviceClustersFromMongo([])


# In[ ]:


clusterWiseDevices = defaultdict(lambda: set())
for d in tqdm(deviceClusterMapExisiting):
    clusterWiseDevices[deviceClusterMapExisiting[d]].add(d)


# In[ ]:


marked = {}
clusterMap = {}
for x in newClusterWiseDevices:
    mxinter = -1
    mxidx = -1
    for y in clusterWiseDevices:
        if y in marked:
            continue
        inter = round(len(clusterWiseDevices[y].intersection(newClusterWiseDevices[x])) / len(clusterWiseDevices[y])*100, 3)
        if inter > mxinter:
            mxinter = inter
            mxidx = y
            
    if mxidx < 0:
        mxidx = x
    
    marked[mxidx] = 1
    clusterMap[x] = mxidx
    Log("Mapping cluster " + str(x) + " to " + str(mxidx) + ", %age intersection = " + str(mxinter))


# In[ ]:


updatedDeviceClusters = {}
for cluster in newClusterWiseDevices:
    for deviceId in tqdm(newClusterWiseDevices[cluster]):
        updatedDeviceClusters[deviceId] = clusterMap[cluster]


# In[ ]:


insert_start = time.time()
insertDeviceClustersInMongo(updatedDeviceClusters, deviceClusterMapExisiting, Log)
insert_time = (time.time() - insert_start)

Log("Insertion took %s percent of time" % round(100 * insert_time / (time.time() - start_time), 3))


# In[ ]:


#this one is different need to run this scoring thing for 30 minutes for computing scores, need to call updated device clusters from clustering part 
# and also defining log for the time, and newmeanmap for filtering and data reading from json 
dataWithClusters = data_filtered.select('hashId', 'overallTimeSpent', F.udf(lambda deviceId: updatedDeviceClusters[deviceId], IntegerType())('deviceId').alias('cluster'))
newsClusterMean = dataWithClusters.groupby('hashId', 'cluster').agg(F.sum('overallTimeSpent'),F.count('hashId'))
newsClusterMeanDf = newsClusterMean.toPandas()


# In[ ]:


newsTSpentMap = defaultdict(lambda: defaultdict(lambda: (7, 1)))
for v in tqdm(newsClusterMeanDf.values):
    newsTSpentMap[v[0]][v[1]] = (v[2], v[3])


# In[ ]:


Log("Inserting News Score Data")
insert_start = time.time()
insertNewsTSpentInMongo(newsTSpentMap)
insertNewsScoreInMongo(newsTSpentMap)
insert_time = (time.time() - insert_start)
Log("Inserting News Score took %s percent of time" % round(100 * insert_time / (time.time() - start_time), 3))


# In[ ]:


Log("Exectution finished successfully, in : %s minutes, %s seconds " % (int((time.time() - start_time) / 60), int((time.time() - start_time) % 60)), flag=True)


# In[ ]:




