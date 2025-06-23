#!/usr/bin/env python
# coding: utf-8

# In[1]:


# %load_ext autoreload 
# %autoreload 2


# In[ ]:





# In[5]:


print('Starting of LDA_CV Clustering Update')
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
if not os.path.exists('/home/karanverma/live_algo_logs/CV/clustering_logs/'):
    os.makedirs('/home/karanverma/live_algo_logs/CV/clustering_logs/')


# In[6]:


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

conf = SparkConf().setAll([('spark.driver.memory', '20g'), ('spark.broadcast.blockSize', '10m'), ("spark.executor.instances", '30')])
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# In[7]:


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


# In[8]:


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


# In[9]:


# cd home/karanverma/


# In[10]:


from Utils import *
def getNewsData(d1, d2):
    newsMap = getNewsInDates(d1, d2)
    hashIdList = list(newsMap.keys())

    hashIdsWithFilter = []
    for h in hashIdList:
        if 'newsLanguage' in newsMap[h] and newsMap[h]['newsLanguage'] == 'english' and newsMap[h]['publishGroupList'][0]['countryCode'] == 'IN':
            hashIdsWithFilter.append(h.split('-')[0])
    
    return hashIdsWithFilter, newsMap


# In[11]:


import logging
import datetime
logger = logging.getLogger(str(datetime.datetime.today().date()))
hdlr = logging.FileHandler(
    '/home/karanverma/live_algo_logs/CV/clustering_logs/' + str(datetime.datetime.today().date()) + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)   
logger.setLevel(logging.DEBUG)


# In[12]:


def Log(s, flag=True):
    if flag:
        logger.info(s)
        print(s)

Log('----', flag=True)


# In[13]:


n_days = 40
st_date = datetime.datetime.now() - datetime.timedelta(days=n_days+1)

date_fmt = "%Y/%m/%d"

dates = [(st_date + datetime.timedelta(days=i)) for i in range(n_days+1)]
dates.sort()

dates_str = [date.strftime(date_fmt) for date in dates]

# millisMin = dates[0].timestamp() * 1000
# millisMax = (dates[-1] + datetime.timedelta(days=1)).timestamp() * 1000

hashIdsWithFilter, newsMap = getNewsData(dates[0], datetime.datetime.now())


# In[14]:


NIS_RAW_DATA_BASE_PATH = "gs://inshorts-segment-raw/data/segment-raw-v5a/"

path = get_raw_path(datetime.datetime.now().strftime("%Y/%m/%d"))
today_data = process_raw_data(path)
today_data = today_data.filter(today_data.hashId.isin(hashIdsWithFilter))


# In[15]:


datestr = [d.strftime(date_fmt) for d in dates]
paths = get_path(datestr, 'timeSpentFrontEvents')

data = sqlContext.read.parquet(*paths)

data = filter_app(data, app_name=None)
data = filter_tenant(data, tenant='en')

# data = data.filter((data.eventTimestamp > millisMin) & (data.eventTimestamp < millisMax))
data = data.select(data.deviceId, data.overallTimeSpent, (F.split(data.hashId, '-')[0]).alias('hashId'))        .groupby('deviceId', 'hashId')         .agg(F.max('overallTimeSpent').alias('overallTimeSpent'))

data = data.filter(data.hashId.isin(hashIdsWithFilter))


# In[16]:


data = data.union(today_data)
data.cache()


# In[17]:


def popularity(data, newsMeanMap):
    Log("Computing news mean")
    newsMeandf = data.groupby(data.hashId).agg({'overallTimeSpent' : 'sum', 'hashId' : 'count'}).toPandas()

    for v in newsMeandf.values:
        newsMeanMap[v[0]] = newsMeanMap[v[0]][0] + v[1],  newsMeanMap[v[0]][1] + v[2]

newsMeanMap = defaultdict(lambda: (7, 1))
popularity(data, newsMeanMap)


# In[19]:


# import re
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


# In[21]:


from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA
import time



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

data_filtered = data.filter(F.udf(lambda hashId, overallTimeSpent: overallTimeSpent > (2.5 * newsMeanMap[hashId][0]/newsMeanMap[hashId][1]), BooleanType())('hashId', 'overallTimeSpent'))
abcset=sqlContext.read.parquet("gs://pvtrough_asia_south1/tf_idf/devices_fulfilling_criteria")
df_temp=abcset.filter(abcset.set_typ == 'lda').select(['deviceid']).distinct()
df_temp=df_temp.withColumnRenamed("deviceid","deviceId")


data_filtered=data_filtered.join(df_temp,["deviceId"],"inner")


# In[22]:


# d1 = data.select('deviceId').distinct().collect()
d2 = data_filtered.select('deviceId').distinct().collect()
d3 = df_temp.select('deviceId').distinct().collect()
d_diff = set(d3) - set(d2)
print("device difference initial : " , len(d_diff))


# In[23]:


from tqdm import tqdm
df_left = pd.DataFrame(columns = ['deviceId', 'cluster_left_out_date'])
left_out = set(d3) - set(d2)
pbar = tqdm(total=len(left_out))
cluster_left_out = (datetime.datetime.now() + datetime.timedelta(days=0)) .strftime("%Y_%m_%d") 
i = 0
for d in left_out:
    df_left.loc[i,'deviceId'] = d['deviceId']
    df_left.loc[i,'cluster_left_out_date'] = cluster_left_out
    i= i+1
    pbar.update(1)
pbar.close()


# In[24]:


dfl = df_left.copy()
print(dfl.head())


# In[25]:



path_leftout = 'gs://pvtrough_asia_south1/clustering/leftout_devices_lda_cv'
df_left = sqlContext.createDataFrame(dfl[['deviceId', 'cluster_left_out_date']])
df_left.coalesce(1).write.partitionBy('cluster_left_out_date').csv(path_leftout , sep=',', header=True,mode = 'append')

    


# In[26]:


training_start = time.time()

data_text = data_filtered.select('deviceId', newsToTokensUdf('hashId', 'overallTimeSpent').alias('tokens')).groupby('deviceId').agg(F.collect_list('tokens').alias('tokensAll'))
data_title = data_text.select('deviceId', F.udf(toTokenCollection, ArrayType(StringType()))('tokensAll').alias('tokensList'))
row = data_filtered.take(1)

cv = CountVectorizer()
cv.setInputCol('tokensList')
cv.setOutputCol("vectors")

model = cv.fit(data_title)

data_vector = model.transform(data_title)

numClusters = 10
lda = LDA(k=numClusters, seed=1, optimizer="online")
lda.setFeaturesCol("vectors")

ldaModel = lda.fit(data_vector)

data_lda = ldaModel.transform(data_vector)

topicDf = data_lda.select('deviceId', 'topicDistribution').toPandas()

labels = {}

for v in tqdm(topicDf.values):
    labels[v[0]] = int(np.argmax(np.array(v[1])))

Log("Training took %s minutes"%(int((time.time() - training_start) / 60)))


# In[28]:


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

mongoClient = getMongoClient()
trialDB = mongoClient['trialDB_CV']
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
        for cluster in c['nOpened']:
            data[int(cluster)] = c['nOpened'][cluster]
        newsNotiSpentMap[c['_id']] = data
        
    return newsNotiSpentMap

def insertNewsNotiSpentInMongo(newsNotiSpentMap):
    for hashId in newsNotiSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsNotiSpentMap[hashId]:
            data[str(cluster)] = newsNotiSpentMap[hashId][cluster]
        NotiSpentData = {"$set" : {"nOpened" : data}}
        newsNotiSpentCollection.update_one(key, NotiSpentData, upsert=True)

def getNewsNotiScoreInMongo(hashIds):
    newsNotiScoreMap = {}
    cursor = newsNotiScoreCollection.find({"_id" : {"$in" : hashIds }})
    
    for c in cursor:
        data = {}
        for cluster in c['nOpened']:
            data[int(cluster)] = c['nOpened'][cluster]
        newsNotiScoreMap[c['_id']] = data
        
    return newsNotiScoreMap

def insertNewsNotiScoreInMongo(newsNotiSpentMap):
    for hashId in newsNotiSpentMap:
        key = {"_id" : hashId}
        data = {}
        for cluster in newsNotiSpentMap[hashId]:
            if newsNotiSpentMap[hashId][cluster][1] > 1:
                data[str(cluster)] = newsNotiSpentMap[hashId][cluster][0] / newsNotiSpentMap[hashId][cluster][1]  
        
        notiScoreData = {"$set" : {"nOpened" : data, "updatedAt" : datetime.datetime.now()}}
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
    
    Log("Updated " + str(round(cntUpdated / len(deviceClusters), 3) * 100) + " device clusters")


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

    logger.info("reset " + str(round(resetCount / len(deviceIds) * 100, 3)) + " % device vectors the db")
    
    return deviceVectorMap
        
def getNewsVectors(hashIds):
    newsVectorMap = getNewsVectorsFromMongo(hashIds)
    resetCount = 0
    for hashId in hashIds:
        """todo get it from mongo"""
        if hashId not in newsVectorMap or not allVectorsOkay(newsVectorMap[hashId]) or len(newsVectorMap[hashId]) != NUM_DIMS:
            newsVectorMap[hashId] = np.random.randn(NUM_DIMS)
            resetCount += 0
        
    logger.info("reset " + str(round(resetCount / len(hashIds) * 100, 3)) + " % news vectors the db")
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


# In[29]:



# from ColabUtils import *

deviceVectors = {}
for v in tqdm(topicDf.values):
    deviceVectors[v[0]] = [float(i) for i in v[1]]
insertDeviceVectorsInMongo(deviceVectors)


# In[30]:


newClusterWiseDevices = defaultdict(lambda: set())
for deviceId, cluster in tqdm(labels.items()):
    newClusterWiseDevices[cluster].add(deviceId)


# In[ ]:





# In[31]:


# from ColabUtils import *
deviceClusterMapExisiting = getDeviceClustersFromMongo([])


# In[2]:





# In[32]:


clusterWiseDevices = defaultdict(lambda: set())
for d in tqdm(deviceClusterMapExisiting):
    clusterWiseDevices[deviceClusterMapExisiting[d]].add(d)


# In[20]:


# [len(newClusterWiseDevices[x]) for x in newClusterWiseDevices]


# In[33]:


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


# In[34]:


updatedDeviceClusters = {}
for cluster in newClusterWiseDevices:
    for deviceId in tqdm(newClusterWiseDevices[cluster]):
        updatedDeviceClusters[deviceId] = clusterMap[cluster]


# In[35]:


insert_start = time.time()
# from ColabUtils import *
insertDeviceClustersInMongo(updatedDeviceClusters, deviceClusterMapExisiting, Log)


# In[36]:


insert_time = (time.time() - insert_start)

Log("Insertion took %s percent of time" % round(100 * insert_time / (time.time() - start_time), 3))


# In[37]:


dataWithClusters = data_filtered.select('hashId', 'overallTimeSpent', F.udf(lambda deviceId: updatedDeviceClusters[deviceId], IntegerType())('deviceId').alias('cluster'))
newsClusterMean = dataWithClusters.groupby('hashId', 'cluster').agg(F.sum('overallTimeSpent'),F.count('hashId'))
newsClusterMeanDf = newsClusterMean.toPandas()


# In[38]:


newsTSpentMap = defaultdict(lambda: defaultdict(lambda: (7, 1)))
for v in tqdm(newsClusterMeanDf.values):
    newsTSpentMap[v[0]][v[1]] = (v[2], v[3])


# In[39]:


Log("Inserting News Score Data")
insert_start = time.time()
insertNewsTSpentInMongo(newsTSpentMap)
insertNewsTScoreInMongo(newsTSpentMap)
insert_time = (time.time() - insert_start)
Log("Inserting News Score took %s percent of time" % round(100 * insert_time / (time.time() - start_time), 3))


# In[40]:


Log("Exectution finished successfully, in : %s minutes, %s seconds " % (int((time.time() - start_time) / 60), int((time.time() - start_time) % 60)), flag=True)


# In[ ]:




