#!/usr/bin/env python
# coding: utf-8

# In[44]:


print('Starting of Notification Classifier training')
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
if not os.path.exists('/home/Gourav/Models/Noti_Classifier/logs/'):
    os.makedirs('/home/Gourav/Models/Noti_Classifier/logs/')


# In[ ]:


import time
start_time = time.time()

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

conf = SparkConf().setAll([('spark.driver.memory', '100g'), ('spark.broadcast.blockSize', '50m'), ("spark.executor.instances", '30')])
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# In[46]:


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



# In[47]:


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


# In[48]:


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


# In[49]:


import logging

logger = logging.getLogger(str(datetime.datetime.today().date()))
hdlr = logging.FileHandler(
    '/home/Gourav/Models/Noti_Classifier/logs/' + str(datetime.datetime.today().date()) + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


def Log(s, flag=True):
    if flag:
        logger.info(s)
        print(s)

Log('----', flag=True)


# In[167]:


# for deployment purpose
train_days = 10
#test_days = 1
embed_days = 30
n_days = train_days + embed_days
k = 0
st_date = datetime.datetime.today() - datetime.timedelta(days=n_days+1)


date_fmt = "%Y/%m/%d"

dates = [(st_date + datetime.timedelta(days=i)) for i in range(n_days+1)]
dates.sort()

dates_str = [date.strftime(date_fmt) for date in dates]
dates_str_embed = dates_str[0:embed_days]
dates_str_train = dates_str[embed_days:embed_days+train_days+1]
#dates_str_test = dates_str[-test_days:]
print("embed data dates : ", dates_str_embed, len(dates_str_embed))
print("train data dates : ",dates_str_train, len(dates_str_train) )
#print("test data dates : ",dates_str_test, len(dates_str_test))
# millisMin = dates[0].timestamp() * 1000
# millisMax = (dates[-1] + datetime.timedelta(days=1)).timestamp() * 1000
print(dates[0], dates[-1])
hashIdsWithFilter, newsMap = getNewsData(dates[0], dates[-1])


# In[168]:

paths = get_path(dates_str, 'timeSpentFrontEvents')

data_comb = sqlContext.read.parquet(*paths)
# paths = get_path(dates_str_train, 'otherEvents')
# data_train = sqlContext.read.parquet(*paths)
data_comb = data_comb.select(data_comb.deviceId, (F.split(data_comb.hashId, '-')[0]).alias('hashId'))
user_noti=sqlContext.read.parquet('gs://pvtrough_asia_south1/noti_class/devices_fulfilling_criteria_2023_10_31')
user_noti = user_noti.filter(user_noti.set_typ.isNotNull())
data_comb2 = data_comb.join(user_noti,['deviceId'],"inner")
#generate datasets

import datetime
def timetimestamp_hour(timestamp):
    timestamp = timestamp / 1000  # Convert milliseconds to seconds
    dt = datetime.datetime.fromtimestamp(timestamp)
    hour = dt.hour
    
    return hour
timetimestamp_hourUDF = F.udf(timetimestamp_hour, IntegerType())

def generate_data(datestr):
#     datestr = [d.strftime(date_fmt) for d in dates]
    paths = get_path(datestr, 'otherEvents')
#     print(sorted(paths), len(paths))
    data = sqlContext.read.parquet(*paths)

    data = filter_app(data, app_name=None)
    data = filter_tenant(data, tenant='en')
    data = data.filter(data.categories.isNotNull())

    from pyspark.sql.functions import explode

    # data = data.filter((data.eventTimestamp > millisMin) & (data.eventTimestamp < millisMax))

    data = data.select(data.deviceId, (F.split(data.hashId, '-')[0]).alias('hashId'), data.overallTimeSpent, data.notificationOpened, data.notificationShown, data.notificationType,data.eventName,data.eventTimestamp,explode(data.categories)).withColumnRenamed("col","category")
    data = data.filter( data.eventName.isin(['Notification Shown','Notification Opened']) )

    data = data.filter(data.hashId.isin(hashIdsWithFilter))
    
    data = data.select('deviceId', 'hashId' ,'overallTimeSpent', 'notificationOpened', 'notificationShown','category','eventTimestamp')            .groupby('deviceId', 'hashId','eventTimestamp','category')            .agg(F.max('overallTimeSpent').alias('timeSpent'), 
                 F.sum('notificationShown').alias('notiShown'),F.sum('notificationOpened').alias('notiOpened'))
    data = data.withColumn("notiShown",F.when(data.notiShown < data.notiOpened, 
                                                     data.notiOpened).otherwise(data.notiShown))
    
    data=data.select('deviceId','hashId','category','notiOpened',timetimestamp_hourUDF('eventTimestamp').alias('hour_day'))
    return data

data_embed = generate_data(dates_str_embed)
data_train = generate_data(dates_str_train)

#data_test = generate_data(dates_str_test)



# this is where we will be training embeddings
#data_comb = data_embed.union(data_train)


# In[169]:



data_embed = data_embed.filter(data_embed.notiOpened>0)    

import torch.nn as nn
from torch.nn.parallel import DataParallel
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, recall_score, precision_score, f1_score
import torch
from transformers import TrainingArguments, Trainer
from transformers import BertTokenizer, BertForSequenceClassification, RobertaTokenizer , RobertaForSequenceClassification
from transformers import EarlyStoppingCallback


model_name = "roberta-base"
num_classes = 17
# model_path = "output/checkpoint-12000"
tokenizer = RobertaTokenizer.from_pretrained(model_name)
model = RobertaForSequenceClassification.from_pretrained(model_name, num_labels=num_classes,output_hidden_states=True)

# Move the model to the device
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)

# Check the number of available GPUs
if torch.cuda.device_count() > 1:
    # Specify which GPUs to use
    device_ids = [0, 1]  # Adjust the GPU IDs based on your system configuration
    model = DataParallel(model, device_ids=device_ids)
    print(isinstance(model, DataParallel))
    
    


def get_embedding(model, tokenizer, text):
    model.eval()

    with torch.no_grad():
        device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
        input_ids = tokenizer.encode(text, add_special_tokens=True, return_tensors='pt').to(device)
        outputs = model(input_ids=input_ids)
        hidden_states = outputs.hidden_states
        last_layer_hidden_states = hidden_states[-1]
        embeddings = torch.mean(last_layer_hidden_states, dim=1).squeeze().cpu().numpy()

    return embeddings



# In[170]:

from pyspark.sql.functions import array_contains, size, col
from pyspark.sql.functions import array, col


from pyspark.sql.functions import array, col

#generate news embeddings based on notification text
embedding_start = time.time()

newsMapProcessed = {}
document_embeddings = {}

for hId in tqdm(newsMap):
    
    h = hId.split('-')[0]
#     print(h)
    if (h not in hashIdsWithFilter) :
        continue
    else:    
        newsMapProcessed[h] = {}
        newsMapProcessed[h]['title'] = newsMap[hId]['title']
        newsMapProcessed[h]['content'] = newsMap[hId]['content']
        newsMapProcessed[h]['features'] = newsMapProcessed[h]['title'] #+ "." + newsMapProcessed[h]['content']
        document_embeddings[h] = get_embedding(model, tokenizer, newsMapProcessed[h]['features'])
        

        

Log("Embedding took %s minutes"%(int((time.time() - embedding_start) / 60)))
# path_embedding = "gs://pvtrough_asia_south1/clustering/News_tilte_embedding"+datetime.datetime(2023,7,19,0,0,0).strftime("%Y_%m_%d")+"/"

# df_em_pandas = pd.DataFrame(document_embeddings).T
# df_em_pandas['hid'] = df_em_pandas.index
# df_em_spark = (sqlContext.createDataFrame(df_em_pandas))
# df_em_spark.write.csv(path_embedding , sep=',', header=True,mode = 'overwrite')
data_as_lists = {key: value.tolist() for key, value in document_embeddings.items()}
schema = StructType([
    StructField("hid", StringType(), True),
    StructField("embedding", ArrayType(DoubleType()), True)
])
rows = [(key, value) for key, value in data_as_lists.items()]
df_news_embedding=sqlContext.createDataFrame(rows, schema=schema)
df_news_embedding = df_news_embedding.where(size(col("embedding")) == 768)






    


# In[ ]:


# path_embedding = "gs://pvtrough_asia_south1/clustering/News_tilte_embedding"+datetime.datetime(2023,7,19,0,0,0).strftime("%Y_%m_%d")+"/"
# from pyspark.sql.functions import array, col
# from pyspark.sql.functions import array_contains, size, col

# df_news_embedding = sqlContext.read.csv(path_embedding, sep=',',
#                          inferSchema=True, header=True)
# df_news_embedding = df_news_embedding.distinct()
# feat_cols = [str(x) for x in range(0,len(df_news_embedding.columns) - 1)]
# df_news_embedding = df_news_embedding.select('hid', array([col(x) for x in feat_cols ]).alias('embedding'))
# #df_news_embedding.take(1)
# df_news_embedding = df_news_embedding.where(size(col("embedding")) == 768)


# In[ ]:


newsMapProcessed = {}
document_embeddings={}
embedding_start = time.time()


for hId in tqdm(newsMap):
    
    h = hId.split('-')[0]
#     print(h)
    if (h not in hashIdsWithFilter) :
        continue
    else:    
        newsMapProcessed[h] = {}
        newsMapProcessed[h]['title'] = newsMap[hId]['title']
        newsMapProcessed[h]['content'] = newsMap[hId]['content']
        newsMapProcessed[h]['features'] = newsMapProcessed[h]['title'] + newsMapProcessed[h]['content']
        document_embeddings[h] = get_embedding(model, tokenizer, newsMapProcessed[h]['features'])


        

        
        

Log("Embedding took %s minutes"%(int((time.time() - embedding_start) / 60)))


# path_embedding = "gs://pvtrough_asia_south1/clustering/News_content_embedding"+datetime.datetime(2023,7,19,0,0,0).strftime("%Y_%m_%d")+"/"
# df_em_pandas = pd.DataFrame(document_embeddings).T
# df_em_pandas['hid'] = df_em_pandas.index
# df_em_spark = (sqlContext.createDataFrame(df_em_pandas))

# df_em_spark.write.csv(path_embedding , sep=',', header=True,mode = 'overwrite')
data_as_lists = {key: value.tolist() for key, value in document_embeddings.items()}
schema = StructType([
    StructField("hid", StringType(), True),
    StructField("embedding", ArrayType(DoubleType()), True)
])
rows = [(key, value) for key, value in data_as_lists.items()]
df_embedding=sqlContext.createDataFrame(rows, schema=schema)
df_embedding = df_embedding.where(size(col("embedding")) == 768)

# In[ ]:


# df_embedding = sqlContext.read.csv(path_embedding, sep=',',
#                          inferSchema=True, header=True)

# from pyspark.sql.functions import array, col
# feat_cols = [str(x) for x in range(0,len(df_embedding.columns) - 1)]
# df_embedding = df_embedding.select('hid', array([col(x) for x in feat_cols ]).alias('embedding'))


# In[ ]:


from pyspark.sql.functions import *

data_filtered = data_comb2
# # calculate idf_weight
N = data_filtered.select('hashId').distinct().count()
users = data_filtered.groupBy('hashId').agg(F.countDistinct('deviceId').alias('users'))
idf_weights = users.withColumn('idf_weight', F.log(N / F.col('users')))

# Multiply embeddings with IDF weights
df_result = df_embedding.join(idf_weights, df_embedding.hid == idf_weights.hashId)
df_result = df_result.withColumn('weighted_embedding', F.expr('transform(embedding, (x, i) -> x * idf_weight)'))


df_result = df_result.select('hid','weighted_embedding')
df_joined = data_filtered.join(df_result, data_filtered.hashId == df_result.hid , 'inner')


# assuming your dataframe is called df
grouped_df = df_joined.groupBy("deviceId").agg(*[
    expr(f"avg(weighted_embedding[{i}]) as avg_{i}") for i in range(768)
])



# assuming your dataframe is called grouped_df
avg_cols = [col(f"avg_{i}") for i in range(768)]
grouped_df = grouped_df.withColumn("topic_avg_embedding", array(*avg_cols))

path_vec = "gs://pvtrough_asia_south1/clustering/MLP_user_embeddings"+datetime.datetime(2023,10,29,0,0,0).strftime("%Y_%m_%d")+"/"
path_vec_exploded = "gs://pvtrough_asia_south1/clustering/MLP_device_vectors_exploded_"+datetime.datetime(2023,10,29,0,0,0).strftime("%Y_%m_%d")+"/"
print(path_vec, path_vec_exploded)

grouped_df.select('deviceId', 'topic_avg_embedding').write.parquet(path=path_vec, mode='overwrite')


# In[178]:


from pyspark.sql.functions import array_contains, size, col
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

#num_clusters = 1000
path_vec = "gs://pvtrough_asia_south1/clustering/MLP_user_embeddings"+datetime.datetime(2023,10,29,0,0,0).strftime("%Y_%m_%d")+"/"
# path_vec = "gs://pvtrough_asia_south1/clustering/device_vectors_roberta_live/"
print(path_vec)
# print(path_vec)
temp = sqlContext.read.parquet(path_vec)
df_devices_embeddings = temp.filter(~array_contains(temp.topic_avg_embedding, float('nan')))

#df_devices_embeddings=temp
df_devices_embeddings = df_devices_embeddings.dropna(how='all')
df_devices_embeddings = df_devices_embeddings.where(size(col("topic_avg_embedding")) == 768)


# # Training

# In[181]:


train_data = data_train.join(df_devices_embeddings,["deviceId"],"inner")
train_data = train_data.join(df_news_embedding,train_data.hashId == df_news_embedding.hid,"inner")
train_data = train_data.select('deviceId', 'hashId', 'topic_avg_embedding', 'embedding','notiOpened','hour_day','category')
train_data = train_data.withColumn("notiOpened",F.when(train_data.notiOpened > 1, 
                                                     1).otherwise(train_data.notiOpened))


path_vec = "gs://pvtrough_asia_south1/clustering/MLP_user_embeddings_train"+datetime.datetime(2023,10,29,0,0,0).strftime("%Y_%m_%d")+"/"

train_data.select('hour_day').write.parquet(path=path_vec, mode='overwrite')
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCols=['hour_day'], outputCols=['time_onehot'], dropLast=False)

df_train1 = encoder.fit(train_data).transform(train_data)

from pyspark.ml.functions import vector_to_array
df_train1 = df_train1.select('*', vector_to_array('time_onehot').alias('col_timehot'))
df_train1=df_train1.select('topic_avg_embedding','embedding','notiOpened','col_timehot','deviceId','hashId')

from pyspark.sql.functions import col, concat

train_data_features = df_train1.withColumn('embedding_features', 
                                            concat(col('topic_avg_embedding'), col('embedding'),col('col_timehot')))\
                                .select('embedding_features', 'notiOpened','deviceId','hashId')


from pyspark.ml.functions import array_to_vector

train_data_features=train_data_features.select(array_to_vector('embedding_features').alias('vec_embedding'),'notiOpened','deviceId','hashId')#.collect()



# In[ ]:


training_start=time.time()
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import when, lit

train_data_not=train_data_features.sample(fraction = 0.1)

check_dfgh=train_data_not
class_counts = check_dfgh.groupBy("notiOpened").count()
total_count = check_dfgh.count()
class_weights = class_counts.withColumn("weight", lit(total_count) / (class_counts["count"] * 2))
df_with_weights = check_dfgh.join(class_weights, "notiOpened", "left")
df_with_weights = df_with_weights.withColumn("weight", when(df_with_weights["weight"].isNull(), 1.0).otherwise(df_with_weights["weight"]))

# class_weights_spark={0: 0.5363765873532117, 1: 7.372552325278177}
# from itertools import chain

# mapping_expr = F.create_map([F.lit(x) for x in chain(*class_weights_spark.items())])

# train_data_not = train_data_not.withColumn("weight", mapping_expr.getItem(F.col("notiOpened")))


lr = LogisticRegression(labelCol="notiOpened", featuresCol="vec_embedding",weightCol="weight")
model = lr.fit(df_with_weights)
Log("Model training took %s minutes"%(int((time.time() - training_start) / 60)))
model.write().overwrite().save("gs://pvtrough_asia_south1/clustering/Noti_model14" + "/model")

# In[ ]:


Log('training done for noti classifier')


# In[ ]:


Log("Model training took %s minutes"%(int((time.time() - start_time) / 60)))


# In[ ]:




