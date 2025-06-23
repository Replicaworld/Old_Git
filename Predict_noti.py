#!/usr/bin/env python
# coding: utf-8

# In[410]:


print('Starting of Notification Classifier predictor')
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
if not os.path.exists('/home/Gourav/Models/Noti_Classifier/output_logs/'):
    os.makedirs('/home/Gourav/Models/Noti_Classifier/output_logs/')


# In[411]:


start_time = time.time()


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


# In[ ]:





# In[412]:


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
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pymongo import MongoClient
from pymongo import UpdateOne
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
from pyspark.sql.functions import array, col

from pyspark.sql.functions import *
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col, concat
from pyspark.ml.functions import array_to_vector

from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql.functions import array, col
from pyspark.ml.functions import vector_to_array


# In[413]:


#reading batchwise data to dump scores for the notification for news


# In[414]:


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



# In[415]:


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


# In[416]:


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


# In[ ]:





# In[417]:


import logging

logger = logging.getLogger(str(datetime.datetime.today().date()))
hdlr = logging.FileHandler(
    '/home/Gourav/Models/Noti_Classifier/output_logs/' + str(datetime.datetime.today().date()) + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


def Log(s, flag=True):
    if flag:
        logger.info(s)
        print(s)

Log('----', flag=True)


# In[418]:


from datetime import datetime, timedelta
n_days=7
st_date = datetime.today() - timedelta(days=n_days+1)
date_fmt = "%Y/%m/%d"

dates = [(st_date + timedelta(days=i)) for i in range(n_days+1)]
dates.sort()

dates_str = [date.strftime(date_fmt) for date in dates]
paths = get_path(dates_str, 'otherEvents')

data = sqlContext.read.parquet(*paths)
hashIdsWithFilter,newsMap=getNewsData(datetime.now() - timedelta(minutes=60), datetime.now())

#result_df = data.groupBy("deviceId").agg(countDistinct("hashId").alias("hashIdCount"))
#filtered_df = result_df.filter(result_df["hashIdCount"] > 10)


# In[419]:


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


# In[420]:


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
#path_embedding = "gs://pvtrough_asia_south1/clustering/News_tilte_embedding_test"+datetime(2023,7,19,0,0,0).strftime("%Y_%m_%d")+"/"

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

from pyspark.sql.functions import array, col
from pyspark.sql.functions import array, col
from pyspark.sql.functions import array_contains, size, col




# In[421]:

user_noti=sqlContext.read.parquet('gs://pvtrough_asia_south1/noti_class/devices_fulfilling_criteria_2023_10_31')
user_noti = user_noti.filter(user_noti.set_typ.isNotNull())
device_data=user_noti.select("deviceId").distinct()
unique_hash_ids=list(document_embeddings.keys())
list_df_hash = sqlContext.createDataFrame([(item,) for item in unique_hash_ids], ["hashId"])

data_test = device_data.crossJoin(list_df_hash)
data_test = data_test.withColumn("hour_day", lit(datetime.now().hour-1))


# In[ ]:





# In[422]:


# path_embedding = "gs://pvtrough_asia_south1/clustering/News_tilte_embedding_test"+datetime(2023,7,19,0,0,0).strftime("%Y_%m_%d")+"/"
# from pyspark.sql.functions import array, col
# from pyspark.sql.functions import array_contains, size, col

# df_news_embedding = sqlContext.read.csv(path_embedding, sep=',',
#                          inferSchema=True, header=True)
# df_news_embedding = df_news_embedding.distinct()
# feat_cols = [str(x) for x in range(0,len(df_news_embedding.columns) - 1)]
# df_news_embedding = df_news_embedding.select('hid', array([col(x) for x in feat_cols ]).alias('embedding'))
# #df_news_embedding.take(1)
# df_news_embedding = df_news_embedding.where(size(col("embedding")) == 768)


# In[423]:


path_vec = "gs://pvtrough_asia_south1/clustering/MLP_user_embeddings"+datetime(2023,10,28,0,0,0).strftime("%Y_%m_%d")+"/"
temp = sqlContext.read.parquet(path_vec)
df_devices_embeddings = temp.filter(~array_contains(temp.topic_avg_embedding, float('nan')))


df_devices_embeddings = df_devices_embeddings.dropna(how='all')
df_devices_embeddings = df_devices_embeddings.where(size(col("topic_avg_embedding")) == 768)


# In[424]:



test_data = data_test.join(df_devices_embeddings,["deviceId"],"inner")
test_data = test_data.join(df_news_embedding,test_data.hashId == df_news_embedding.hid,"inner")
test_data = test_data.select('deviceId', 'hashId', 'topic_avg_embedding', 'embedding','hour_day')
# test_data = test_data.withColumn("notiOpened",F.when(test_data.notiOpened > 1, 
#                                                      1).otherwise(test_data.notiOpened))
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCols=['hour_day'], outputCols=['time_onehot'], dropLast=False)

path_vec = "gs://pvtrough_asia_south1/clustering/MLP_user_embeddings_train"+datetime(2023,10,28,0,0,0).strftime("%Y_%m_%d")+"/"

temp_train = sqlContext.read.parquet(path_vec)
df_test1 = encoder.fit(temp_train).transform(test_data)
from pyspark.ml.functions import vector_to_array
df_test1 = df_test1.select('*', vector_to_array('time_onehot').alias('col_timehot'))
df_test1=df_test1.select('topic_avg_embedding','embedding','col_timehot','deviceId','hashId')


from pyspark.sql.functions import col, concat

test_data_features = df_test1.withColumn('embedding_features', 
                                            concat(col('topic_avg_embedding'), col('embedding'),col('col_timehot')))\
                                .select('embedding_features','deviceId','hashId')
                                    

    
from pyspark.ml.functions import array_to_vector

test_data_features=test_data_features.select(array_to_vector('embedding_features').alias('vec_embedding'),'deviceId','hashId')#.collect()


from pyspark.ml.classification import LogisticRegressionModel

model = LogisticRegressionModel.load("gs://pvtrough_asia_south1/clustering/Noti_model13" + "/model")
    
predictions = model.transform(test_data_features)
predictions_temp=predictions.select(['probability','prediction','deviceId','hashId'])
#predictions_check = predictions_temp.join(test_data,["deviceId","hashId"],"inner")
#predictions_final=predictions_check.select(['deviceId','hashId','hour_day','notiOpened','probability','prediction'])
predictions_final = predictions_temp.select('*', vector_to_array('probability').alias('arr_prob'))
gfcv=predictions_final.select('deviceId','hashId','arr_prob',F.udf(lambda x:x[0],FloatType())('arr_prob').alias('prob_zero'))


#     ranked =  gfcv.withColumn(
#       "rank", dense_rank().over(Window.partitionBy("deviceId").orderBy(desc("prob_zero"))))

rank_noti_user=gfcv.select('deviceId','hashId','prob_zero')



#hgjdj=rank_noti_user.collect()
    


# In[425]:


scores_as_tuples = rank_noti_user.rdd.map(tuple).collect()


# In[426]:


NotiscoreMap = defaultdict(lambda: defaultdict(lambda: (7, 1)))

for my_tuple in scores_as_tuples:
    NotiscoreMap[my_tuple[0]][my_tuple[1]] = (my_tuple[2])

    


# In[427]:


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

mongoClient = getMongoClient()
trainDB = mongoClient['trainDB']
NoticlassScore = trainDB['NoticlassScore']


# In[428]:


# def insertNotiscoreinMongo(NotiscoreMap):
#     for deviceid in NotiscoreMap:
#         key = {"_id" : deviceid}
#         score_data = {}
#         for hashid in NotiscoreMap[deviceid]:
            
#             score_data[hashid] = NotiscoreMap[deviceid][hashid]
        
#         scoreData = {"$set" : {"score" : score_data, "updatedAt" : datetime.datetime.now()}}
#         NoticlassScore.update_one(key, scoreData, upsert=True)



def insertNotiscoreinMongo(NotiscoreMap):
    opgh=[]
    for deviceid in NotiscoreMap:
        key = {"_id" : deviceid}
        score_data = {}
        for hashid in NotiscoreMap[deviceid]:
            
            score_data[hashid] = NotiscoreMap[deviceid][hashid]
        
        scoreData = {"$set" : {f'score.{key}': value for key, value in score_data.items()}, '$currentDate' : {'updatedAt': True}}
        #NoticlassScore.update_one(key, scoreData)
        opgh.append(UpdateOne(key, scoreData, upsert=True))
    NoticlassScore.bulk_write(opgh,ordered=False)


# In[ ]:


Log("Inserting Noti Score Data")
insert_start = time.time()
insertNotiscoreinMongo(NotiscoreMap)
Log("Inserting Noti Score took %s minutes"%(int((time.time() - insert_start) / 60)))


# In[ ]:


Log('dumping of noti_score done')


# In[ ]:


Log("Model prediction took took %s minutes"%(int((time.time() - start_time) / 60)))


# In[ ]:




