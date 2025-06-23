#Databricks notebook source exported at Mon, 16 May 2016 13:28:44 UTC
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
import time, json, requests
from datetime import datetime
import datetime, os, yaml
from itertools import repeat
from pprint import pprint
""" Configurations """
CONFIG_FILEPATH = open("/root/config.ini").read().split('\n')[0]
with open(CONFIG_FILEPATH, 'r') as ymlfile:
  cfg = yaml.load(ymlfile)

#red_conf = cfg['redis']
lda_conf = cfg['lda']
data_conf = cfg['data']
spark_conf = cfg['spark']
train_conf = cfg['training']
mongo_conf = cfg['mongo']
s3_conf = cfg['s3']
trend_conf = cfg['trending']

# COMMAND ----------
#os.system("export AWS_ACCESS_KEY_ID=AKIAIMBMZVCDDQKHP66A AWS_SECRET_ACCESS_KEY=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 AWS_DEFAULT_REGION=us-east-1")
#end = int(round(time.time()))   #time at present in seconds
#start = int(round(time.time())) - 24*60*60   #time exactly 24 hrs earlier in seconds

def getDataPath(date):
  return "s3n://exports.localytics.n-q/newsinshorts/" + str(date) + "/*/*.log.gz"
date_today = datetime.datetime.today().strftime('%Y/%m/%d')
date_yesterday = (datetime.datetime.strptime(date_today,'%Y/%m/%d') - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
#date_today = str(datetime.now().year) + "/" + str(datetime.now().month).zfill(2) + "/" + str(datetime.now().day).zfill(2)
#date_yesterday = str(datetime.now().year) + "/" + str(datetime.now().month).zfill(2) + "/" + str(datetime.now().day-1).zfill(2)
datapath = ','.join([getDataPath(date_today),getDataPath(date_yesterday)])
print datapath

# COMMAND ----------
#conf = SparkConf().setMaster(spark_conf['master_url']).setAppName(trend_conf['app_name']).setAll([("spark.mesos.coarse",trend_conf['mesos']),("spark.eventLog.enabled",trend_conf['logging']),("spark.cores.max", trend_conf['cores']),("spark.executor.memory",trend_conf['memory'])])
sc = SparkContext(conf = conf)
user_data = sc.textFile(datapath).map(lambda x: json.loads(x)).filter(lambda x: 'name' in x and 'custom_0' in x and 'custom' in x and x['custom_0'] == 'en').map(lambda x : (x['name'],x['custom']))
user_data.cache()

# COMMAND ----------

def extract(x,event):
  return x[0] == event and 'timestamp' in x[1] and ('hashId' in x[1]) and float(x[1]['timestamp']) > start and float(x[1]['timestamp']) < end

# COMMAND ----------

newsViewData = user_data.filter(lambda x : extract(x,'TimeSpent-Front')).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
timeSpentData = user_data.filter(lambda x : extract(x,'TimeSpent-Front') and float(x[1]['timeSpent']) > 8).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
fullTimeSpentData = user_data.filter(lambda x : extract(x,'TimeSpent-Back') and float(x[1]['timeSpent']) > 10).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
videoViewData = user_data.filter(lambda x : extract(x,'Video Play Clicked') and float(x[1]['timeSpent']) > 7).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
newsSharedData = user_data.filter(lambda x : extract(x,'News Shared')).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
newsBookmarkedData = user_data.filter(lambda x : extract(x,'News Bookmarked')).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)

# COMMAND ----------

newsViews = [n for n in sorted(newsViewData.collect(), key=lambda x : -x[1]) if n[1] > 5000]
timeSpentNews = [n for n in sorted(timeSpentData.collect(), key=lambda x : -x[1]) if n[1] > 1000]
fullTimeSpentNews = [n for n in sorted(fullTimeSpentData.collect(), key=lambda x : -x[1]) if n[1] > 50]
videoViewNews = [n for n in sorted(videoViewData.collect(), key=lambda x : -x[1]) if n[1] > 10]
newsShared = [n for n in sorted(newsSharedData.collect(), key=lambda x : -x[1]) if n[1] > 10]
newsBookmarked = [n for n in sorted(newsBookmarkedData.collect(), key=lambda x : -x[1]) if n[1] > 10]

# COMMAND ----------

set([x[0] for x in newsViews]).intersection(set([x[0] for x in newsShared]))

# COMMAND ----------

most_views_dict = {}
for n in newsViews:
  most_views_dict[n[0]] = float(n[1])

# COMMAND ----------

all = [timeSpentNews, fullTimeSpentNews, videoViewNews, newsShared, newsBookmarked]
weights = [0.4, 0.4, 0.4, 0.1, 0.1] # 30% weight to most_read, 40% to full_story/video; 15% to news sharing; 15% to bookmarks


# COMMAND ----------

normalized = [[] for i in repeat(None, len(all))]
scores = {}
for i in range(len(all)):
  results = all[i]
  for x in results:
    hashId = x[0]
    occ = float(x[1])
    if most_views_dict.get(hashId) is not None:
      normalized[i].append((round(occ/most_views_dict[hashId],4), hashId))
  
  normalized[i] = sorted(normalized[i], key = lambda x : -x[0])
  
  if len(normalized[i]) < 1:
    continue
  print "data:", i, " : ", len(normalized[i]), " : ", normalized[i][0][0]
  max_score = normalized[i][0][0]
  factor = 1.0 / max_score
  normalized[i] = [(x[0]*factor, x[1]) for x in normalized[i]]

  for x in normalized[i]:
    score = x[0]
    hashId = x[1]
    if scores.get(hashId) is not None:
      scores[hashId] += score * weights[i]
    else:
      scores[hashId] = score * weights[i]

# sort the final trending list in descending order of trending score
final = sorted(scores.items(), key = lambda x : -x[1])

trending = []
for x in final:
  if x[1] > 0.22:
    trending.append(str(x[0])) 
trending = list(reversed(trending))
trending = [i[:-2] for i in trending] ## for groupid AB testing
pprint(trending)

# COMMAND ----------
with open('/mnt/data/trending.txt','w') as f:
  for n in trending:
    f.write(n+'\n')

# COMMAND ----------


