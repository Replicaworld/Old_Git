import requests, json, urllib, os, gensim, redis, datetime, yaml
from pymongo import MongoClient
import pymongo, datetime

""" Configurations """
CONFIG_FILEPATH = open("/root/config.ini").read().split('\n')[0]
with open(CONFIG_FILEPATH, 'r') as ymlfile:
  cfg = yaml.load(ymlfile)

lda_conf = cfg['lda']
data_conf = cfg['data']
spark_conf = cfg['spark']
train_conf = cfg['training']
mongo_conf = cfg['mongo']
s3_conf = cfg['s3']
bow_conf = cfg['bow']
""" Config Loaded """

conn_url = 'mongodb://' + mongo_conf['news']['user'] + ':' + mongo_conf['news']['password'] + '@' + mongo_conf['news']['host'] + ':' + str(mongo_conf['news']['port']) + '/' + mongo_conf['news']['db']
client = MongoClient(conn_url)
db = client[mongo_conf['news']['db']]
newsColl = db.News
newsGroupColl = db.NewsGroup

def getNews():
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).date()
    formatted_yesterday = datetime.datetime.combine(yesterday, datetime.datetime.min.time())
    news_list = [str(t['_id']) for t in list(newsColl.find({'createdAt' : {'$gte' : formatted_yesterday} }).sort('createdAt', pymongo.DESCENDING))]
    return news_list

def getPathContent(path):
    with open(path, 'r') as myfile:
        content=myfile.read().replace('\n', '')
        return content

news = getNews()
bowTups = []

for n in news:
    hash_id = n
    path_bow = os.path.join(data_conf['base_path_bow'], hash_id)
    try:
      if os.path.isfile(path_bow):
          print "BOW available: ", path_bow
          bows = getPathContent(path_bow)
          bowTups.append((hash_id, bows))
      else:
          print "BOW not available :", path_bow
    except Exception, err:
      print "Error in : " , hash_id, " : ", err

def pushToMongo(bowTups):
  hosts = ','.join([str(host) + ':' + str(mongo_conf['bow']['port']) for host in mongo_conf['bow']['hosts']])
  conn_url = 'mongodb://' + mongo_conf['bow']['user'] + ':' + mongo_conf['bow']['password'] + '@' + hosts + '/' + mongo_conf['bow']['db']
  client = MongoClient(conn_url, minPoolSize=600, maxPoolSize=None)
  db = client[mongo_conf['bow']['db']]
  bowColl = db[mongo_conf['bow']['coll']]
  bulk = bowColl.initialize_unordered_bulk_op()
  for i in bowTups:
    hashId = i[0]
    bow = i[1]
    bulk.find({'_id' : hashId}).upsert().update({"$set": {'_id' : hashId , "bow" : bow}})
  bulk.execute()

pushToMongo(bowTups)
