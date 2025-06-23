import requests, json, urllib, os, gensim, redis, datetime, yaml
from pymongo import MongoClient
import pymongo, datetime

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

conn_url = 'mongodb://' + mongo_conf['news']['user'] + ':' + mongo_conf['news']['password'] + '@' + mongo_conf['news']['host'] + ':' + str(mongo_conf['news']['port']) + '/' + mongo_conf['news']['db']
client = MongoClient(conn_url)
db = client[mongo_conf['news']['db']]
newsColl = db.News
newsGroupColl = db.NewsGroup

r = redis.StrictRedis(host='inbox-redis-master-ext-elb001-90832592.us-east-1.elb.amazonaws.com',port=6379,db=3)

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
pipe = r.pipeline()

for n in news:
    hash_id = n
    path_bow = os.path.join(data_conf['base_path_bow'], hash_id)
    try:
      if os.path.isfile(path_bow):
          print "BOW available: ", path_bow
          bows = getPathContent(path_bow)
          pipe.setex("bow:" + hash_id, 172800, str(bows))
      else:
          print "BOW not available :", path_bow
    except Exception, err:
      print "Error in : " , hash_id, " : ", err

pipe.execute()
