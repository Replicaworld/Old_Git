#Databricks notebook source exported at Mon, 16 May 2016 13:29:16 UTC
#from pyspark.sql import SQLContext
import time, json, requests, math, base64
from datetime import datetime
from itertools import repeat
from pprint import pprint
import urllib, httplib

# COMMAND ----------

## Send Mail Module ##
def sendMailList(title,text,l):
  for rec in l:
    sendMail(title,text,rec)
    
def sendMail(title,text,rec):
  import smtplib
  import mimetypes
  from email.mime.multipart import MIMEMultipart
  from email import encoders
  from email.message import Message
  from email.mime.audio import MIMEAudio
  from email.mime.base import MIMEBase
  from email.mime.image import MIMEImage
  from email.mime.text import MIMEText

  emailfrom = "reportbot@inshorts.com"
  emailto = rec#,kamal@inshorts.com,azhar@inshorts.com"
  fileToSend = "hi naveen"
  username = "reportbot@inshorts.com"
  password = "pC~%z3rACD6vt*zGX"

  msg = MIMEText(text)
  msg["From"] = emailfrom
  msg["To"] = emailto
  msg["Subject"] = title
  msg.preamble = "Trending Stories"
  
  server = smtplib.SMTP("smtp.gmail.com:587")
  server.starttls()
  server.login(username,password)
  server.sendmail(emailfrom, emailto, msg.as_string())
  server.quit()
  print "done"

# COMMAND ----------

def getTimes(type):
  if type == 'MINUTES30':
    t = 0.5
  elif type == 'HOUR1':
    t = 1
  elif type == "HOURS3":
    t = 3
  elif type == "HOURS6":
    t = 6
  elif type == "HOURS12":
    t = 12
  elif type == "DAY1":
    t = 24
  return (int(round(time.time())) - t*60*60, int(round(time.time())), t)

def getDataPath(date):
  return "s3n://AKIAIMBMZVCDDQKHP66A:JP7FzIkcMzKeOP7D2FB2e42fvncl7%2FyUIYQSVoNY@exports.localytics.n-q/newsinshorts/" + str(date) + "/*/*.log.gz"

def fetchHashIdSince(start_time):
  url = "https://read-api.newsinshorts.com/en/v1/news?max_limit=200"
  r = requests.get(url)
  r = json.loads(r.text)["news_list"]
  hashids = [x["hash_id"] for x in r if x["created_at"] >= start_time]
  hashTitleMap = {x["hash_id"]:x["title"] for x in r if x["created_at"] >= start_time}
  hashOldHashMap = {x["hash_id"]:x["old_hash_id"] for x in r if x["created_at"] >= start_time}
  return hashids, hashTitleMap, hashOldHashMap

def fetchDataLocalytics(conds,metric,dimsns):
  headers = {"Accept" : "application/vnd.localytics.v1+hal+json","Authorization":"Basic " + base64.b64encode("19540d885ff8f7799629fcb-f34f0a66-3b9d-11e5-b420-0013a62af900:f0c77015b109b036f1cc158-f34f0d6c-3b9d-11e5-b420-0013a62af900")}
  url = "https://api.localytics.com/v1/query?" + "app_id=b87254e1c6aeec24d1b2c0b-b0aafd2c-b839-11e4-a93d-005cf8cbabd8&"+"metrics=" + metric +"&" + "dimensions=" + dimsns +"&" + "conditions=" + urllib.quote_plus(conds)
  r = requests.get(url,headers = headers)
  res = json.loads(r.text)
  res = res["results"]
  return res

def getDataLocalytics(conds,metric,dimsns):
  res = fetchDataLocalytics(conds,metric,dimsns)
  if 'avg(a:timeSpent)' in metric:
    tuples = [(x["a:hashId"], x["occurrences"], x['avg(a:timeSpent)']) for x in res]
  else:
    tuples = [(x["a:hashId"], x["occurrences"]) for x in res]
  return tuples
  

def makeNewsRequest(_list):
  url = "https://write-api.newsinshorts.com/en/v2/news/search"
  headers = {'X-AUTH-TOKEN' : 'mBJ8SZExJthGJFRMmCvMSA==', 'X-REGION-ID' : 'IN' ,'Content-Type':'application/json', 'Authorization': 'h6g2eG3lnIN2o73HTHjiDg=='}
  payload = json.dumps({
      "hash_id_list": list(set(_list))
  })
  r = requests.post(url, headers=headers, data=payload)
  return r

def makeNewsCatMap(hashIdList):
  newsCatMap = {}
  hashIdList = list(set(hashIdList))
  BATCH_SIZE = 2000
  NUM_BATCHES = int(math.ceil(len(hashIdList)/float(BATCH_SIZE)))
  new_l = [hashIdList[i*BATCH_SIZE:(i+1)*BATCH_SIZE] for i in range(NUM_BATCHES)]
  for _list in new_l:
    r = makeNewsRequest(_list)
    for news in json.loads(r.text):
      newsCatMap[str(news["hash_id"])] = list(news["categories"])
  return newsCatMap

def makeInvMap(newsCatMap):
  invMap = {}
  for k in newsCatMap:
    categories = newsCatMap[k]
    for cat in categories:
      if invMap.get(cat) is None:
        invMap[cat] = []
      invMap[cat].append(k)
  return invMap

def makeListForIndex(newsCatMap,l):
  reqMap = {}
  for n in l:
    hashId = n[1]
    cats = newsCatMap.get(hashId)
    if cats is not None:
      #pprint("hashId = " + str(hashId) + " , cats = " + str(cats) + " n = " + str(n))
      for cat in list(cats):
        if reqMap.get(cat) is None:
          reqMap[cat] = []
        reqMap[cat].append(n)
  return reqMap


# COMMAND ----------

date_today = str(datetime.now().year) + "/" + str(datetime.now().month).zfill(2) + "/" + str(datetime.now().day).zfill(2)
date_yesterday = str(datetime.now().year) + "/" + str(datetime.now().month).zfill(2) + "/" + str(datetime.now().day-1).zfill(2)
datapath = ','.join([getDataPath(date_today),getDataPath(date_yesterday)])

# COMMAND ----------

#user_data = sc.textFile(datapath).map(lambda x: json.loads(x)).filter(lambda x: 'name' in x and 'custom_0' in x and 'custom' in x and x['custom_0'] == 'en').map(lambda x : (x['name'],x['custom']))
#user_data.cache()

# COMMAND ----------

def extract(x,event,start,end):
  return x[0] == event and 'timestamp' in x[1] and ('hashId' in x[1]) and float(x[1]['timestamp']) > start and float(x[1]['timestamp']) < end

# COMMAND ----------

def dumpStoriesFor(duration):
  start,end,t = getTimes(duration)
  LATEST_NEWS, hashTitleMap, hashOldHashMap = fetchHashIdSince((int(round(time.time())) - int(t*60*60))*1000)
  EKDUM_LATEST_NEWS = fetchHashIdSince((int(round(time.time())) - int(2*60*60))*1000)[0]
  #newsData = user_data.filter(lambda x : extract(x,'TimeSpent-Front',start,end))

  newsViewData = getDataLocalytics("{\"tenant\":\"en\",\"event_name\":\"TimeSpent-Front\",\"n:timestamp\":[\"between\"," + str(start) + "," + str(end) + "],\"occurrences\":[\">\",500],\"n:timeSpent\":[\"between\",0,300]}","occurrences,avg(a:timeSpent)","a:hashId")#newsData.map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
  timeSpentData = getDataLocalytics("{\"tenant\":\"en\",\"event_name\":\"TimeSpent-Front\",\"n:timestamp\":[\"between\"," + str(start) + "," + str(end) + "],\"n:timeSpent\":[\">\",4]}","occurrences","a:hashId")#user_data.filter(lambda x : extract(x,'TimeSpent-Front',start,end) and float(x[1]['timeSpent']) > 7).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
  fullTimeSpentData = getDataLocalytics("{\"tenant\":\"en\",\"event_name\":\"TimeSpent-Back\",\"n:timestamp\":[\"between\"," + str(start) + "," + str(end) + "],\"n:timeSpent\":[\">\",8]}","occurrences","a:hashId")#user_data.filter(lambda x : extract(x,'TimeSpent-Back',start,end) and float(x[1]['timeSpent']) > 10).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
  videoViewData = getDataLocalytics("{\"tenant\":\"en\",\"event_name\":\"Video Play Clicked\",\"n:timestamp\":[\"between\"," + str(start) + "," + str(end) + "],\"n:timeSpent\":[\">\",7]}","occurrences","a:hashId")#user_data.filter(lambda x : extract(x,'Video Play Clicked',start,end) and float(x[1]['timeSpent']) > 7).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
  newsSharedData = getDataLocalytics("{\"tenant\":\"en\",\"event_name\":\"News Shared\",\"n:timestamp\":[\"between\"," + str(start) + "," + str(end) + "]}","occurrences","a:hashId")#user_data.filter(lambda x : extract(x,'News Shared',start,end)).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
  newsBookmarkedData = getDataLocalytics("{\"tenant\":\"en\",\"event_name\":\"News Bookmarked\",\"n:timestamp\":[\"between\"," + str(start) + "," + str(end) + "]}","occurrences","a:hashId")#user_data.filter(lambda x : extract(x,'News Bookmarked',start,end)).map(lambda x : (x[1]['hashId'],1)).reduceByKey(lambda x,y : x+y)
  
  hashIdList = [n[0] for n in newsViewData]#newsData.map(lambda x : x[1]['hashId']).collect()
  newsCatMap = makeNewsCatMap(hashIdList)
  catNewsList = makeInvMap(newsCatMap)
  
  newsViews = [n for n in sorted(newsViewData, key=lambda x : -x[1]) if n[1] > 500 and n[0] in LATEST_NEWS]
  timeSpentNews = [n for n in sorted(timeSpentData, key=lambda x : -x[1]) if n[1] > 70 and n[0] in LATEST_NEWS]
  fullTimeSpentNews = [n for n in sorted(fullTimeSpentData, key=lambda x : -x[1]) if n[1] > 10 and n[0] in LATEST_NEWS]
  videoViewNews = [n for n in sorted(videoViewData, key=lambda x : -x[1]) if n[1] > 5 and n[0] in LATEST_NEWS]
  newsShared = [n for n in sorted(newsSharedData, key=lambda x : -x[1]) if n[1] > 5 and n[0] in LATEST_NEWS]
  newsBookmarked = [n for n in sorted(newsBookmarkedData, key=lambda x : -x[1]) if n[1] > 5 and n[0] in LATEST_NEWS]

  ## maps ##
  newsAvgTimeMap = {n[0]:n[2] for n in newsViewData}
  newsViewsMap = {n[0]:n[1] for n in newsViewData}
  newsShareMap = {n[0]:n[1] for n in newsSharedData}

  most_views_dict = {n[0] : float(n[1]) for n in newsViews}
  all_data = [timeSpentNews, fullTimeSpentNews, videoViewNews, newsShared]#, newsBookmarked]
  weights = [0.6, 0.2, 0.2, 0.2]#, 0.1] # 30% weight to most_read, 40% to full_story/video; 15% to news sharing; 15% to bookmarks
  all_tags = ['timeSpentNews', 'fullTimeSpentNews', 'videoViewNews', 'newsShared']#, 'newsBookmarked']

  normalized = [[] for i in repeat(None, len(all_data))]
  scores = {}
  for i in range(len(all_data)):
    results = all_data[i]
    for x in results:
      hashId = x[0]
      occ = float(x[1])
      if most_views_dict.get(hashId) is not None:
        normalized[i].append((round(occ/most_views_dict[hashId],4), hashId))

    normalized[i] = sorted(normalized[i], key = lambda x : -x[0])
    if len(normalized[i]) < 1:
      continue
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
    trending.append((x[1],x[0]))

  catWiseMostReadDist = makeListForIndex(newsCatMap, normalized[0])
  catWiseMostSharedDist = makeListForIndex(newsCatMap, normalized[3])
  catWiseTrendingDist = makeListForIndex(newsCatMap, trending)

  MOST_SHARED = [{"category":k, "news_list": [{"rank" : i+1, "hash_id" : catWiseMostSharedDist[k][i][1], "views":newsViewsMap.get(catWiseMostSharedDist[k][i][1]), "avg_time_spent":newsAvgTimeMap.get(catWiseMostSharedDist[k][i][1]), "shares":newsShareMap.get(catWiseMostSharedDist[k][i][1])}  for i in range(len(catWiseMostSharedDist[k]))]} for k in catWiseMostSharedDist]
  MOST_READ = [{"category":k, "news_list": [{"rank" : i+1, "hash_id" : catWiseMostReadDist[k][i][1], "views": newsViewsMap.get(catWiseMostReadDist[k][i][1]), "avg_time_spent": newsAvgTimeMap.get(catWiseMostReadDist[k][i][1]), "shares": newsShareMap.get(catWiseMostReadDist[k][i][1])}  for i in range(len(catWiseMostReadDist[k]))]} for k in catWiseMostReadDist]
  TRENDING = [{"category":k, "news_list": [{"rank" : i+1, "hash_id" : catWiseTrendingDist[k][i][1], "views":newsViewsMap.get(catWiseTrendingDist[k][i][1]), "avg_time_spent":newsAvgTimeMap.get(catWiseTrendingDist[k][i][1]), "shares":newsShareMap.get(catWiseTrendingDist[k][i][1])}  for i in range(len(catWiseTrendingDist[k]))]} for k in catWiseTrendingDist]
  MOST_SHARED.append({"category":"All", "news_list": [{"rank" : i+1, "hash_id" : normalized[3][i][1], "views": newsViewsMap.get(normalized[3][i][1]), "avg_time_spent": newsAvgTimeMap.get(normalized[3][i][1]), "shares": newsShareMap.get(normalized[3][i][1])} for i in range(len(normalized[3]))]})
  MOST_READ.append({"category":"All", "news_list": [{"rank" : i+1, "hash_id" : normalized[0][i][1], "views": newsViewsMap.get(normalized[0][i][1]), "avg_time_spent": newsAvgTimeMap.get(normalized[0][i][1]), "shares": newsShareMap.get(normalized[0][i][1])} for i in range(len(normalized[0]))]})
  TRENDING.append({"category":"All", "news_list": [{"rank" : i+1, "hash_id" : trending[i][1], "views":newsViewsMap.get(trending[i][1]), "avg_time_spent":newsAvgTimeMap.get(trending[i][1]), "shares": newsShareMap.get(trending[i][1])} for i in range(len(trending))]})

  try:
    if duration == "DAY1":
      text = ""
      textDown = ""
      NON_NOTIF_CATS = ["MISCELLANEOUS", "HATKE"]
      for x in TRENDING:
        cat = x["category"]
        if cat.upper() in NON_NOTIF_CATS:
          continue
        newss = [hashTitleMap[n["hash_id"]] + str(" - https://www.inshorts.com/en/news/" + hashOldHashMap[n["hash_id"]]) for n in x["news_list"][:1] if n["hash_id"] in EKDUM_LATEST_NEWS]
        newssDown = [hashTitleMap[n["hash_id"]] + str(" - https://www.inshorts.com/en/news/" + hashOldHashMap[n["hash_id"]]) for n in x["news_list"][-1:] if n["hash_id"] in EKDUM_LATEST_NEWS]
        if len(newss) > 0:
          text += cat.upper() + ":\n"
          for n in newss:
            text += str(n) + "\n"
          text += "\n"
        if len(newssDown) > 0:
          textDown += cat.upper() + ":\n"
          for n in newssDown:
            textDown += str(n) + "\n"
          textDown += "\n"
      if text != "" :
        text += "\nGo to: https://editor-panel.newsinshorts.com/notifications"
        title = "TRENDING STORIES"
        sendMailList(title, text, ["naveen@inshorts.com","Review@inshorts.com"])
      if textDown != "" :
        title = "NOT SO READ STORIES"
        #sendMailList(title, textDown, ["naveen@inshorts.com","Review@inshorts.com"])
      #pprint("text = " + text + ", textDown = ",textDown)
  except Exception as err:
    pprint('Error' + str(err))
    
  timestamp = round(time.time())
  payload = json.dumps({
      "time_period":duration,
      "timestamp":timestamp,
      "category_wise_details":{
          "MOST_SHARED": MOST_SHARED,
          "MOST_READ": MOST_READ,
          "TRENDING": TRENDING
      }
  })
  pprint("payload = " + payload)
  url = "https://write-api.newsinshorts.com/en/v1/analytics/"
  headers = {'X-AUTH-TOKEN' : 'mBJ8SZExJthGJFRMmCvMSA==', 'X-REGION-ID' : 'IN' ,'Content-Type':'application/json', 'Authorization': 'h6g2eG3lnIN2o73HTHjiDg=='}
  #payload = json.dumps({"most_trending_news" : most_trending_news, "most_read_news" : most_read_news, "most_shared_news":most_shared_news })
  r = requests.post(url, headers=headers, data=payload)
  print r

# COMMAND ----------

hour_of_day = int(datetime.today().strftime("%H"))
if hour_of_day is not None:
  #dumpStoriesFor("MINUTES30")
  dumpStoriesFor("HOUR1")
  dumpStoriesFor("HOURS3")
  dumpStoriesFor("HOURS6")
  dumpStoriesFor("HOURS12")
  dumpStoriesFor("DAY1")
else:
  print "Sorry dude, but it's so late. You shouldn't disturb people like this. Let them rest :)"

# COMMAND ----------

# url = "https://write-api.newsinshorts.com/en/v2/news/search"
# headers = {'X-AUTH-TOKEN' : 'lmLiu4zKJIJFG3Cg5W7ULQ==', 'Content-Type':'application/json', 'Authorization': 'h6g2eG3lnIN2o73HTHjiDg=='}
# payload = json.dumps({
#   "hash_id_list": ["47101684-0010-0002-1364-000012345678", "46641608-0010-0002-1362-000012345678"]
# })
# r = requests.post(url, headers=headers, data=payload)
# #print r.text
# newsCatMap = {}
# for news in json.loads(r.text):
#     print news# = list(news["category_names"])
# #print newsCatMap
