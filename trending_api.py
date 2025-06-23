import requests, json
def readFile(filepath):
  array = open(filepath).read().split('\n')
  return filter(lambda x: x != '', array)
trending = readFile('/mnt/data/trending.txt')
url = "https://write-api.newsinshorts.com/en/v2/news/trending"
headers = {'X-AUTH-TOKEN' : 'mBJ8SZExJthGJFRMmCvMSA==', 'X-REGION-ID' : 'IN' ,'Content-Type':'application/json', 'Authorization': 'h6g2eG3lnIN2o73HTHjiDg=='}
payload = json.dumps({"group_id_set":trending})
r = requests.put(url, headers=headers, data=payload)
print r
