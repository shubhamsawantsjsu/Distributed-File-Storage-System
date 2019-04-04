import redis

_redis_port = 6379

r = redis.StrictRedis(host='localhost', port=_redis_port, db=0)

def setData(key, value):
    r.set(key,value)

def getData(key):
    return (r.get(key)).decode('utf-8')

def get(key):
    return (r.get(key))

#metadata -> node, seq
def saveMetaData(username, filename, metaData):
    key = username + "_" + filename
    val = ""
    r.set(key,str(metaData).encode('utf-8'))
    # for data in metaData:
    #     temp = ""
    #     for item in data:   # node and seq
    #         temp+=str(item)+","
    #     temp = temp[:-1]
    #     val+= temp+"_"
    # val = val[:-1]
    # print(val)
    #r.set(key, val)

def saveList(key, value):
    r.rpush(key, value)


