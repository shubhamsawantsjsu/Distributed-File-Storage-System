import redis
import ast

_redis_port = 6379

r = redis.StrictRedis(host='localhost', port=_redis_port, db=0)

def setData(key, value):
    r.set(key,value)

def getData(key):
    return (r.get(key)).decode('utf-8')

def get(key):
    return (r.get(key))

def getFileData(key):
    return r.get(key)
    
#metadata -> node, seq
def saveMetaData(username, filename, metaData):
    key = username + "_" + filename
    r.set(key,str(metaData).encode('utf-8'))

def parseMetaData(username, filename):
    key = username + "_" + filename
    return ast.literal_eval(r.get(key).decode('utf-8'))

def saveList(key, value):
    r.rpush(key, value)


