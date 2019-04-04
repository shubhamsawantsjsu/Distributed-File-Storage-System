import json
import pymongo
import yaml
from bson.objectid import ObjectId
from gridfs import GridFS



class Database():
    def __init__(self):
         config_dict = yaml.load(open('config.yaml'), Loader=yaml.FullLoader)
         config_dict = config_dict['two']
         mongo_uri = config_dict['mongodb_url']
         connection = pymongo.MongoClient(mongo_uri)
         dbfs = connection['Fluffy']
         self.fluffy_files = GridFS(dbfs)
     
    ##
    # File System
    ##
    def get_filebyid(self, file_id):
        file_id = ObjectId(file_id)
        file_object = self.fluffy_files.get(file_id)
        return file_object

    def list_files(self):
        results = self.fluffy_files.find({})
        for r in results:
            print('{0} {1}'.format(r['filename'], r['md5']))
        return [row for row in results]

    def search_files(self, search_query):
        results = self.fluffy_files.find(search_query)
        return [row for row in results]

    def get_strings(self, file_id):
        file_id = ObjectId(file_id)
        results = self.fluffy_files.find_one({'filename': '{0}_strings.txt'.format(str(file_id))})
        return results

    def create_file(self, file_data, session_id, sha256, filename, pid=None, file_meta=None):
        if len(session_id) == 24:
            session_id = ObjectId(session_id)
        file_id = self.fluffy_files.put(file_data, filename=filename, sess_id=session_id, sha256=sha256, pid=pid, file_meta=file_meta)
        return file_id
 
    def drop_file(self, file_id):
        file_id = ObjectId(file_id)
        self.fluffy_files.delete(file_id)
        return True    