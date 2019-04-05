from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import grpc
import sys
sys.path.append('../generated')
sys.path.append('../utils')
import db
import fileService_pb2_grpc
import fileService_pb2
import heartbeat_pb2_grpc
import heartbeat_pb2
import time
import yaml
import threading
import hashlib
import concurrent.futures

class DownloadHelper():

    def __init__(self, hostname, server_port, activeNodesChecker):
        self.active_ip_channel_dict = activeNodesChecker.getActiveChannels()
        self.serverAddress = hostname+":"+server_port
        self.seqDataMap = {}

    def getDataFromNodes(self, username, filename, metaData):
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
            list_of_executors = {executor.submit(self.getDataFromIndividualNode, metas, username, filename): metas for metas in metaData}
            for future in concurrent.futures.as_completed(list_of_executors):
                try:
                    future.result()
                except Exception as exec:
                    print(exec)

        print("All tasks are completed")
        return self.buildTheDataFromMap()

    def getDataFromIndividualNode(self, meta, username, filename):
        print("Inside getDataFromIndividualNode")
        print("Task Executed {}".format(threading.current_thread()))
        node, seqNo = str(meta[0]), meta[1]

        data = bytes("",'utf-8')
        result = {}

        if(node==str(self.serverAddress)):
            key = username + "_" + filename + "_" + str(seqNo)
            data = db.getFileData(key)
        else:
            print("Fetching Data from Node {}".format(node))
            channel = self.active_ip_channel_dict[node]
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            responses = stub.DownloadFile(fileService_pb2.FileInfo(username = username, filename = filename, seqNo = seqNo))
            for response in responses:
                data+=response.data

        self.seqDataMap[seqNo] = data
        print("returning from the getDataFromIndividualNode")

    def buildTheDataFromMap(self):
        fileData = bytes("",'utf-8')
        totalNumberOfChunks = len(self.seqDataMap)

        for i in range(1, totalNumberOfChunks+1):
            fileData+=self.seqDataMap.get(i)
        
        return fileData