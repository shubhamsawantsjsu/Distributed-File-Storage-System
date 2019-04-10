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

#
#   *** DownloadHelper Utility : Helper class to fetch fileData chunks from other nodes. ***
#
class DownloadHelper():

    def __init__(self, hostname, server_port, activeNodesChecker):
        self.active_ip_channel_dict = activeNodesChecker.getActiveChannels()
        self.serverAddress = hostname+":"+server_port
        self.seqDataMap = {}

    # This method is responsible for getting data from all the nodes using medata.
    # This method stitches back the data and sends to UploadFile service. 
    def getDataFromNodes(self, username, filename, metaData):
        
        # Define a threadpool and start separate threads where each thread will get data from a particular node and will update the 'seqDataMap'.
        with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
            list_of_executors = {executor.submit(self.getDataFromIndividualNode, metas, username, filename): metas for metas in metaData}
            for future in concurrent.futures.as_completed(list_of_executors):
                try:
                    future.result()
                except Exception as exec:
                    print(exec)

        print("All tasks are completed")

        # Stitch the data back from 'seqDataMap'.
        return self.buildTheDataFromMap()

    # This method is responsible for getting data from specific node.
    # Multiple threads will call this method to get the data parallely from each node.
    def getDataFromIndividualNode(self, meta, username, filename):
        print("Inside getDataFromIndividualNode")
        print("Task Executed {}".format(threading.current_thread()))
        node, seqNo, replicaNode = str(meta[0]), meta[1], str(meta[2])

        data = bytes("",'utf-8')
        result = {}

        if(node==str(self.serverAddress)):
            key = username + "_" + filename + "_" + str(seqNo)
            data = db.getFileData(key)
        else:
            if(node in self.active_ip_channel_dict): 
                channel = self.active_ip_channel_dict[node]
                print("Fetching Data from Node {}".format(node))
            elif(replicaNode in self.active_ip_channel_dict):
                channel = self.active_ip_channel_dict[replicaNode]
                print("Fetching Data from Node {}".format(replicaNode))
            else:
                print("Both original and replica nodes are down!")
                return

            stub = fileService_pb2_grpc.FileserviceStub(channel)
            responses = stub.DownloadFile(fileService_pb2.FileInfo(username = username, filename = filename, seqNo = seqNo))
            for response in responses:
                data+=response.data

        self.seqDataMap[seqNo] = data
        print("returning from the getDataFromIndividualNode")

    # This method is responsible for stitching back the data from seqDataMap.
    def buildTheDataFromMap(self):
        fileData = bytes("",'utf-8')
        totalNumberOfChunks = len(self.seqDataMap)

        for i in range(1, totalNumberOfChunks+1):
            fileData+=self.seqDataMap.get(i)
        
        return fileData