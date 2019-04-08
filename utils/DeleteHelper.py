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

class DeleteHelper():

    def __init__(self, hostname, server_port, activeNodesChecker):
        self.serverAddress = hostname+":"+server_port
        self.active_ip_channel_dict = activeNodesChecker.getActiveChannels()

    def deleteFileChunksAndMetaFromNodes(self, username, filename, metaData):

        # Start separate threads where each thread will go to the node and delete the file chunk.
        with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
            list_of_executors = {executor.submit(self.deleteDataAndMetaFromIndividualChunk, metas, username, filename): metas for metas in metaData}
            for future in concurrent.futures.as_completed(list_of_executors):
                try:
                    future.result()
                except Exception as exec:
                    print(exec)

        print("All tasks are completed")

    def deleteDataAndMetaFromIndividualChunk(self, meta, username, filename):

        node, seqNo, replicaNode = str(meta[0]), meta[1], str(meta[2])
        metaDataKey = username+"_"+filename 
        dataChunkKey = username+"_"+filename+"_"+str(seqNo)

        if(node==str(self.serverAddress)):
            if(db.keyExists(metaDataKey)==1):
                print("Deleting the metadataEntry from local db :", node)
                db.deleteEntry(metaDataKey)
            if(db.keyExists(dataChunkKey)):
                print("Deleting the data chunk from local db:", node)
                db.deleteEntry(dataChunkKey)
        else:
            if(node in self.active_ip_channel_dict): 
                channel = self.active_ip_channel_dict[node]
                stub = fileService_pb2_grpc.FileserviceStub(channel)
                response = stub.FileDelete(fileService_pb2.FileInfo(username=username, filename=filename, seqNo=seqNo))

                if(response.success==True):
                    print("Successfully deleted chunk from node : ", node)
                else:
                    print("Chunk could not be deleted from node :", node)

            if(replicaNode in self.active_ip_channel_dict):
                channel = self.active_ip_channel_dict[replicaNode]
                stub = fileService_pb2_grpc.FileserviceStub(channel)
                response = stub.FileDelete(fileService_pb2.FileInfo(username=username, filename=filename, seqNo=seqNo))
                
                print(type(response.success))
                if(response.success==True):
                    print("Successfully deleted chunk from ReplicaNode : ", node)
                else:
                    print("Chunk could not be deleted from ReplicaNode :", node)




