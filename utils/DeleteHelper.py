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
        self.activeNodesChecker = activeNodesChecker

    # This method is responsible for deleting dataChunks and metadata from all the nodes.
    def deleteFileChunksAndMetaFromNodes(self, username, filename, metaData):

        # Define a threadpool and start separate threads where each thread will go to the node and delete the file chunk.
        with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
            list_of_executors = {executor.submit(self.deleteDataAndMetaFromIndividualChunk, metas, username, filename): metas for metas in metaData}
            for future in concurrent.futures.as_completed(list_of_executors):
                try:
                    future.result()
                except Exception as exec:
                    print(exec)

        print("All tasks are completed")

    # This method takes meta object which consists of node address, seqNo and replicaNode address. 
    # It deletes corresponding dataChunk and metadata.
    def deleteDataAndMetaFromIndividualChunk(self, meta, username, filename):

        node, seqNo, replicaNode = str(meta[0]), meta[1], str(meta[2])
        metaDataKey = username+"_"+filename
        dataChunkKey = username+"_"+filename+"_"+str(seqNo)

        if(db.keyExists(metaDataKey)==1):
            print("deleteDataAndMetaFromIndividualChunk: Deleting the metadataEntry from local db :", node)
            db.deleteEntry(metaDataKey)

        if(db.keyExists(dataChunkKey)):
            print("deleteDataAndMetaFromIndividualChunk: Deleting the data chunk from local db:", node)
            db.deleteEntry(dataChunkKey)

        active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()

        if( node != self.serverAddress and node in active_ip_channel_dict):
            channel = active_ip_channel_dict[node]
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            response = stub.FileDelete(fileService_pb2.FileInfo(username=username, filename=filename, seqNo=seqNo))

            if(response.success==True):
                print("deleteDataAndMetaFromIndividualChunk : Successfully deleted chunk from node : ", node)
            else:
                print("deleteDataAndMetaFromIndividualChunk : Chunk could not be deleted from node :", node)

        if(replicaNode!=self.serverAddress and replicaNode in self.activeNodesChecker.getActiveChannels()):
            print("Deleting ")
            channel = active_ip_channel_dict[replicaNode]
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            response = stub.FileDelete(fileService_pb2.FileInfo(username=username, filename=filename, seqNo=seqNo))

            print(type(response.success))
            if(response.success==True):
                print("Successfully deleted chunk from ReplicaNode : ", node)
            else:
                print("Chunk could not be deleted from ReplicaNode :", node)