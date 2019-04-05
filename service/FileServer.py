from concurrent import futures

import grpc
import sys
sys.path.append('../generated')
sys.path.append('../utils')
sys.path.append('../proto')
import db
import fileService_pb2_grpc
import fileService_pb2
import heartbeat_pb2_grpc
import heartbeat_pb2
import time
import yaml
import threading
import hashlib
from ShardingHandler import ShardingHandler
from DownloadHelper import DownloadHelper

UPLOAD_SHARD_SIZE = 50*1024*1024

class FileServer(fileService_pb2_grpc.FileserviceServicer):
    def __init__(self, primary, hostname, server_port, activeNodesChecker, shardingHandler):
        self.primary = primary
        self.serverPort = server_port
        self.serverAddress = hostname+":"+server_port
        self.activeNodesChecker = activeNodesChecker
        self.shardingHandler = shardingHandler
        self.hostname = hostname

    def UploadFile(self, request_iterator, context):
        print("Inside Server method ---------- UploadFile")
        data=bytes("",'utf-8')
        username, filename = "", ""
        totalDataSize=0
        active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()

        metaData=[]

        if(self.primary==1):

            currDataSize = 0
            currDataBytes = bytes("",'utf-8')
            seqNo=1
            
            node = self.getLeastLoadedNode()

            if(node==-1):
                return fileService_pb2.ack(success=False, message="Error Saving File. No active nodes.")

            for request in request_iterator:
                username, filename = request.username, request.filename
                if(self.fileExists(username, filename)):
                    return fileService_pb2.ack(success=False, message="File already exists for this user.")
                break
            
            currDataSize+= sys.getsizeof(request.data)
            currDataBytes+=request.data
            
            print("Came here")
            for request in request_iterator:
                if((currDataSize + sys.getsizeof(request.data)) > UPLOAD_SHARD_SIZE):
                    self.sendDataToDestination(currDataBytes, node, username, filename, seqNo, active_ip_channel_dict[node])
                    metaData.append([node, seqNo])
                    currDataBytes = request.data
                    currDataSize = sys.getsizeof(request.data)
                    seqNo+=1
                    node = self.shardingHandler.leastUtilizedNode()
                else:
                    currDataSize+= sys.getsizeof(request.data)
                    currDataBytes+=request.data

            if(currDataSize>0):
                self.sendDataToDestination(currDataBytes, node, username, filename, seqNo, active_ip_channel_dict[node])
                metaData.append([node, seqNo])

            db.saveMetaData(username, filename, metaData)
            self.saveMetadataOnAllNodes(username, filename, metaData)
            return fileService_pb2.ack(success=True, message="Saved")

        else:
            print("Saving the data on my local db")
            sequenceNumberOfChunk = 0
            dataToBeSaved = bytes("",'utf-8')
            for request in request_iterator:
                username, filename, sequenceNumberOfChunk = request.username, request.filename, request.seqNo
                dataToBeSaved+=request.data
            key = username + "_" + filename + "_" + str(sequenceNumberOfChunk)
            db.setData(key, dataToBeSaved)
            return fileService_pb2.ack(success=True, message="Saved")

    def sendDataToDestination(self, currDataBytes, node, username, filename, seqNo, channel):
        if(node==self.serverAddress):
            #print("Self node : saving the data on local db")
            key = username + "_" + filename + "_" + str(seqNo)
            db.setData(key, currDataBytes)
        else:
            print("Sending the UPLOAD_SHARD_SIZE to node :", node)
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            response = stub.UploadFile(self.sendDataInStream(currDataBytes, username, filename, seqNo))
            print("Response from uploadFile: ", response.message)

    def sendDataInStream(self, dataBytes, username, filename, seqNo):
        chunk_size = 4000000
        start, end = 0, chunk_size
        while(True):
            chunk = dataBytes[start:end]
            if(len(chunk)==0): break
            start=end
            end += chunk_size
            yield fileService_pb2.FileData(username=username, filename=filename, data=chunk, seqNo=seqNo)

    def DownloadFile(self, request, context):

        if(self.primary==1):
            metaData = db.parseMetaData(request.username, request.filename)
            downloadHelper = DownloadHelper(self.primary, self.hostname, self.serverPort, self.activeNodesChecker)
            data = downloadHelper.getDataFromNodes(request.username, request.filename, metaData)
            print("Sending the data to client")
            chunk_size = 4000000
            start, end = 0, chunk_size
            while(True):
                chunk = data[start:end]
                if(len(chunk)==0): break
                start=end
                end += chunk_size
                yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=chunk, seqNo = request.seqNo)

        else:
            key = request.username + "_" + request.filename + "_" + str(request.seqNo)
            data = db.getFileData(key)
            chunk_size = 4000000
            start, end = 0, chunk_size
            while(True):
                chunk = data[start:end]
                if(len(chunk)==0): break
                start=end
                end += chunk_size
                yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=chunk, seqNo = request.seqNo)

    # def DownloadFile(self, request, context):
    #     print("Download File - ", request.filename)

    #     #If primary, find which node has file
    #     if(self.primary==1):
    #         metaData = db.parseMetaData(request.username, request.filename)
    #         for meta in metaData:
    #             #print("Meta=", meta)
    #             node, seqNo = str(meta[0]), meta[1]
    #             if(node==str(self.serverAddress)):
    #                 key = request.username + "_" + request.filename + "_" + str(seqNo)
    #                 data = db.getFileData(key)
    #                 chunk_size = 4000000
    #                 start, end = 0, chunk_size
    #                 while(True):
    #                     chunk = data[start:end]
    #                     if(len(chunk)==0): break
    #                     start=end
    #                     end += chunk_size
    #                     yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=chunk, seqNo = seqNo)
                    
    #             else:
    #                 print("Fetching Data from Node {}".format(node))
    #                 active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()
    #                 channel = active_ip_channel_dict[node]
    #                 stub = fileService_pb2_grpc.FileserviceStub(channel)
    #                 responses = stub.DownloadFile(fileService_pb2.FileInfo(username = request.username, filename = request.filename, seqNo = seqNo))
    #                 for response in responses:
    #                     yield response
    #     #else not primary - search for file
    #     else:
    #         key = request.username + "_" + request.filename + "_" + str(request.seqNo)
    #         data = db.getFileData(key)
    #         chunk_size = 3*1024*1024
    #         start, end = 0, chunk_size
    #         while(True):
    #             chunk = data[start:end]
    #             if(len(chunk)==0): break
    #             start=end
    #             end += chunk_size
    #             yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=chunk, seqNo = request.seqNo)

    def ListFiles(self, request, context):
        print("List Files Called")

        #Get files in DB and return file names
        return fileService_pb2.FileList(lstFileNames="FILE-LIST")
    
    def fileExists(self, username, filename):
        return db.keyExists(username + "_" + filename)
    
    def getLeastLoadedNode(self):
        print("Ready to enter sharding handler")
        node = self.shardingHandler.leastUtilizedNode()
        print("Least loaded node is :", node)
        return node

    def MetaDataInfo(self, request, context):
        print("Inside Metadatainfo")
        fileName = request.filename
        seqValues = request.seqValues
        db.saveMetaDataOnOtherNodes(fileName, seqValues)
        ack_message = "Successfully saved the metadata on " + self.serverAddress
        return fileService_pb2.ack(success=False, message=ack_message)

    def saveMetadataOnAllNodes(self, username, filename, metadata):
        print("saveMetadataOnAllNodes")
        active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()
        uniqueFileName = username + "_" + filename
        for ip, channel in active_ip_channel_dict.items():
            if(self.isChannelAlive(channel)):
                print("Active IP", ip)
                stub = fileService_pb2_grpc.FileserviceStub(channel)
                print("STUB->", stub)
                response = stub.MetaDataInfo(fileService_pb2.MetaData(filename=uniqueFileName, seqValues=str(metadata).encode('utf-8')))
                print(response.message)

    def isChannelAlive(self, channel):
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            #print("Connection timeout. Unable to connect to port ")
            return False
        return True
            

