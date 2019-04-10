from concurrent import futures

from threading import Thread
import os
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
from DeleteHelper import DeleteHelper
from lru import LRU

UPLOAD_SHARD_SIZE = 50*1024*1024

#
#   *** FileServer Service : FileServer service as per fileService.proto file. ***
#   *** This class implements all the required methods to serve the user requests. *** 
#
class FileServer(fileService_pb2_grpc.FileserviceServicer):
    def __init__(self, hostname, server_port, activeNodesChecker, shardingHandler, superNodeAddress):
        self.serverPort = server_port
        self.serverAddress = hostname+":"+server_port
        self.activeNodesChecker = activeNodesChecker
        self.shardingHandler = shardingHandler
        self.hostname = hostname
        self.lru = LRU(5)
        self.superNodeAddress = superNodeAddress
    
    #
    #   This service gets invoked when user uploads a new file.
    #
    def UploadFile(self, request_iterator, context):
        print("Inside Server method ---------- UploadFile")
        data=bytes("",'utf-8')
        username, filename = "", ""
        totalDataSize=0
        active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()

        # list to store the info related to file location.
        metaData=[]

        # If the node is the leader of the cluster. 
        if(int(db.get("primaryStatus"))==1):
            print("Inside primary upload")
            currDataSize = 0
            currDataBytes = bytes("",'utf-8')
            seqNo=1
            
            # Step 1:
            # Get 2 least loaded nodes based on the CPU stats. 
            # 'Node' is where the actual data goes and 'node_replica' is where replica will go.
            node, node_replica = self.getLeastLoadedNode()

            if(node==-1):
                return fileService_pb2.ack(success=False, message="Error Saving File. No active nodes.")

            # Step 2: 
            # Check whether file already exists, if yes then return with message 'File already exists'.
            for request in request_iterator:
                username, filename = request.username, request.filename
                print("Key is-----------------", username+"_"+filename)
                if(self.fileExists(username, filename)==1):
                    print("sending neg ack")
                    return fileService_pb2.ack(success=False, message="File already exists for this user. Please rename or delete file first.")
                break
            
            # Step 3:
            # Make chunks of size 'UPLOAD_SHARD_SIZE' and start sending the data to the least utilized node trough gRPC streaming.
            currDataSize+= sys.getsizeof(request.data)
            currDataBytes+=request.data

            for request in request_iterator:

                if((currDataSize + sys.getsizeof(request.data)) > UPLOAD_SHARD_SIZE):
                    response = self.sendDataToDestination(currDataBytes, node, node_replica, username, filename, seqNo, active_ip_channel_dict[node])
                    metaData.append([node, seqNo, node_replica])
                    currDataBytes = request.data
                    currDataSize = sys.getsizeof(request.data)
                    seqNo+=1
                    node, node_replica = self.getLeastLoadedNode()
                else:
                    currDataSize+= sys.getsizeof(request.data)
                    currDataBytes+=request.data

            if(currDataSize>0):
                response = self.sendDataToDestination(currDataBytes, node, node_replica, username, filename, seqNo, active_ip_channel_dict[node])
                metaData.append([node, seqNo, node_replica])

            # Step 4: 
            # Save the metadata on the primary node after the completion of sharding.
            if(response.success):
                db.saveMetaData(username, filename, metaData)
                db.saveUserFile(username, filename)

            # Step 5:
            # Make a gRPC call to replicate the matadata on all the other nodes.
            self.saveMetadataOnAllNodes(username, filename, metaData)

            return fileService_pb2.ack(success=True, message="Saved")

        # If the node is not the leader.
        else:
            print("Saving the data on my local db")
            sequenceNumberOfChunk = 0
            dataToBeSaved = bytes("",'utf-8')

            # Gather all the data from gRPC stream
            for request in request_iterator:
                username, filename, sequenceNumberOfChunk = request.username, request.filename, request.seqNo
                dataToBeSaved+=request.data
            key = username + "_" + filename + "_" + str(sequenceNumberOfChunk)

            # Save the data in local DB.
            db.setData(key, dataToBeSaved)

            # After saving the chunk in the local DB, make a gRPC call to save the replica of the chunk on different 
            # node only if the replicaNode is present.
            if(request.replicaNode!=""):
                print("Sending replication to ", request.replicaNode)
                replica_channel = active_ip_channel_dict[request.replicaNode]
                t1 = Thread(target=self.replicateChunkData, args=(replica_channel, dataToBeSaved, username, filename, sequenceNumberOfChunk ,))
                t1.start()
                # stub = fileService_pb2_grpc.FileserviceStub(replica_channel)
                # response = stub.UploadFile(self.sendDataInStream(dataToBeSaved, username, filename, sequenceNumberOfChunk, ""))

            return fileService_pb2.ack(success=True, message="Saved")

    def replicateChunkData(self, replica_channel, dataToBeSaved, username, filename, sequenceNumberOfChunk):
        stub = fileService_pb2_grpc.FileserviceStub(replica_channel)
        response = stub.UploadFile(self.sendDataInStream(dataToBeSaved, username, filename, sequenceNumberOfChunk, ""))

    # This helper method is responsible for sending the data to destination node through gRPC stream.
    def sendDataToDestination(self, currDataBytes, node, nodeReplica, username, filename, seqNo, channel):
        if(node==self.serverAddress):
            key = username + "_" + filename + "_" + str(seqNo)
            db.setData(key, currDataBytes)
            if(nodeReplica!=""):
                print("Sending replication to ", nodeReplica)
                active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()
                replica_channel = active_ip_channel_dict[nodeReplica]
                stub = fileService_pb2_grpc.FileserviceStub(replica_channel)
                response = stub.UploadFile(self.sendDataInStream(currDataBytes, username, filename, seqNo, ""))
                return response
        else:
            print("Sending the UPLOAD_SHARD_SIZE to node :", node)
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            response = stub.UploadFile(self.sendDataInStream(currDataBytes, username, filename, seqNo, nodeReplica))
            print("Response from uploadFile: ", response.message)
            return response

    # This helper method actually makes chunks of less than 4MB and streams them through gRPC.
    # 4 MB is the max data packet size in gRPC while sending. That's why it is necessary. 
    def sendDataInStream(self, dataBytes, username, filename, seqNo, replicaNode):
        chunk_size = 4000000
        start, end = 0, chunk_size
        while(True):
            chunk = dataBytes[start:end]
            if(len(chunk)==0): break
            start=end
            end += chunk_size
            yield fileService_pb2.FileData(username=username, filename=filename, data=chunk, seqNo=seqNo, replicaNode=replicaNode)

    #
    #   This service gets invoked when user requests an uploaded file.
    #
    def DownloadFile(self, request, context):

        print("Inside Download")

        # If the node is the leader of the cluster. 
        if(int(db.get("primaryStatus"))==1):
            
            print("Inside primary download")
            
            # Check if file exists
            if(self.fileExists(request.username, request.filename)==0):
                print("File does not exist")
                yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=bytes("",'utf-8'), seqNo = 0)
                return

            # If the file is present in cache then just fetch it and return. No need to go to individual node.
            if(self.lru.has_key(request.username + "_" + request.filename)):
                print("Fetching data from Cache")
                CHUNK_SIZE=4000000
                fileName = request.username + "_" + request.filename
                filePath = self.lru[fileName]
                outfile = os.path.join(filePath, fileName)
                
                with open(outfile, 'rb') as infile:
                    while True:
                        chunk = infile.read(CHUNK_SIZE)
                        if not chunk: break
                        yield fileService_pb2.FileData(username=request.username, filename=request.filename, data=chunk, seqNo=1)
            
            # If the file is not present in the cache, then fetch it from the individual node.
            else:
                print("Fetching the metadata")

                # Step 1: get metadata i.e. the location of chunks.
                metaData = db.parseMetaData(request.username, request.filename)

                print(metaData)
                
                #Step 2: make gRPC calls and get the fileData from all the nodes.
                downloadHelper = DownloadHelper(self.hostname, self.serverPort, self.activeNodesChecker)
                data = downloadHelper.getDataFromNodes(request.username, request.filename, metaData)
                print("Sending the data to client")

                #Step 3: send the file to supernode using gRPC streaming.
                chunk_size = 4000000
                start, end = 0, chunk_size
                while(True):
                    chunk = data[start:end]
                    if(len(chunk)==0): break
                    start=end
                    end += chunk_size
                    yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=chunk, seqNo = request.seqNo)
                
                # Step 4: update the cache based on LRU(least recently used) algorithm.
                self.saveInCache(request.username, request.filename, data)

        # If the node is not the leader, then just fetch the fileChunk from the local db and stream it back to leader.
        else:
            key = request.username + "_" + request.filename + "_" + str(request.seqNo)
            print(key)
            data = db.getFileData(key)
            chunk_size = 4000000
            start, end = 0, chunk_size
            while(True):
                chunk = data[start:end]
                if(len(chunk)==0): break
                start=end
                end += chunk_size
                yield fileService_pb2.FileData(username = request.username, filename = request.filename, data=chunk, seqNo = request.seqNo)

    # This service is responsible fetching all the files.
    def FileList(self, request, context):
        print("File List Called")
        userFiles = db.getUserFiles(request.username)
        return fileService_pb2.FileListResponse(Filenames=str(userFiles))
    
    # This helper method checks whether the file is present in db or not.
    def fileExists(self, username, filename):
        print("isFile Present", db.keyExists(username + "_" + filename))
        return db.keyExists(username + "_" + filename)
    
    # This helper method returns 2 least loaded nodes from the cluster.
    def getLeastLoadedNode(self):
        print("Ready to enter sharding handler")
        node, node_replica = self.shardingHandler.leastUtilizedNode()
        print("Least loaded node is :", node)
        print("Replica node - ", node_replica)
        return node, node_replica

    # This helper method replicates the metadata on all nodes.
    def saveMetadataOnAllNodes(self, username, filename, metadata):
        print("saveMetadataOnAllNodes")
        active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()
        uniqueFileName = username + "_" + filename
        for ip, channel in active_ip_channel_dict.items():
            if(self.isChannelAlive(channel)):
                stub = fileService_pb2_grpc.FileserviceStub(channel)
                response = stub.MetaDataInfo(fileService_pb2.MetaData(filename=uniqueFileName, seqValues=str(metadata).encode('utf-8')))
                print(response.message)

    # This service is responsible for saving the metadata on local db.
    def MetaDataInfo(self, request, context):
        print("Inside Metadatainfo")
        fileName = request.filename
        seqValues = request.seqValues
        db.saveMetaDataOnOtherNodes(fileName, seqValues)
        ack_message = "Successfully saved the metadata on " + self.serverAddress
        return fileService_pb2.ack(success=True, message=ack_message)

    # This helper method checks whethere created channel is alive or not
    def isChannelAlive(self, channel):
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            #print("Connection timeout. Unable to connect to port ")
            return False
        return True
    
    # This helper method is responsible for updating the cache for faster lookup.
    def saveInCache(self, username, filename, data):
        if(len(self.lru.items())>=self.lru.get_size()):
            fileToDel, path = self.lru.peek_last_item()
            os.remove(path+"/"+fileToDel)
        
        self.lru[username+"_"+filename]="cache"
        filePath=os.path.join('cache', username+"_"+filename)
        saveFile = open(filePath, 'wb')
        saveFile.write(data)
        saveFile.close()

    # This service is responsible for sending the whole cluster stats to superNode
    def getClusterStats(self, request, context):
        print("Inside getClusterStats")
        active_ip_channel_dict = self.activeNodesChecker.getActiveChannels()
        total_cpu_usage, total_disk_space, total_used_mem = 0.0,0.0,0.0
        total_nodes = 0
        for ip, channel in active_ip_channel_dict.items():
            if(self.isChannelAlive(channel)):
                stub = heartbeat_pb2_grpc.HearBeatStub(channel)
                stats = stub.isAlive(heartbeat_pb2.NodeInfo(ip="", port=""))
                total_cpu_usage = float(stats.cpu_usage)
                total_disk_space = float(stats.disk_space)
                total_used_mem = float(stats.used_mem)
                total_nodes+=1

        if(total_nodes==0):
            return fileService_pb2.ClusterStats(cpu_usage = str(100.00), disk_space = str(100.00), used_mem = str(100.00))

        return fileService_pb2.ClusterStats(cpu_usage = str(total_cpu_usage/total_nodes), disk_space = str(total_disk_space/total_nodes), used_mem = str(total_used_mem/total_nodes))

    # This service is responsible for sending the leader info to superNode as soon as leader changes.
    def getLeaderInfo(self, request, context):
        channel = grpc.insecure_channel('{}'.format(self.superNodeAddress))
        stub = fileService_pb2_grpc.FileserviceStub(channel)
        response = stub.getLeaderInfo(fileService_pb2.ClusterInfo(ip = self.hostname, port= self.serverPort, clusterName="team1"))
        print(response.message)

    #
    #   This service gets invoked when user deletes a file.
    #
    def FileDelete(self, request, data):
        username = request.username
        filename = request.filename

        if(int(db.get("primaryStatus"))==1):

            if(self.fileExists(username, filename)==0):
                print("File does not exist")
                return fileService_pb2.ack(success=False, message="File does not exist")

            print("Fetching metadata from leader")
            metadata = db.parseMetaData(request.username, request.filename)
            print("Successfully retrieved metadata from leader")

            deleteHelper = DeleteHelper(self.hostname, self.serverPort, self.activeNodesChecker)
            deleteHelper.deleteFileChunksAndMetaFromNodes(username, filename, metadata)

            return fileService_pb2.ack(success=True, message="Successfully deleted file from the cluster")

        else:
            seqNo = -1

            try:
                seqNo = request.seqNo
            except:
                return fileService_pb2.ack(success=False, message="Internal Error")

            metaDataKey = username+"_"+filename 
            dataChunkKey = username+"_"+filename+"_"+str(seqNo)

            if(db.keyExists(metaDataKey)==1):
                print("FileDelete: Deleting the metadataEntry from local db :")
                db.deleteEntry(metaDataKey)
            if(db.keyExists(dataChunkKey)):
                print("FileDelete: Deleting the data chunk from local db: ")
                db.deleteEntry(dataChunkKey)

            return fileService_pb2.ack(success=True, message="Successfully deleted file from the cluster")

    #
    #   This service gets invoked when user wants to check if the file is present.
    #
    def FileSearch(self, request, data):
        username, filename = request.username, request.filename

        if(self.fileExists(username, filename)==1):
            return fileService_pb2.ack(success=True, message="File exists in the cluster.")
        else:
            return fileService_pb2.ack(success=False, message="File does not exist in the cluster.")

    #
    #   This service gets invoked when user wants to update a file.
    #
    def UpdateFile(self, request_iterator, context):
        
        username, filename = "", ""
        fileData = bytes("",'utf-8')

        for request in request_iterator:
            fileData+=request.data
            username, filename = request.username, request.filename

        def getFileChunks(fileData):
            # Maximum chunk size that can be sent
            CHUNK_SIZE=4000000

            outfile = os.path.join('files', fileName)
            
            sTime=time.time()

            while True:
                chunk = fileData.read(CHUNK_SIZE)
                if not chunk: break

                yield fileService_pb2.FileData(username=username, filename=fileName, data=chunk, seqNo=1)
            print("Time for upload= ", time.time()-sTime)

        if(int(db.get("primaryStatus"))==1):
            channel = grpc.insecure_channel('{}'.format(self.serverAddress))
            stub = fileService_pb2_grpc.FileserviceStub(channel)

            response1 = stub.FileDelete(fileService_pb2.FileInfo(username=userName, filename=fileName))

            if(response1.success):
                response2 = stub.UploadFile(getFileChunks(fileData))
                if(response2.success):
                    return fileService_pb2.ack(success=True, message="File suceessfully updated.")
                else:
                    return fileService_pb2.ack(success=False, message="Internal error.")
            else:
                return fileService_pb2.ack(success=False, message="Internal error.")





            












            




        



    

