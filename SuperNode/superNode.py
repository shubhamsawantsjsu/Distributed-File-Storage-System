import sys  
sys.path.append('./proto')
from concurrent import futures
from threading import Thread
import grpc
    
import db
import fileService_pb2_grpc
import fileService_pb2
import time
import threading
from ClusterStatus import ClusterStatus


_ONE_DAY_IN_SECONDS = 60 * 60 * 24

#
#   *** FileServer Service : FileServer service as per fileService.proto file. ***
#   *** This class implements all the required methods to serve the user requests. *** 
#
class FileServer(fileService_pb2_grpc.FileserviceServicer):
    def __init__(self, hostIP, port):
        self.serverAddress = hostIP+":"+port
        self.clusterLeaders = {}
        self.clusterStatus = ClusterStatus()
        self.ip_channel_dict = {}

    #
    #   This service gets invoked when each cluster's leader informs the supernode
    #   about who the current cluster leader is
    #
    def getLeaderInfo(self, request, context):
        print("getLeaderInfo Called")
        address = request.ip + ":" + request.port
        self.clusterLeaders[request.clusterName] = address
        print("ClusterLeaders: ",self.clusterLeaders)
        channel = grpc.insecure_channel('{}'.format(address))
        self.ip_channel_dict[address] = channel
        return fileService_pb2.ack(success=True, message="Leader Updated.")

    #
    #   This service gets invoked when client uploads a new file.
    #
    def UploadFile(self, request_iterator, context):
        print("Inside Server method ---------- UploadFile")
        
        # Get the two clusters that have the most resources based on cluster stats
        node, node_replica, clusterName, clusterReplica = self.clusterStatus.leastUtilizedNode(self.clusterLeaders)
       
        if(node==-1):
            return fileService_pb2.ack(success=False, message="No Active Clusters.")
        
        print("Node found is:{}, replica is:{}".format(node, node_replica))

        channel1 = self.ip_channel_dict[node]
        stub1 = fileService_pb2_grpc.FileserviceStub(channel1)
        if(node_replica!="" and node_replica in self.ip_channel_dict):
            channel2 = self.ip_channel_dict[node_replica]
            stub2 = fileService_pb2_grpc.FileserviceStub(channel2)
        else: stub2 = None
        
        filename, username = "",""
        data = bytes("",'utf-8')
        
        for request in request_iterator:
            filename, username = request.filename, request.username
            data=request.data
            break
        
        if(self.fileExists(username, filename)):
            return fileService_pb2.ack(success=False, message="File already exists for this user. Please rename or delete file first.")

        
        def sendDataStreaming(username, filename, data):
            yield fileService_pb2.FileData(username=username, filename=filename, data=data)
            for request in request_iterator:
                data+=request.data
                yield fileService_pb2.FileData(username=request.username, filename=request.filename, data=request.data)
        
        resp1 = stub1.UploadFile(sendDataStreaming(username, filename, request.data))
        
        # Replicate current file to alternate cluster
        if(stub2 is not None):
            t1 = Thread(target=self.replicateData, args=(stub2,username,filename,data,))
            t1.start()

        # save Meta Map of username+filename->clusterName that its stored on
        if(resp1.success):
            db.saveMetaData(username, filename, clusterName, clusterReplica)
            db.saveUserFile(username, filename)
        
        return resp1


    #
    #   This service gets invoked when user requests an uploaded file.
    #
    def DownloadFile(self, request, context):

         # Check if file exists
        if(self.fileExists(request.username, request.filename)==False):
            return fileService_pb2.FileData(username=request.username, filename=request.filename, data=bytes("",'utf-8'))

        fileMeta = db.parseMetaData(request.username, request.filename)
        
        primaryIP, replicaIP = -1,-1
        channel1, channel2 = -1,-1
        if(fileMeta[0] in self.clusterLeaders): 
            primaryIP = self.clusterLeaders[fileMeta[0]]
            channel1 = self.clusterStatus.isChannelAlive(primaryIP)
            
        if(fileMeta[1] in self.clusterLeaders):
            replicaIP = self.clusterLeaders[fileMeta[1]]
            channel2 = self.clusterStatus.isChannelAlive(replicaIP)

        if(channel1):
            stub = fileService_pb2_grpc.FileserviceStub(channel1)
            responses = stub.DownloadFile(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
            for response in responses:
                yield response
        elif(channel2):
            stub = fileService_pb2_grpc.FileserviceStub(channel2)
            responses = stub.DownloadFile(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
            for response in responses:
                yield response
        else:
            return fileService_pb2.FileData(username=request.username, filename=request.filename, data=bytes("",'utf-8'))

    #
    #   Function to check if file exists in db (redis)
    #  
    def fileExists(self,username, filename):
        return db.keyExists(username + "_" + filename)

    #
    #   Function that takes care of file replication on alternate cluster
    #  
    def replicateData(self,stub, username, filename, data):
        
        def streamData(username, filename, data):
            chunk_size = 4000000
            start, end = 0, chunk_size
            while(True):
                chunk = data[start:end]
                if(len(chunk)==0): break
                start=end
                end += chunk_size
                yield fileService_pb2.FileData(username=username, filename=filename, data=chunk)
            
        resp = stub.UploadFile(streamData(username,filename,data))


    #
    #   This services is invoked when user wants to delete a file
    #  
    def FileDelete(self, request, context):
        print("In FileDelete")

        if(self.fileExists(request.username, request.filename)==False):
            return fileService_pb2.ack(success=False, message="File {} does not exist.".format(request.filename))

        fileMeta = db.parseMetaData(request.username, request.filename)
        print("FileMeta = ", fileMeta)

        primaryIP, replicaIP = -1,-1
        channel1, channel2 = -1,-1
        if(fileMeta[0] in self.clusterLeaders): 
            primaryIP = self.clusterLeaders[fileMeta[0]]
            channel1 = self.clusterStatus.isChannelAlive(primaryIP)
            
        if(fileMeta[1] in self.clusterLeaders):
            replicaIP = self.clusterLeaders[fileMeta[1]]
            channel2 = self.clusterStatus.isChannelAlive(replicaIP)
        
        print("PrimarIP={}, replicaIP={}".format(primaryIP,replicaIP))

        if(channel1!=-1):
            stub = fileService_pb2_grpc.FileserviceStub(channel1)
            response = stub.FileDelete(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
        
        if(channel2!=-1):
            stub = fileService_pb2_grpc.FileserviceStub(channel2)
            response = stub.FileDelete(fileService_pb2.FileInfo(username = request.username, filename = request.filename))

        if(response.success==True):
            db.deleteEntry(request.username + "_" + request.filename)
            return fileService_pb2.ack(success=True, message="File successfully deleted from cluster : " + fileMeta[0])
        else:
            return fileService_pb2.ack(success=False, message="Internal error")
            
    #
    #   This services is invoked when user wants to check if a file exists
    #  
    def FileSearch(self, request, context):

        if(self.fileExists(request.username, request.filename)==False):
            return fileService_pb2.ack(success=False, message="File {} does not exist.".format(request.filename))

        fileMeta = db.parseMetaData(request.username, request.filename)

        primaryIP, replicaIP = -1,-1
        channel1, channel2 = -1,-1

        if(fileMeta[0] in self.clusterLeaders): 
            primaryIP = self.clusterLeaders[fileMeta[0]]
            channel1 = self.clusterStatus.isChannelAlive(primaryIP)
            
        if(fileMeta[1] in self.clusterLeaders):
            replicaIP = self.clusterLeaders[fileMeta[1]]
            channel2 = self.clusterStatus.isChannelAlive(replicaIP)

        if(channel1 != -1):
            stub = fileService_pb2_grpc.FileserviceStub(channel1)
            response = stub.FileSearch(fileService_pb2.FileInfo(username = request.username, filename = request.filename))

        if(channel2 != -1):
            stub = fileService_pb2_grpc.FileserviceStub(channel2)
            response = stub.FileSearch(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
        
        if(response.success==True):
            return fileService_pb2.ack(success=True, message="File exists! ")
        else:
            return fileService_pb2.ack(success=False, message="File does not exist in any cluster.")

    #
    #   This services lists all files under a user
    #  
    def FileList(self, request, context):
        userFiles = db.getUserFiles(request.username)
        return fileService_pb2.FileListResponse(Filenames=str(userFiles))


def run_server(hostIP, port):
    print('Supernode started on {}:{}'.format(hostIP, port))

    #GRPC 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fileService_pb2_grpc.add_FileserviceServicer_to_server(FileServer(hostIP, port), server)
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

# ----------------------Main-------------------- #
if __name__ == '__main__':
    hostIP = "192.168.0.9"
    port = "9000"
    run_server(hostIP, port)