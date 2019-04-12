from concurrent import futures
import grpc
import sys
import db
sys.path.append('./proto')
import fileService_pb2_grpc
import fileService_pb2
import time
import threading

class ClusterStatus():
    def leastUtilizedNode(self, clusterList):
        #print("In leastUtilizedNode")
        minVal, minVal2 = 301.00, 301.00
        leastLoadedNode, leastLoadedNode2 = "",""
        clusterName, clusterName2 = "", ""
        #print("Checking clusterList-{}", clusterList)
        for cluster in clusterList:
            channel = self.isChannelAlive(clusterList[cluster])
            #print("After checking channel alive, channel=",channel)
            if(channel):
                stub = fileService_pb2_grpc.FileserviceStub(channel)
                stats = stub.getClusterStats(fileService_pb2.Empty())
                #print("stats=",stats)
                total = 300.00 - (float(stats.cpu_usage) + float(stats.disk_space) + float(stats.used_mem))
                if ((total/3)<minVal):
                   minVal2 = minVal
                   minVal = total/3
                   leastLoadedNode2 = leastLoadedNode
                   clusterName2 = clusterName
                   leastLoadedNode = clusterList[cluster]
                   clusterName = cluster
                elif((total/3)<minVal2):
                   minVal2 = total/3
                   leastLoadedNode2 = clusterList[cluster]
                   clusterName2 = cluster

        if(leastLoadedNode==""):
            return -1,-1,-1,-1
        return leastLoadedNode, leastLoadedNode2, clusterName, clusterName2

    def isChannelAlive(self, ip_address):
        #print("In isChannelAlive")
        try:
            channel = grpc.insecure_channel('{}'.format(ip_address))
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            return False
        return channel