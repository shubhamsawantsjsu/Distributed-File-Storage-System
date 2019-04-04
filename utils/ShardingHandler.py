from concurrent import futures

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

class ShardingHandler():
    def __init__(self, activeNodesChecker):
        self.active_ip_channel_dict = activeNodesChecker.getActiveChannels()

    def leastUtilizedNode(self):
        #print("Inside leastUtilizedNode method")
        #return "localhost:4000"
        return self.leastUtilizedNodeHelper()
    
    def leastUtilizedNodeHelper(self):
        minVal = 301.00
        leastLoadedNode = ""
        for ip, channel in self.active_ip_channel_dict.items():
            if(self.isChannelAlive(channel)):
                stub = heartbeat_pb2_grpc.HearBeatStub(channel)
                stats = stub.isAlive(heartbeat_pb2.NodeInfo(ip="", port=""))
                total = float(stats.cpu_usage) + float(stats.disk_space) + float(stats.used_mem)
                if ((total/3)<minVal):
                    minVal = total/3
                    leastLoadedNode = ip
        if(leastLoadedNode==""):
            return -1
        return leastLoadedNode

    def isChannelAlive(self, channel):
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            return False
        return True