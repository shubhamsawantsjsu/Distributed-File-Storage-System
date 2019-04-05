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
from Raft import Raft

class RaftHelper():
    def __init__(self, hostname, server_port, raft_port, activeNodesChecker):
        self.activeNodesChecker = activeNodesChecker
        self.serverAddress = hostname + ":" + raft_port
        self.raft_port = raft_port

    def startRaftServer(self):
        print("Inside startRaftServer")
        time.sleep(4)
        print("------------------------------Starting Raft Server-------------------------------------")
        otherNodes = self.getListOfOtherNodes(self.activeNodesChecker.getAllAvailableIPAddresses())

        for node in otherNodes:
            print(node)
        
        print("Done printing")
        #print("ServerAddress is :", self.serverAddress)
    
        raftInstance = Raft(self.serverAddress, otherNodes) #self.activeNodesChecker.getAllAvailableIPAddresses())
        print("Raft utility has been started")
        while(True):
            time.sleep(1)
            print("current_leader is running at address", raftInstance._getLeader())
            self.updatePrimaryStatus(raftInstance._getLeader())

    def getListOfOtherNodes(self, AllAvailableIPAddresses):
        allavailableIps = self.activeNodesChecker.getAllAvailableIPAddresses()
        raftNodes = []
        for ip in allavailableIps:
            ip, port = ip.split(':')
            raftNodes.append(ip+":"+self.raft_port)
        return raftNodes
    
    def updatePrimaryStatus(self, leaderIPAddress):
        isPrimary = db.get("primaryStatus")
        if(self.serverAddress==leaderIPAddress and isPrimary==0):
            db.setData("primaryStatus", 1)
        elif(self.serverAddress!=leaderIPAddress and isPrimary==1):
            db.setData("primaryStatus", 0)





        

