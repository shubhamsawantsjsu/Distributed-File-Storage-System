from concurrent import futures

import grpc
import sys
sys.path.append("../generated")
sys.path.append("../utils")
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
from pysyncobj import SyncObj, replicated

#
#   Raft Utility : Helper class to start the raft service.
#
class RaftHelper():
    def __init__(self, hostname, server_port, raft_port, activeNodesChecker, superNodeAddress):
        self.activeNodesChecker = activeNodesChecker
        self.serverAddress = hostname + ":" + raft_port
        self.raft_port = raft_port
        self.superNodeAddress = superNodeAddress
        self.hostname = hostname
        self.serverPort = server_port

    #
    #  A thread will start for this method. This method keeps updating the primaryStatus field in db
    #  whenever leader goes down.
    #  Also this method is responisble for sending the newly elected leader info to SuperNode.
    #
    def startRaftServer(self):
        time.sleep(4)
        print("------------------------------Starting Raft Server-------------------------------------")
        otherNodes = self.getListOfOtherNodes(self.activeNodesChecker.getAllAvailableIPAddresses())

        for node in otherNodes:
            print(node)

        otherNodes.remove(self.serverAddress)

        raftInstance = Raft(self.serverAddress, otherNodes)
        print("Raft utility has been started")

        n = 0
        old_value = -1
        isLeaderUpdated = False
        
        while True:
            time.sleep(0.5)
            if raftInstance.getCounter() != old_value:
                old_value = raftInstance.getCounter()
            if raftInstance._getLeader() is None:
                #print(len(self.activeNodesChecker.getActiveChannels()))
                if(not isLeaderUpdated and len(self.activeNodesChecker.getActiveChannels())==1):
                    print("Since the leader is None, hence declaring myself the leader:", self.serverAddress)
                    db.setData("primaryStatus", 1)
                    self.sendLeaderInfoToSuperNode()
                    isLeaderUpdated = True
                continue
            n += 1
            if n % 20 == 0:
                if True:
                    print("===================================")
                    print("Am I the leader?", raftInstance._isLeader())
                    print("Current Leader running at address:", raftInstance._getLeader())
                    self.updatePrimaryStatus(raftInstance._isLeader(), raftInstance)

    def getListOfOtherNodes(self, AllAvailableIPAddresses):
        allavailableIps = self.activeNodesChecker.getAllAvailableIPAddresses()
        raftNodes = []
        for ip in allavailableIps:
            ip, port = ip.split(":")
            raftNodes.append(ip+":"+self.raft_port)
        return raftNodes

    # Method to update the primaryStatus flag in db and also to send newly elected leader info to supernode
    def updatePrimaryStatus(self, isLeader, raftInstance):
        isPrimary = int(db.get("primaryStatus"))

        if(isPrimary==1):
            self.sendLeaderInfoToSuperNode()

        if (raftInstance._getLeader() is None):
            db.setData("primaryStatus", 1)
            self.sendLeaderInfoToSuperNode()
        elif(isLeader and isPrimary==0):
            db.setData("primaryStatus", 1)
            self.sendLeaderInfoToSuperNode()
        elif(not isLeader and isPrimary==1):
            db.setData("primaryStatus", 0)

    # Method to send newly elected leader info to supernode
    def sendLeaderInfoToSuperNode(self):        
        try:
            channel = grpc.insecure_channel('{}'.format(self.superNodeAddress))
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            response = stub.getLeaderInfo(fileService_pb2.ClusterInfo(ip = self.hostname, port= self.serverPort, clusterName="team1"))
            print(response.message)
        except:
            print("Not able to connect to supernode")
            pass

        
        


