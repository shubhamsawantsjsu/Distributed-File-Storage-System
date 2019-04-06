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

class RaftHelper():
   def __init__(self, hostname, server_port, raft_port, activeNodesChecker):
       self.activeNodesChecker = activeNodesChecker
       self.serverAddress = hostname + ":" + raft_port
       self.raft_port = raft_port

   def startRaftServer(self):
      db.setData("primaryStatus", 0)
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
      while True:
          time.sleep(0.005)
          if raftInstance.getCounter() != old_value:
              old_value = raftInstance.getCounter()
          if raftInstance._getLeader() is None:
              continue
          n += 1
          if n % 20 == 0:
              if True:
                  print("===================================")
                  print("Current Leader running at address:", raftInstance._isLeader())
                  print(raftInstance._getLeader())
                  self.updatePrimaryStatus(raftInstance._isLeader(), raftInstance)

   def getListOfOtherNodes(self, AllAvailableIPAddresses):
       allavailableIps = self.activeNodesChecker.getAllAvailableIPAddresses()
       raftNodes = []
       for ip in allavailableIps:
           ip, port = ip.split(":")
           raftNodes.append(ip+":"+self.raft_port)
       return raftNodes

   def updatePrimaryStatus(self, isLeader, raftInstance):
       isPrimary = int(db.get("primaryStatus"))
       if (raftInstance._getLeader() is None):
           db.setData("primaryStatus", 1)
       elif(isLeader and isPrimary==0):
           db.setData("primaryStatus", 1)
       elif(not isLeader and isPrimary==1):
           db.setData("primaryStatus", 0)