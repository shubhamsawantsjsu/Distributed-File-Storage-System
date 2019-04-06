from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from threading import Thread
import grpc
import sys      #pip3 install sys
sys.path.append('./generated')
sys.path.append('./utils')
sys.path.append('./service')
sys.path.append('./proto')
import db
import fileService_pb2_grpc
import fileService_pb2
import heartbeat_pb2_grpc
import heartbeat_pb2
import time
import yaml
import threading
import hashlib
import HeartbeatService
from ActiveNodesChecker import ActiveNodesChecker
from ShardingHandler import ShardingHandler
from FileServer import FileServer
from Raft import Raft
from RaftHelper import RaftHelper

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

def run_server(hostname, server_port, raft_port, super_node_address):
    if(int(db.get("primaryStatus"))==1): print("This server is the current leader.")
    print('gRPC Port:{}'.format(server_port))
   
    activeNodesChecker = ActiveNodesChecker()
    shardingHandler = ShardingHandler(activeNodesChecker)
    raftHelper = RaftHelper(hostname, server_port, raft_port, activeNodesChecker)

    #GRPC - File Service + heartbeat service
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fileService_pb2_grpc.add_FileserviceServicer_to_server(FileServer(hostname, server_port, activeNodesChecker, shardingHandler, super_node_address), server)
    heartbeat_pb2_grpc.add_HearBeatServicer_to_server(HeartbeatService.Heartbeat(), server)
    server.add_insecure_port('[::]:{}'.format(server_port))
    server.start()

    print("Starting raft")    

    t1 = Thread(target=RaftHelper.startRaftServer, args=(raftHelper,))
    t2 = Thread(target=ActiveNodesChecker.readAvailableIPAddresses, args=(activeNodesChecker,))

    t2.start()
    t1.start()

    print("Both threads have been started")

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

# ----------------------Main-------------------- #
if __name__ == '__main__':
    # config_dict = yaml.load(open('config.yaml'), Loader=yaml.FullLoader) #yaml.load(open('config.yaml'))
    config_dict_orig = yaml.load(open('config.yaml'))
    if(len(sys.argv)<2):
        print("Usage python3 server.py <<server No>>")
        print("Enter one, two or three for server No.")
        exit()
    config_dict = config_dict_orig[str(sys.argv[1]).lower()]
    server_host = config_dict['hostname']
    server_port = str(config_dict['server_port'])
    primary = int(db.get("primaryStatus")) #config_dict['primary']
    raft_port = str(config_dict['raft_port'])
    super_node_address = config_dict_orig['super_node_address']
    run_server(server_host, server_port, raft_port, super_node_address)