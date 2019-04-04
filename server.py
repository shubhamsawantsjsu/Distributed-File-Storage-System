from concurrent import futures
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

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

def run_server(server_port, primary):
    if(primary==1): print("This server is the current leader.")
    print('gRPC Port:{}'.format(server_port))
   
    activeNodesChecker = ActiveNodesChecker()
    shardingHandler = ShardingHandler(activeNodesChecker)

    #GRPC - File Service + heartbeat service
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fileService_pb2_grpc.add_FileserviceServicer_to_server(FileServer(primary, server_port, activeNodesChecker, shardingHandler), server)
    heartbeat_pb2_grpc.add_HearBeatServicer_to_server(HeartbeatService.Heartbeat(primary), server)
    server.add_insecure_port('[::]:{}'.format(server_port))
    server.start()

    ## Monitor the iptable.txt for node changes and keep track of active channels ##
    threading.Thread(target=ActiveNodesChecker.readAvailableIPAddresses(activeNodesChecker), daemon=True).start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

# ----------------------Main-------------------- #
if __name__ == '__main__':
    # config_dict = yaml.load(open('config.yaml'), Loader=yaml.FullLoader) #yaml.load(open('config.yaml'))
    config_dict = yaml.load(open('config.yaml'))
    if(len(sys.argv)<2):
        print("Usage python3 server.py <<server No>>")
        print("Enter one, two or three for server No.")
        exit()
    config_dict = config_dict[str(sys.argv[1]).lower()]

    server_port = str(config_dict['server_port'])
    primary = config_dict['primary']
    run_server(server_port, primary)