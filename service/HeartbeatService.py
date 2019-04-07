from __future__ import print_function
import psutil
import grpc
import sys
sys.path.append('../generated')
sys.path.append('../utils')
import heartbeat_pb2
import heartbeat_pb2_grpc
import db

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

#
#   ***HeartBeat Service : HeartBeat service as per heartbeat.proto file.***
#   
class Heartbeat(heartbeat_pb2_grpc.HearBeatServicer):
    def __init__(self):
        self.primary = int(db.get("primaryStatus"))

    #
    #   ***Returns the Machine stats only if the node is alive.***
    #   Each time request comes to leaderNode, it will ask all the other nodes in the cluster to send 
    #   response(CPU stats) through isAlive method.
    #
    def isAlive(self, request, context):
        cpu_usage = str(psutil.cpu_percent())
        disk_space = str(psutil.virtual_memory()[2])
        used_mem = str(psutil.disk_usage('/')[3])
        stats = heartbeat_pb2.Stats(cpu_usage = cpu_usage, disk_space = disk_space, used_mem = used_mem)
        return stats

    def getCPUusage(self):
        print('CPU % used', psutil.cpu_percent())
        print('physical memory % used:', psutil.virtual_memory()[2])
        print('Secondary memory % used', psutil.disk_usage('/')[3])
    


        
