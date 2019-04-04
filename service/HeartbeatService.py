from __future__ import print_function
import psutil
import grpc
import sys
sys.path.append('../generated')
import heartbeat_pb2
import heartbeat_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class Heartbeat(heartbeat_pb2_grpc.HearBeatServicer):
    def __init__(self, primary):
        self.primary = primary

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
    


        
