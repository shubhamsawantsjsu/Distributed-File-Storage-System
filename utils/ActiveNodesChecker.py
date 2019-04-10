import sys
sys.path.append('../generated')
sys.path.append('../utils')
import db
import time
import grpc

#
#   *** ActiveNodesChecker Utility : Helper class to keep track of active nodes. ***
#
class ActiveNodesChecker():

    def __init__(self):
        self.channel_ip_map = {}
        self.active_ip_channel_dict = {}

    #
    #  A thread will start for this method. This method keeps updating the active_ip_channel_dict map.
    #
    def readAvailableIPAddresses(self):
        print("Inside readAvailableIPAddresses")

        # Read all the available IP addresses from iptable.txt
        ip_addresses = self.getAllAvailableIPAddresses()

        # Create channels with all the IP addresses
        self.createChannelListForAvailableIPs(ip_addresses)
        db.setData("ip_addresses", self.getStringFromIPAddressesList(ip_addresses))

        while True:
            time.sleep(0.5)
            ip_addresses=[]

            try:
                ip_addresses_old = self.getIPAddressListFromString(db.getData("ip_addresses"))
            except:
                db.setData("ip_addresses","")

            ip_addresses = self.getAllAvailableIPAddresses()
            
            db.setData("ip_addresses", self.getStringFromIPAddressesList(ip_addresses))

            # If there is any addition or deletion of node then create a new channel for that and update {channel, ip} map.
            if(ip_addresses != ip_addresses_old):
                self.createChannelListForAvailableIPs(ip_addresses)

            # Update the active {IP, channel} map
            self.heartBeatChecker()

    # This method return a list of IP Addresses present in iptable.txt
    def getAllAvailableIPAddresses(self):
        ip_addresses=[]
        with open('iptable.txt') as f:
            for line in f:
                ip_addresses.append(line.split()[0])
        return ip_addresses

    def getIPAddressListFromString(self, ipAddresses):
        result = []
        if ipAddresses=="":  return result
        return ipAddresses.split(',')
    
    def getStringFromIPAddressesList(self, ipAddressList):
        ipAddressString = ""
        for ipAddress in ipAddressList:
            ipAddressString+=ipAddress+","
        ipAddressString = ipAddressString[:-1]
        return ipAddressString

    #Create Channel:IP HashMap
    def createChannelListForAvailableIPs(self, ip_addresses):
        self.channel_ip_map = {}
        for ip_address in ip_addresses:
            channel = grpc.insecure_channel('{}'.format(ip_address))
            self.channel_ip_map[channel]=ip_address

    # This method keeps updating the active channels based on their aliveness.
    # It removes the channels from the list if the node is down.
    def heartBeatChecker(self):
        for channel in self.channel_ip_map:
            if (self.isChannelAlive(channel)):
                if (self.channel_ip_map.get(channel) not in self.active_ip_channel_dict):
                    self.active_ip_channel_dict[self.channel_ip_map.get(channel)]=channel
            else:
                if (self.channel_ip_map.get(channel) in self.active_ip_channel_dict):
                    del self.active_ip_channel_dict[self.channel_ip_map.get(channel)]
    
    # This method checks whether the channel is alive or not.
    def isChannelAlive(self, channel):
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            return False
        return True

    # This method returns a map of active {ip, channel}
    def getActiveChannels(self):
        return self.active_ip_channel_dict