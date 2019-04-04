import sys
sys.path.append('../generated')
sys.path.append('../utils')
import db
import time
import grpc



class ActiveNodesChecker():

    def __init__(self):
        self.channel_ip_map = {}
        self.active_ip_channel_dict = {}

    #Read IPs from Static Text file
    def readAvailableIPAddresses(self):
        print("Inside readAvailableIPAddresses")
        ip_addresses=[]
        with open('iptable.txt') as f:
                for line in f:
                    ip_addresses.append(line.split()[0])

        self.createChannelListForAvailableIPs(ip_addresses)
        db.setData("ip_addresses", self.getStringFromIPAddressesList(ip_addresses))

        while True:
            time.sleep(3)
            ip_addresses=[]

            try:
                ip_addresses_old = self.getIPAddressListFromString(db.getData("ip_addresses"))
            except:
                db.setData("ip_addresses","")

            with open('iptable.txt') as f:
                for line in f:
                    ip_addresses.append(line.split()[0])
            db.setData("ip_addresses", self.getStringFromIPAddressesList(ip_addresses))

            if(ip_addresses != ip_addresses_old):
                print("Current", ip_addresses)
                print("Old", ip_addresses_old)
                self.createChannelListForAvailableIPs(ip_addresses)
                print("Came here")
        
            self.heartBeatChecker()

            # for dic in self.active_ip_channel_dict:
            #     print(dic)

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

    def heartBeatChecker(self):

        for channel in self.channel_ip_map:
            if (self.isChannelAlive(channel)):
                if (self.channel_ip_map.get(channel) not in self.active_ip_channel_dict):
                    self.active_ip_channel_dict[self.channel_ip_map.get(channel)]=channel
            else:
                if (self.channel_ip_map.get(channel) in self.active_ip_channel_dict):
                    del self.active_ip_channel_dict[self.channel_ip_map.get(channel)]
        
    def isChannelAlive(self, channel):
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            #print("Connection timeout. Unable to connect to port ")
            return False
        return True

    def getActiveChannels(self):
        return self.active_ip_channel_dict