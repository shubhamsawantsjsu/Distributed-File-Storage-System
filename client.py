from concurrent import futures

import sys      #pip3 install sys
sys.path.append('./generated')
sys.path.append('./proto')
sys.path.append('./utils')
import grpc
import fileService_pb2_grpc
import fileService_pb2
import heartbeat_pb2_grpc
import heartbeat_pb2
import sys
import time
import yaml
import threading
import os

def getFileData():
    #fileName = 'fileToBeUploaded.img'
    fileName = input("Enter filename:")
    outfile = os.path.join('files', fileName)
    file_data = open(outfile, 'rb').read()
    fileData = fileService_pb2.FileData(fileName=fileName, data=file_data)
    return fileData

def getFileChunks():
     # Maximum chunk size that can be sent
    CHUNK_SIZE=4000000

    # Location of source image
    username = input("Enter Username: ")
    fileName = input("Enter filename: ")

    # This file is for dev purposes. Each line is one piece of the message being sent individually
    outfile = os.path.join('files', fileName)
    
    sTime=time.time()
    with open(outfile, 'rb') as infile:
        while True:
            chunk = infile.read(CHUNK_SIZE)
            if not chunk: break

            # Do what you want with each chunk (in dev, write line to file)
            yield fileService_pb2.FileData(username=username, filename=fileName, data=chunk, seqNo=1)
    print("Time for upload= ", time.time()-sTime)


def downloadTheFile(stub):
    userName = input("Enter Username: ")
    fileName = input("Enter file name: ")
    data = bytes("",'utf-8')
    sTime=time.time()
    responses = stub.DownloadFile(fileService_pb2.FileInfo(username=userName, filename=fileName))
    #print(responses)
    for response in responses:
        fileName = response.filename
        data += response.data
    
    print("Time for Download = ", time.time()-sTime)
    filePath=os.path.join('downloads', fileName)
    saveFile = open(filePath, 'wb')
    saveFile.write(data)
    saveFile.close()
    
    print("File Downloaded - ", fileName)


def uploadTheFileChunks(stub):
    #fileData = getFileData()
    
    response = stub.UploadFile(getFileChunks())
    if(response.success): print("File successfully Uploaded")
    else:
        print("Failed to upload. Message - ", response.message)

def handleUserInputs(stub):
    print("1. Download a file.")
    print("2. Upload a file")
    option = input("Please choose an option.")

    if(option=='1'):
        downloadTheFile(stub)
    elif(option=='2'):
        uploadTheFileChunks(stub)

def run_client(serverAddress):
    with grpc.insecure_channel(serverAddress) as channel:
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            print("Connection timeout. Unable to connect to port ")
            exit()
        else:
            print("Connected")
        stub = fileService_pb2_grpc.FileserviceStub(channel)
        print("Stub--->", stub)
        handleUserInputs(stub)


if __name__ == '__main__':
    # config_dict = yaml.load(open('config.yaml'))
    # if(len(sys.argv)<2):
    #     print("Usage python3 server.py <<server No>>")
    #     print("Enter one, two or Three for server No.")
    #     exit()
    # config_dict = config_dict[str(sys.argv[1]).lower()]

    # server_port = str(config_dict['server_port'])
    # #connection_ports = config_dict['connection_port']
    # rest_server_port = config_dict['rest_server_port']
    # primary = config_dict['primary']
    # writeLog=[]
    run_client('192.168.0.6:3000')