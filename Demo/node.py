import time
import socket
import json
import threading


class ClusterPipe:

    def __init__(self, clusterName, address, port):
        self.clusterName = clusterName
        self.address = address
        self.sendQueue = []
        self.server = False
        self.connected = False
        self.link = {}
        self.thisCluster = ''
        self.port = port
        self.inputNodePipes = {}
        self.outputNodePipes = {}
        self.bufferSize = 1024
        self.bufferExponent = 30
        self.thread = ClusterThread()
        self.thread.giveThreadData(self)
        self.sendThread = ClusterSendThread()
        self.sendThread.giveThreadData(self)
        self.source = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def runSyncThread(self):
        if self.thisCluster == self.clusterName:
            
            print('Created source cluster.\n',end='')
            self.createSource()
        else:
            self.createSatellite()
            print('Created satellite cluster.\n',end='')
        self.thread.start()
        self.sendThread.start()

    def transferData(self, nodePipe, data):
        sendData = {}
        if nodePipe.outputName in self.outputNodePipes:            
            sendData[nodePipe.outputName] = data
            self.sendQueue.append(sendData)

    def receiveData(self, data):
        for i in data:
            self.inputNodePipes[i].putData(data[i])

    def registerInputNodePipe(self, nodePipe):
        self.inputNodePipes[nodePipe.outputName] = nodePipe

    def registerOutputNodePipe(self, nodePipe):
        self.outputNodePipes[nodePipe.outputName] = nodePipe

    def createSource(self):
        self.source.bind(('', self.port))
        self.source.listen()
        self.server = True
        print('Waiting for clusters to connect...')
        self.conn, self.addr = self.source.accept()
        print('Found satellite cluster at '+str(self.addr[0])+':'+str(self.addr[1])+'\n',end='')
        self.connected = True

    def createSatellite(self):
        while self.connected == False:
            try:
                print('Connecting to Source...')
                self.source.connect((self.address, self.port))
                print('Found source cluster at ' + str(self.address) + ':' + str(self.port)+'\n',end='')
                self.connected = True
            except socket.error:
                pass


class ClusterThread(threading.Thread):

    def giveThreadData(self, clusterPipe):
        self.clusterPipe = clusterPipe
        self.decoder = json.JSONDecoder()

    def run(self):
        while True:
            if self.clusterPipe.server:
                recvDataJSON = ''
                bufferLength = int(self.clusterPipe.conn.recv(self.clusterPipe.bufferExponent))
                recvDataJSON = self.clusterPipe.conn.recv(bufferLength).decode()
                self.clusterPipe.receiveData(self.decoder.decode(recvDataJSON))
            else:
                recvDataJSON = ''
                bufferLength = int(self.clusterPipe.source.recv(self.clusterPipe.bufferExponent))
                recvDataJSON = self.clusterPipe.source.recv(bufferLength).decode()
                self.clusterPipe.receiveData(self.decoder.decode(recvDataJSON))
            
class ClusterSendThread(threading.Thread):

    def giveThreadData(self, clusterPipe):
        self.clusterPipe = clusterPipe
        self.wait = False
        self.encoder = json.JSONEncoder()

    def run(self):
        while True:
            if self.wait:
                time.sleep(0.5)
            self.wait = (len(self.clusterPipe.sendQueue) == 0)
            if not self.wait:
                sendData = self.clusterPipe.sendQueue.pop(0)
                sendDataEncoded = self.encoder.encode(sendData)
                bufferString = format(len(sendDataEncoded), str(self.clusterPipe.bufferExponent))
                streamData = (bufferString + sendDataEncoded).encode()
                if self.clusterPipe.server:
                    self.clusterPipe.conn.sendto(streamData, self.clusterPipe.addr)
                else:
                    self.clusterPipe.source.send(streamData)
        


class NodePipe:

    def __init__(self, name = 'Default', cluster = 'Source'):
        self.data = []
        self.cluster = cluster
        self.outputName = name
        self.notifies = []
        self.takerScore = {}

    def putData(self, data):
        self.data.append(data)
        for i in self.notifies:
            i(self, data)

    def addTaker(self, taker):
        self.takerScore[taker] = 0

    def addChangeListener(self, listener):
        self.notifies.append(listener)

    def getName(self):
        return self.cluster + '.' + self.outputName

    def getData(self, taker = None):
        if taker == None:
            return self.data.pop(0)
        val = self.data[self.takerScore[taker]]
        self.takerScore[taker] += 1
        self.checkTakers()
        return val

    def checkTakers(self):
        maxscore = 100000
        for i in self.takerScore:
            maxscore = min(self.takerScore[i], maxscore)
        for i in self.takerScore:
            self.takerScore[i] -= maxscore
        while maxscore > 0:
            self.data.pop(0)
            maxscore -= 1

    def hasData(self, taker = None):
        if taker == None:
            return len(self.data) != 0

        return len(self.data) > self.takerScore[taker]

class Node:


    def __init__(self, app, name, cluster):
        
        self.inputPipes = {}
        self.callback = None
        self.outputCallback = None
        self.suspended = False
        self.cluster = cluster
        self.name = name
        self.wait = False
        self.outputPipe = NodePipe(name, cluster)
        self.app = app

    def addInputPipe(self, pipe):
        self.inputPipes[pipe.outputName] = pipe
        print('Node Pipe laid from',pipe.outputName,'to',self.name)
        pipe.addChangeListener(self.killSuspension)
        pipe.addTaker(self)

    def setInputPipe(self, pipe):
        self.inputPipes[pipe.outputName] = pipe
        pipe.addChangeListener(self.killSuspension)
        print('Node Pipe laid from',pipe.outputName,'to',self.name)
        pipe.addTaker(self)

    def canProcess(self):
        can = True
        for i in self.inputPipes:
            can = can and self.inputPipes[i].hasData(self)
        return can

    def killSuspension(self, nodePipe, data):
        self.suspended = False

    def feed(self):
        while True:
            if (self.canProcess()):
                streamData = {i:self.inputPipes[i].getData(self) for i in self.inputPipes} 
                self.stream(streamData)
                if (self.callback != None):
                    self.callback(streamData)
                if (self.outputCallback != None):
                    self.outputCallback(streamData)
            else:
                time.sleep(0.5)

    def deliver(self, data):
        self.outputPipe.putData(data)

    def stream(self, data):
        pass
