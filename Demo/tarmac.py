import json
import threading
from node import *
from nodes import *

class NodeManager:


    def __init__(self, app):
        self.nodes = {}
        self.threads = []
        self.inputNodes = {}
        self.outputNodes = {}
        self.dataFromClusters = {}
        self.app = app
        self.portNumber = 44444 
        self.setCluster(self.app.getCluster())
        self.getNodeSetup()
        self.start()

    def stream(self, string, data):
        self.inputNodes[string].inputPipes['Default'].putData(data)
        
    def hasOutputData(self, string):
        return self.outputNodes[string].outputPipe.hasData()

    def getOutputData(self, string):
        return self.outputNodes[string].outputPipe.getData()

    def setOutputCallBack(self, string, function):
        self.outputNodes[string].outputCallback = function

    def compileNodeLibrary(self):
        compiled = open('setup/compiled.py', 'w')
        writeString = ''
        for i in self.nodeList:
            i = F.gn(i[0])
            writeString += open('nodes/' + i + '.py').read() + '\n'
        compiled.write(writeString)
        compiled.close()
        comp = __import__('nodes')
        for i in self.nodeList:
            j = F.gc(i[0])
            k = F.gn(i[0])
            if self.isCluster(j):
                self.nodes[i[0]] = eval('comp.' + k + '.' + k + '(self.app, "' + k +'", "' + j +'")')

    def setCluster(self, name):
        globals()['ClusterName'] = name
        self.cluster = name

    def isCluster(self, name):
        return name == self.cluster

    def generatePortNumber(self):
        return self.portNumber

    def getHigherOrder(self, cluster1, cluster2):
        for i in range(len(self.clusters)):
            if self.clusters[i][0] == cluster1:
                return cluster1
            elif self.clusters[i][0] == cluster2:
                return cluster2
            
    def makeClusterLinks(self):
        self.links = []
        for i in self.allNodeList:
            for j in i[1]:
                b = F.gc(i[0])
                j = F.gc(j)
                if (not {b, j} in self.links) and (b.startswith(self.cluster) or j.startswith(self.cluster)) and (b != j):
                    self.links.append({b,j})
        port = self.generatePortNumber()
        self.clusterLinks = []
        for i in self.links:
            ilist = list(i)
            server = self.getHigherOrder(ilist[0], ilist[1])
            address = ''
            for j in self.clusters:
                if j[0] == server:
                    address = j[1]
            nClusterPipe = ClusterPipe(server, address, port)
            self.clusterLinks.append(nClusterPipe)
            nClusterPipe.link = i.copy()
            nClusterPipe.thisCluster = self.cluster

    def getNodeSetup(self):
        self.structure = json.load(open('setup/structure.json'))
        self.allNodeList = [[i.get('node'), i.get('sourceNodes')] for i in self.structure.get('nodes')]
        self.clusters = [[i.get('name'),i.get('address')] for i in self.structure.get('clusters')]
        
        self.nodeList = []
        for i in self.allNodeList:
            if i[0].startswith(self.cluster + '.'):
                self.nodeList.append(i.copy())
                
        self.compileNodeLibrary()

        self.makeClusterLinks()
        
        for i in self.allNodeList:
            node = i[0][i[0].index('.') + 1:]
            cluster = i[0][:i[0].index('.')]
            for j in i[1]:
                sourceNode = j[j.index('.') + 1:]
                sourceCluster = j[:j.index('.')]
                if self.cluster == cluster == sourceCluster:
                    self.nodes[i[0]].addInputPipe(self.nodes[j].outputPipe)
                elif self.cluster == cluster:
                    nPipe = NodePipe(sourceNode, sourceCluster)
                    self.nodes[i[0]].addInputPipe(nPipe)
                    for k in self.clusterLinks:
                        if k.link == {F.gc(i[0]), F.gc(j)}:
                            k.registerInputNodePipe(nPipe)
                            nPipe.addChangeListener(k.transferData)
                elif self.cluster == sourceCluster:
                    for k in self.clusterLinks:
                        if k.link == {F.gc(i[0]), F.gc(j)}:
                            k.registerOutputNodePipe(self.nodes[j].outputPipe)
                            self.nodes[j].outputPipe.addChangeListener(k.transferData)
                
        for i in self.structure.get('inputNodes'):
            if self.isCluster(F.gc(i)):
                self.inputNodes[F.gn(i)] = self.nodes[i]
                self.inputNodes[F.gn(i)].setInputPipe(NodePipe())
        for i in self.structure.get('outputNodes'):
            if self.isCluster(F.gc(i)):
                self.outputNodes[F.gn(i)] = self.nodes[i]


    def setNodeCallBack(self, string, function):
        self.nodes[string].callback = function

    def start(self):
        for i in self.nodes:
            nThread = NodeThread()
            nThread.setNode(self.nodes[i])
            nThread.start()
        for i in self.clusterLinks:
            i.runSyncThread()


class NodeThread(threading.Thread):

    def setNode(self, node):
        self.node = node

    def run(self):
        self.node.feed()
        print('Starting node thread ' + self.node.name + ' ...\n', end='')

        

class Tarmac:

    def __init__(self):
        nodeManager = NodeManager(self)
        self.stream = nodeManager.stream
        self.setNodeCallBack = nodeManager.setNodeCallBack
        self.setOutputCallBack = nodeManager.setOutputCallBack
        self.hasOutputData = nodeManager.hasOutputData
        self.getOutputData = nodeManager.getOutputData
        self.setCluster = nodeManager.setCluster


class F:

    def gc(string):
        return string[:string.index('.')]

    def gn(string):
        return string[string.index('.') + 1:]
        
