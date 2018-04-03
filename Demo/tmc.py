import sys
import os
import json
import time

class Builder:

    def __init__(self):
        self.structure = Structure()
        self.cluster = ''
        self.commands = {'help':self.printHelp,
                         'add cluster ':self.addCluster,
                         'add node ':self.addNode,
                         'link nodes ':self.linkNodes,
                         'use cluster ':self.useCluster}
        self.main()

    def main(self):
        print('TARMAC DINOPS ARCHITECTURE FRAMEWORK CLI /')
        print('----------------------------------------')
        print('Enter Help for commands\n')
        while True:
            command = input('\n=>')

            for i in self.commands:
                if command.startswith(i):
                   self.commands[i](command.replace(i, ''))

    def projectExists(self):
        return os.path.exists('nodes') and os.path.exists('setup')

    def parseCommand(self, command, possibs = ()):
        com = {}
        comarr = command.split(' ')
        if len(comarr) > 0:
            com['main'] = comarr.pop(0)
        if len(comarr) > 0:
            if comarr[0] in possibs:
                com['mainAttrib'] = comarr.pop(0)
        attrib = ''
        while len(comarr) > 0:
            if comarr[0][0] == '-':
                if comarr[0] not in com:
                    attrib = comarr.pop(0)
                    com[attrib] = []
                else:
                    com[attrib].append(comarr.pop(0))
            else:
                com[attrib].append(comarr.pop(0))
        return com

    def addCluster(self, command):
        com = self.parseCommand(command)
        if 'main' in com:
            name = com['main']
            if not self.projectExists():
                self.makeNewProject()
            self.makeClusterPy(name)
            address = None
            if '-a' in com:
                address = com['-a']
            self.structure.addCluster(name, address)
            self.cluster = name
            print('Created '+name+'.py')
            print('Selected '+name+' as active cluster.')

    def makeClusterPy(self, name):
        app = open('apptemplate.py').read()
        file = open(name + '.py', 'w')
        app = app.replace('${tmc|cluster-name}', name)
        file.write(app)
        file.close()

    def useCluster(self, command):
        com = self.parseCommand(command)
        if 'main' in com:
            if self.structure.hasCluster(com['main']):
                self.cluster = com['main']
                print('Selected '+name+' as active cluster.')

    def addNode(self, command):
        com = self.parseCommand(command, ('-i', '-o'))
        if 'main' in com:
            if self.cluster == '':
                print('No cluster specified.')
                return
            name = com['main']
            longName = self.cluster + '.' + name
            self.makeNodePy(name)
            if 'mainAttrib' in com:
                if com['mainAttrib'] == '-i':
                    self.structure.addInputNode(longName)
                    print('Created input node '+name+' in cluster '+self.cluster+'.')
                elif com['mainAttrib'] == '-o':
                    self.structure.addOutputNode(longName)
                    print('Created output node '+name+' in cluster '+self.cluster+'.')
            else:
                self.structure.addNode(longName)
                print('Created node '+name+' in cluster '+self.cluster+'.')
            if '-f' in com:
                for i in com['-f']:
                    if self.structure.findNode(i) != None:
                        self.structure.linkNodes(longName, self.structure.findNode(i))
                        print('Pipe built from Node '+i+' to Node'+name)
                    else:
                        print('Node '+i+' does not exist.')
            if '-t' in com:
                for i in com['-t']:
                    if self.structure.findNode(i) != None:
                        self.structure.linkNodes(self.structure.findNode(i), longName)
                        print('Pipe built from Node '+name+' to Node'+i)
                    else:
                        print('Node '+i+' does not exist.')

    def linkNodes(self, command):
        com = self.parseCommand(command)
        if 'main' in com:
            name = com['main']
            if '-f' in com:
                for i in com['-f']:
                    if self.structure.findNode(i) != None and self.structure.findNode(name) != None:
                        self.structure.linkNodes(self.structure.findNode(name), self.structure.findNode(i))
                        print('Pipe built from Node '+i+' to Node'+name)
                    else:
                        print('Inexistent node specified.')
            if '-t' in com:
                for i in com['-t']:
                    if self.structure.findNode(i) != None and self.structure.findNode(name) != None:
                        self.structure.linkNodes(self.structure.findNode(i), self.structure.findNode(name))
                        print('Pipe built from Node '+name+' to Node'+i)
                    else:
                        print('Inexistent node specified.')
                        
    def makeNodePy(self, name):
        node = open('nodetemplate.py').read()
        file = open('nodes/' + name + '.py', 'w')
        node = node.replace('${tmc|node-name}', name)
        file.write(node)
        file.close()
            

    def makeNewProject(self):
        os.popen('mkdir nodes')
        os.popen('mkdir setup')
        self.structure.saveStructure()


    def printHelp(self, command):
        pass

class Structure:

    def __init__(self):
        self.structure = {'inputNodes':[], 'outputNodes':[], 'clusters':[], 'nodes':[]}
        self.suspend = False

    def addInputNode(self, node):
        if not self.suspend:
            self.loadStructure()
        self.addNode(node)
        self.structure['inputNodes'].append(node)
        if not self.suspend:
            self.saveStructure()

    def addOutputNode(self, node):
        if not self.suspend:
            self.loadStructure()
        self.addNode(node)
        self.structure['outputNodes'].append(node)
        if not self.suspend:
            self.saveStructure()

    def findNode(self, name):
        if not self.suspend:
            self.loadStructure()
        for i in self.structure['nodes']:
            val = i['node']
            if val.count('.') > 0:
                val = i['node'][i['node'].index('.') + 1:]
            if  val == name:
                return i['node']

    def addCluster(self, name, address):
        if not self.suspend:
            self.loadStructure()
        if address == None:
            address = '127.0.0.1'
        self.structure['clusters'].append({'name':name, 'address':address})
        if not self.suspend:
            self.saveStructure()

    def addNode(self, name):
        if not self.suspend:
            self.loadStructure()
        self.structure['nodes'].append({'node':name, 'sourceNodes':[]})
        self.makeInitPy()
        if not self.suspend:
            self.saveStructure()

    def makeInitPy(self):
        file= open('nodes/__init__.py', 'w')
        string = '__all__=' + str(self.getNodeArr())
        file.write(string)
        file.close()

    def getNodeArr(self):
        return [i['node'][i['node'].index('.') + 1:] for i in self.structure['nodes']]

    def hasCluster(self, name):
        if not self.suspend:
            self.loadStructure()
        return name in [i['name'] for i in self.structure['clusters']]

    def linkNodes(self, node, sourceNode):
        if not self.suspend:
            self.loadStructure()
        allnodes = {i['node'] for i in self.structure['nodes']}
        if node in allnodes and sourceNode in allnodes:
            for i in self.structure['nodes']:
                if i['node'] == node:
                    i['sourceNodes'].append(sourceNode)
        if not self.suspend:
            self.saveStructure()

    def saveStructure(self):
        j = json.JSONEncoder()
        structureJson = j.encode(self.structure)
        if not os.path.exists('setup'):
            time.sleep(0.5)
        file = open('setup/structure.json','w')
        file.write(structureJson)
        file.close()

    def loadStructure(self):
        if not os.path.exists('setup'):
            time.sleep(0.5)
        self.structure = json.load(open('setup/structure.json'))
        

Builder()

        
