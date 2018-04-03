import tarmac
import random

class ${tmc|cluster-name}Cluster(tarmac.Tarmac):

    def getCluster(self):
        #Set this value as the cluster you want this device to run
        return '${tmc|cluster-name}'

    def go(self):
        #streams the value 10 to the node system. Override this method with your process
        while True:

            # the values ${tmc|cluster-name}.Input and ${tmc|cluster-name}.Output should be
            # replaced by input and output nodes you created
            self.stream('Input', 10)
            if (self.hasOutputData('Output')):
                print(self.getOutputData('Output'))

${tmc|cluster-name}Cluster().go()
