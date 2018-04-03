import tarmac
import random

class MainAppCluster(tarmac.Tarmac):

    def getCluster(self):
        #Set this value as the cluster you want this device to run
        return 'MainApp'

    def go(self):
        #streams the value 10 to the node system. Override this method with your process
        while True:

            # the values MainApp.Input and MainApp.Output should be
            # replaced by input and output nodes you created
            self.stream('Start', 'Good')
            if (self.hasOutputData('End')):
                print(self.getOutputData('End'))

MainAppCluster().go()
