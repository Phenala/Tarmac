import tarmac
import random

class SecondaryCluster(tarmac.Tarmac):

    def getCluster(self):
        #Set this value as the cluster you want this device to run
        return 'Secondary'

    def go(self):
        pass

SecondaryCluster().go()
