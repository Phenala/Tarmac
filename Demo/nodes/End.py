from node import *
import random

class End(Node):

    def stream(self, data):
        
        inputData = random.choice([data['Middle'], data['AlternateMiddle']])

        outputData = inputData + ' World'
        
        self.deliver(outputData) 
