from node import *

class AlternateMiddle(Node):

    def stream(self, data):
        
        inputData = data['Start']

        outputData = inputData + ' GoodBye'
        
        self.deliver(outputData) 

