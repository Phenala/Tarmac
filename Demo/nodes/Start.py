from node import *

class Start(Node):

    def stream(self, data):
        
        inputData = data['Default']

        outputData = inputData + ' Morning'
        
        self.deliver(outputData) 
