from node import *

class Middle(Node):

    def stream(self, data):
        
        inputData = data['Start']

        outputData = inputData + ' Hello'
        
        self.deliver(outputData) 
