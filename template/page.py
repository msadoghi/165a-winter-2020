# from template.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
    
    def has_capacity(self):
        return 64 - self.num_records > 0
    
    def write(self, value):
    	if self.has_capacity():
        	valueInBytes = value.to_bytes(8, "big")
        	self.data[self.num_records * 8 : (self.num_records + 1) * 8] = valueInBytes
        	self.num_records += 1