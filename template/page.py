from template.config import *

class PageDirectory:
	def __init__(self):
		self.entries = []w

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
    
    #TODO: done??
    def has_capacity(self):
        return PageEntries - num_records > 0
    
    def write(self, value):
        self.num_records += 1
        pass