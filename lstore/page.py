import lstore.config

class Page:

	def __init__(self):
		self.num_records = 0
		self.data = bytearray(4096)

	def has_capacity(self):
		return (lstore.config.PageEntries - self.num_records) > 0

	#If return value is > -1, successful write and returns index written at. Else, need to allocate new page
	def write(self, value):
		if self.has_capacity():
			if isinstance(value, int):
				valueInBytes = value.to_bytes(8, "big")
			elif isinstance(value, str):
				valueInBytes = str.encode(value)

			self.data[self.num_records * 8 : (self.num_records + 1) * 8] = valueInBytes
			self.num_records += 1
			return self.num_records - 1
		return -1

	def read(self, index):
		result = int.from_bytes(self.data[index * 8 : (index + 1) * 8], "big")
		return result

	def inplace_update(self, index, value):
		if isinstance(value, int):
			valueInBytes = value.to_bytes(8, "big")
		elif isinstance(value, str):
			valueInBytes = str.encode(value)

		self.data[index * 8 : (index + 1) * 8] = valueInBytes
