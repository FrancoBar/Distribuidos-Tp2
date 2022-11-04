UINT32_SIZE = 4

def serialize_uint32(u):
	return u.to_bytes(UINT32_SIZE, 'big')

def deserialize_unsigned_number(b):
	return int.from_bytes(b, byteorder='big', signed=False)

def serialize_str(s):
	return bytes(s, encoding='utf-8')

def deserialize_str(b):
	return b.decode('utf-8')