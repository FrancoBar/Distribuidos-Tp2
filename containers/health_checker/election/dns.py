BASE_PORT = 4321

def id_to_addr(id):
	return ("health_checker_" + str(id), BASE_PORT + id)

def addr_to_id(addr):
	return int(addr[1]) - BASE_PORT