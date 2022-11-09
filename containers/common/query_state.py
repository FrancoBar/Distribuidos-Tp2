import os

TABLE_FILE = 'table.txt'
QUERY_TYPE = '.csv'

def _default_serialize_value(value):
    return str(value)

def _default_deserialize_value(value_row):
    return value_row[2]

class QueryState:
    def __init__(self, storage,
        serialize_value=_default_serialize_value,
        deserialize_value=_default_deserialize_value):

        self.storage = storage
        if not os.path.exists(storage):
            os.makedirs(storage)

        self.id = 0
        self.message_table = {}
        self.queries = {}
        self.serialize_value = serialize_value
        self.deserialize_value = deserialize_value
        self._build_queries()
        #Todo: Reduce table size after a number of entries
        self._build_table()

    def _build_queries(self):
        #BORRAR: ver si en vez de hacer lo del substring usamos un includes QUERY_TYPE, asi
        # si despeues cambiamos el tipo de archivo no rompe por olvidarnos de cambiar el -4 
        query_file_list = list(filter(lambda file_name : file_name[-4:] == QUERY_TYPE, os.listdir(self.storage)))
        for query_id in query_file_list:
            prev_size = 0
            current_size = 0
            with open(self.storage + query_id, 'r+') as csvfile:
                self.queries[query_id[:-4]] = {}
                pending_key = None
                pending_value = None
                while True:
                    line = csvfile.readline()
                    if line == '':
                        if pending_key:
                            csvfile.truncate(prev_size)
                        break
                    if '\n' not in line:
                        csvfile.truncate(prev_size if pending_key else current_size)
                        break
                    prev_size = current_size
                    current_size = csvfile.tell()
                    row = line[:-1].split(',')
                    if row[0] == 'c' and pending_key:
                        self.queries[query_id[:-4]][pending_key] = pending_value
                        pending_key = None
                        pending_value = None
                    elif row[0] == 'w':
                        pending_key = row[1]
                        pending_value = self.deserialize_value(row)

    def _build_table(self):
        prev_size = 0
        current_size = 0
        try:
            with open(self.storage + TABLE_FILE, 'r+') as tblfile:
                while True:
                    line = tblfile.readline()
                    if line == '':
                        break
                    if '\n' not in line:
                        tblfile.truncate(current_size)
                        break
                    prev_size = current_size
                    current_size = tblfile.tell()
                    row = line[:-1].split(',')
                    self.message_table[int(row[0])] = int(row[1])
                    self.id = int(row[2]) + 1
        except FileNotFoundError as e:
            return 

    def get_id(self):
        return self.id

    def _get_query(self, query_id):
        if not query_id in self.queries:
            self.queries[query_id] = {}
        return self.queries[query_id]

    def put(self, query_id, key, value):
        query = self._get_query(query_id)
        query[key] = value

        with open(self.storage + str(query_id) + QUERY_TYPE, 'a') as query_file:
            query_file.write('{},{},{}\n'.format('w',key,self.serialize_value(value)))

    def get(self, query_id, key):
        query = self._get_query(query_id)
        if key not in query:
            raise KeyError("On state")
        return query[key]

    def is_already_received(self, query, origin, msg_id):
        if origin not in self.message_table:
            return False
        return (self.message_table[origin] == msg_id)

    def commit(self, query_id, origin, msg_id):
        with open(self.storage + str(query_id) + QUERY_TYPE, 'a') as query_file:
            query_file.write('c\n')

        with open(self.storage + TABLE_FILE, 'a') as table_file:
            table_file.write('{},{},{}\n'.format(origin, msg_id, self.id))

        self.message_table[origin] = msg_id
        self.id = self.id + 1

    def delete(self, query_id):
        if query_id in self.queries:
            del self.queries[query_id]
            os.remove(self.storage + str(query_id) + QUERY_TYPE)

    def __str__(self):
            return 'Id: ' + str(self.id) + '\nMsgTable: ' + str(self.message_table) + '\nQueries: ' + str(self.queries)


# middleware
# constructor(con reconstruccion)

# llega_msg
# get_state(query_id) => {}
# is_already_received(origin, id_msg)
# delete(query_id)
# callback:
#     return {origen, id_msg}
# commit(origin, id_msg, query_id, valor)
#
# origin, id_recv, id_send, query_id,[video_id, RU]

state = QueryState('./storage/')
# state.put('cliente1', 'RU', 89)
# state.commit('cliente1', 1, 20)
# state.put('cliente2', 'ZD', 178)
# state.commit('cliente2', 2, 40)
# state.put('cliente1', 'CU', 29)
# state.commit('cliente1', 1, 20)
print(state)
