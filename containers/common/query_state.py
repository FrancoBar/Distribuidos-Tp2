import os

FILE_TYPE = '.csv'

class QueryState:
    def __init__(self, storage,read_value, write_value):
        self._storage = storage
        self._read_value = read_value
        self._write_value = write_value
        self._queries = {}
        self._build_queries()

    def _build_queries(self):
        if not os.path.exists(self._storage):
            os.makedirs(self._storage)
        
        query_file_list = list(filter(lambda file_name : file_name[-len(FILE_TYPE):] == FILE_TYPE, os.listdir(self._storage)))
        for query_name in query_file_list:
            try:
                query_id = query_name[:-len(FILE_TYPE)]
                self._queries[query_id] = self._build_query(query_name)
            except FileNotFoundError as e:
                return

    def _build_query(self, query_name):
        query = {'id':0 , 'msg_table' : {}, 'values' : {}}
        current_size = 0

        with open(self._storage + query_name, 'r+') as query_file:
            while True:
                line = query_file.readline()
                if line == '':
                    break
                if '\n' not in line:
                    query_file.truncate(current_size)
                    break
                current_size = query_file.tell()

                #ToDo: Check len
                origin, in_id, out_id, key, value = line[:-1].split(',')
                query['msg_table'][origin] = in_id
                query['id'] = int(out_id)
                self._read_value(query['values'], key, value)
    
        return query

    def _get_query(self, query_id):
        if not query_id in self._queries:
            self._queries[query_id] = {'id':0 , 'msg_table' : {}, 'values' : {}}
        return self._queries[query_id]

    def delete_query(self, query_id):
        if query_id in self._queries:
            del self._queries[query_id]
            os.remove(self._storage + str(query_id) + FILE_TYPE)

    def get_values(self, query_id):
        return self._get_query(query_id)['values']

    def get_id(self, query_id):
        return self._get_query(query_id)['id']

    def is_msg_received(self, query_id, origin, msg_id):
        assert(type(msg_id) == str)
        query = self._get_query(query_id)
        if origin not in query['msg_table']:
            return False
        return (query['msg_table'][origin] == msg_id)

    def commit(self, query_id, origin, msg_id, key, value):
        query = self._get_query(query_id)
        out_id = query['id']

        log_entry_header = '{},{},{},{},'.format(origin, msg_id, out_id, key)
        log_entry_body = self._write_value(query['values'], key, value)
        with open(self._storage + str(query_id) + FILE_TYPE, 'a') as query_file:
            query_file.write(log_entry_header + log_entry_body + '\n')

        query['msg_table'][origin] = msg_id
        query['id'] = out_id + 1

    def __str__(self):
            return str(self._queries)

def _default_read_value(query, key, value):
    query[key] = value

def _default_write_value(query, key, value):
    return str(value)

state = QueryState('./storage/', _default_read_value, _default_write_value)

print(state)

state.commit('cliente1', 'origin1', 1, 'key1',20)
state.commit('cliente1', 'origin1', 2, 'key2',40)
state.commit('cliente1', 'origin2', 3, 'key1',20)
state.commit('cliente2', 'origin1', 0, 'key',10)
state.commit('cliente2', 'origin1', 1, 'key',5)

# print(state.get_id('cliente1'), state.get_id('cliente2'))
# print(state.get_values('cliente1'), state.get_values('cliente2'))

# print(state.is_msg_received('cliente1','origin2', '3'))
# print(state.is_msg_received('cliente1','origin2', '2'))
# print(state.is_msg_received('cliente1','origin1', '4'))
# print(state.is_msg_received('cliente1','origin1', 8))

#state.delete_query('cliente1')

print(state)
