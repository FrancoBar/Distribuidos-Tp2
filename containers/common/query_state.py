import os

FILE_TYPE = '.csv'

TABLE_BYTE = 't'
WRITE_BYTE = 'w'
COMMIT_BYTE = 'c'

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
        last_commit_size = 0
        pending_lines = []
        with open(self._storage + query_name, 'r+') as query_file:
            while True:
                line = query_file.readline()
                if line == '' or '\n' not in line:
                    query_file.truncate(last_commit_size)
                    break

                log_type = line[0]
                if log_type == WRITE_BYTE or log_type == TABLE_BYTE:
                    pending_lines.append(line)
                elif log_type == COMMIT_BYTE and len(pending_lines) > 0:
                    last_id = query['id']
                    update_table = {}
                    update_value = {}
                    for pending_line in pending_lines:
                        try:
                            if pending_line[0] == TABLE_BYTE:
                                log_type, origin, in_id, out_id = pending_line[:-1].split(',')
                                update_table[origin] = in_id
                                query['id'] = int(out_id)
                            else:
                                log_type, key, value = pending_line[:-1].split(',')
                                update_value[key] = self._read_value(query['values'], key, value)
                        except ValueError:
                            query['id'] = last_id
                            print('Corrupted line')
                            query_file.truncate(last_commit_size)
                            break
                    pending_lines = []
                    query['values'].update(update_value)
                    query['msg_table'].update(update_table)
                    last_commit_size = query_file.tell()
                else:
                    print('Corrupted line')
                    query_file.truncate(last_commit_size)
                    break
    
        query['id'] = query['id'] + 1
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

    def _write(self, query_id, line):
        with open(self._storage + str(query_id) + FILE_TYPE, 'a') as query_file:
            query_file.write(line)

    def prepare(self, query_id, origin, msg_id):
        query = self._get_query(query_id)
        out_id = query['id']
        self._write(query_id, TABLE_BYTE + ',{},{},{}\n'.format(origin, msg_id, out_id))
        
        query['msg_table'][origin] = msg_id
        query['id'] = out_id + 1

    def write(self, query_id, key, value):
        query = self._get_query(query_id)
        out_id = query['id']
        self._write(query_id, '{},{},{}\n'.format(WRITE_BYTE, key, self._write_value(query['values'], key, value)))

    def commit(self, query_id):
        self._write(query_id, COMMIT_BYTE + '\n')

    def __str__(self):
            return str(self._queries)

def _default_read_value(query, key, value):
    return value

def _default_write_value(query, key, value):
    return str(value)

state = QueryState('./storage/', _default_read_value, _default_write_value)

print(state)

# state.prepare('cliente1', 'origin1', 1)
# state.write('cliente1', 'key1',20)
# state.write('cliente1', 'key2',40)
# state.commit('cliente1')

# state.prepare('cliente1', 'origin1', 2)
# state.write('cliente1', 'key1',10)
# state.write('cliente1', 'key3',50)
# state.commit('cliente1')

# state.prepare('cliente2', 'origin1', 3)
# state.write('cliente2', 'key4',10)
# state.commit('cliente2')

print(state.get_id('cliente1'), state.get_id('cliente2'))
print(state.get_values('cliente1'), state.get_values('cliente2'))

# print(state.is_msg_received('cliente1','origin2', '3'))
# print(state.is_msg_received('cliente1','origin2', '2'))
# print(state.is_msg_received('cliente1','origin1', '4'))
# print(state.is_msg_received('cliente1','origin1', 8))

#state.delete_query('cliente1')

print(state)
