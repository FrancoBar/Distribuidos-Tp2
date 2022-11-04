import json

PROCESS_LIST_COLUMN='process_list'

def broadcast_copies(base_filter, input_message, id, copies, on_recv=None, on_last=None):
    if  PROCESS_LIST_COLUMN not in input_message:
        input_message[PROCESS_LIST_COLUMN] = [id]
        base_filter.put_back(input_message)
        if not on_recv:
            return None
        return on_recv(base_filter, input_message)

    if len(input_message[PROCESS_LIST_COLUMN]) == copies:
        if on_last:
            return on_last(base_filter, input_message)

        return None

    if id not in input_message[PROCESS_LIST_COLUMN]:
        input_message[PROCESS_LIST_COLUMN].append(id)
        base_filter.put_back(input_message)
        if not on_recv:
            return None
        return on_recv(base_filter, input_message)

    base_filter.put_back(input_message)
    return None