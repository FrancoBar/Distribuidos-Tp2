import json
import logging
import sys

PROCESS_LIST_COLUMN='process_list'

def broadcast_copies(base_filter, input_message, id, copies, on_recv=None, on_last=None):
    if  PROCESS_LIST_COLUMN not in input_message:
        input_message[PROCESS_LIST_COLUMN] = [id]
        base_filter.put_back(input_message)
        if not on_recv:
            return None
        return on_recv(input_message)

    if len(input_message[PROCESS_LIST_COLUMN]) == copies:
        if on_last:
            # print("Estoy en el on last")
            # logging.warning("Estoy en el on last")
            # sys.exit(67)
            return on_last(input_message)

        return None

    if id not in input_message[PROCESS_LIST_COLUMN]:
        input_message[PROCESS_LIST_COLUMN].append(id)
        base_filter.put_back(input_message)
        if not on_recv:
            return None
        return on_recv(input_message)

    base_filter.put_back(input_message)
    return None