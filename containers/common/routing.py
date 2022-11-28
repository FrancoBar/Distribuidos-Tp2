import hashlib

def _hash_string(string_to_hash):
    return int(hashlib.sha512(string_to_hash.encode()).hexdigest(), 16)

def hash_fields(message, hashing_attributes):
        hashing_string = ''
        for attribute in hashing_attributes: # Extends to more than 2 receiving ends
            hashing_string += f"-{message[attribute]}"
        return _hash_string(hashing_string)

def hash_router(message, next_stages_data):
    stage_routing_keys = []
    for stage_data in next_stages_data:
        next_stage_name = stage_data["next_stage_name"]
        hashing_attributes = stage_data["hashing_attributes"]
        next_stage_amount = stage_data["next_stage_amount"]
        aux_routing_key = f'{next_stage_name}-{hash_fields(message, hashing_attributes) % next_stage_amount}'
        stage_routing_keys.append(aux_routing_key)
    # print(f'Sent to: {stage_routing_keys}')
    return stage_routing_keys


def router_iter(message, control_route_key, next_stages_data): # message, control_route_key, [{"next_stage_name":..., "hashing_attributes":..., "next_stage_amount":...},...]
    if message['type'] == 'control' or message['type'] == 'priority':
        # return list(map(lambda _: control_route_key, next_stages_data))
        return [control_route_key]

    # stage_routing_keys = []
    # for stage_data in next_stages_data:
    #     next_stage_name = stage_data["next_stage_name"]
    #     hashing_attributes = stage_data["hashing_attributes"]
    #     next_stage_amount = stage_data["next_stage_amount"]
    #     aux_routing_key = f'{next_stage_name}-{hash_fields(message, hashing_attributes) % next_stage_amount}'
    #     stage_routing_keys.append(aux_routing_key)
    # # print(f'Sent to: {stage_routing_keys}')
    # return stage_routing_keys
    return hash_router(message, next_stages_data)


def generate_routing_function(control_route_key, next_stage_names, hashing_attributes, next_stage_amounts):
    stages_routing_data = []
    for i in range(len(next_stage_names)):
        stages_routing_data.append({ 
            "next_stage_name": next_stage_names[i], 
            "hashing_attributes": hashing_attributes[i].split(','), 
            "next_stage_amount": int(next_stage_amounts[i])
        })
    # return lambda message: router_iter(message, control_route_key, stages_rounting_data)
    if control_route_key != None:
        return lambda message: router_iter(message, control_route_key, stages_routing_data)
    else:
        return lambda message: hash_router(message, stages_routing_data)

def last_stage_router(message):
    return [message['client_id']]