import hashlib

# def router_two_receivers(message, control_route_key, next_stage_name_1, 
#                          next_stage_name_2, hashing_attributes_1, hashing_attributes_2,
#                          next_stage_amount_1, next_stage_amount_2):
#     if message['type'] == 'control':
#         return [control_route_key]
#     stage_1_routing_key = f'{next_stage_name_1}-{utils.hash_fields(message, hashing_attributes_1) % next_stage_amount_1}'
#     stage_2_routing_key = f'{next_stage_name_2}-{utils.hash_fields(message, hashing_attributes_2) % next_stage_amount_2}'
#     return [stage_1_routing_key, stage_2_routing_key]
    
# def router(message, control_route_key, next_stage_name, hashing_attributes, next_stage_amount):
#     if message['type'] == 'control':
#         return [control_route_key]
#     stage_routing_key = f'{next_stage_name}-{utils.hash_fields(message, hashing_attributes) % next_stage_amount}'
#     return [stage_routing_key]


def _hash_string(string_to_hash):
    return int(hashlib.sha512(string_to_hash.encode()).hexdigest(), 16)


def hash_fields(message, hashing_attributes):
        hashing_string = ''
        for attribute in hashing_attributes: # Extends to more than 2 receiving ends
            hashing_string += f"-{message[attribute]}"
        return _hash_string(hashing_string)


def router_iter(message, control_route_key, next_stages_data): # message, control_route_key, [{"next_stage_name":..., "hashing_attributes":..., "next_stage_amount":...},...]
    if message['type'] == 'control':
        return [control_route_key]

    stage_routing_keys = []
    for stage_data in next_stages_data:
        next_stage_name = stage_data["next_stage_name"]
        hashing_attributes = stage_data["hashing_attributes"]
        next_stage_amount = stage_data["next_stage_amount"]
        aux_routing_key = f'{next_stage_name}-{hash_fields(message, hashing_attributes) % next_stage_amount}'
        stage_routing_keys.append(aux_routing_key)
    return stage_routing_keys


def generate_routing_function(control_route_key, next_stage_names, hashing_attributes, next_stage_amounts):
    stages_rounting_data = []
    for i in range(len(next_stage_names)):
        stages_rounting_data.append({ 
            "next_stage_name": next_stage_names[i], 
            "hashing_attributes": hashing_attributes[i].split(','), 
            "next_stage_amount": int(next_stage_amounts[i])
        })
    return lambda message: router_iter(message, control_route_key, stages_rounting_data)
