class GeneralFilter:
    def __init__(self, node_id, previous_stage_amount, middleware, query_state):
        self.node_id = node_id
        self.previous_stage_amount = previous_stage_amount
        self.middleware = middleware
        self.query_state = query_state

    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)

        if input_message['case'] == 'eof':
            print(f"client values: {client_values}")

            if len(client_values) == 0:
                self.query_state.delete_query(client_id)
                return

            if not ('eof' in client_values):
                client_values['eof'] = 0
            client_values['eof'] += 1
            self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'eof', client_values['eof'])
            
            if client_values['eof'] == self.previous_stage_amount:
                self._on_last_eof(input_message)
            else:
                self.query_state.commit(client_id, input_message['origin'],str(input_message['msg_id']))
        else:
            self._on_config(input_message)
            

    def _on_config(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        client_values['config'] = None
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'config', 'config')
        self.query_state.commit(client_id, input_message['origin'],str(input_message['msg_id']))


    def _on_last_eof(self, input_message):
        client_id = input_message['client_id']
        input_message['msg_id'] = self.query_state.get_id(client_id)
        input_message['origin'] = self.node_id
        self.middleware.send(input_message)
        self.query_state.delete_query(client_id)

    # Nothing passes the filter by default
    def process_data_message(self, input_message):
        pass

    def process_received_message(self, input_message):
        client_id = input_message['client_id']

        if self.query_state.is_last_msg(client_id, input_message['origin'], str(input_message['msg_id'])):
            return

        if input_message['type'] == 'data':
            self.process_data_message(input_message)
        else:
            self.process_control_message(input_message)

    def start_received_messages_processing(self):
        self.middleware.run()
