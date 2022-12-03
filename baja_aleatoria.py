import subprocess
import docker
import random
import time
import json
import re

BLACKLIST = ['request_listener','client','health_checker']
METHODS = ['stop','kill']
FRASES = ['no podía volar',
		  'resbaló con una cáscara de banana',
		  'encuentró una mezcla de tabulaciónes y espacios. Catástrofe',
		  'renunció para dedicarse a su pasión',
		  'pide un aumento de RAM',
		  'debe atender una conferencia internacional',
		  'no se encuentra disponible en este momento',
		  'gano el segundo lugar en un concurso de belleza',
		  'ahora está en paz',
		  'descargó RAM de internet',
		  'salió a pasear en el bosque']

i = 0
while True:
	result = subprocess.run(['docker', 'ps','-a', '-q'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	containers_list = result.stdout.decode('utf-8').split('\n')[:-1]
	if len(containers_list) == 0:
		print('No containers')
		break
	chosen_container = random.choice(containers_list)
	chosen_method = random.choice(METHODS)
	down_reason = FRASES[i]
	
	result = subprocess.run(['docker', 'inspect', "--format='{{json .Name}}'", chosen_container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	chosen_container_name = re.sub(r'[^a-zA-Z0-9_]+', '', result.stdout.decode('utf-8'))

	is_blacklisted = False
	for blacklisted in BLACKLIST:
		if blacklisted in chosen_container_name:
			is_blacklisted = True
	if is_blacklisted:
		continue

	print(f'{chosen_container} {chosen_method} {chosen_container_name} {down_reason}')
	#subprocess.run(['docker', chosen_method, chosen_container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	
	i = (i + 1) % len(FRASES)
	time.sleep(10)
