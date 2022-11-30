import subprocess
import docker
import random
import time

FRASES = ['no podía volar',
		  'resbaló con una cáscara de banana',
		  'encuentró una mezcla de tabulaciónes y espacios. Catástrofe',
		  'renunció para dedicarse a su pasión',
		  'pide un aumento de RAM',
		  'debe atender una conferencia internacional',
		  'no se encuentra disponible en este momento',
		  'gano el segundo lugar en un concurso de belleza',
		  'ahora está en paz']

i = 0
while True:
	result = subprocess.run(['docker', 'ps','-a', '-q'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	containers_list = result.stdout.decode('utf-8').split('\n')[:-1]

	chosen_container = random.choice(containers_list)
	down_reason = FRASES[i]
	print(f'{chosen_container} {down_reason}')
	subprocess.run(['docker', 'stop', chosen_container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	
	i = (i + 1) % len(FRASES)
	time.sleep(10)
