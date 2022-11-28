# TP2: Tolerancia a Fallos
 **Agustín Cambiano** ,**Barreneche Franco**



## Índice

[TOC]

## Alcance

El sistema deberá responder a los escenarios planteados en la sección correspondiente, presentando alta disponibilidad y  tolerancia a fallos.

Una vez alcanzado un estado correcto por primera vez, ante el fallo de un nodo,  éste deberá recuperarse automáticamente y el conjunto de operaciones a su cargo deberá continuar. Se contemplará la preservación de datos relevantes al negocio, así como al control para la garantía de la correctitúd de los resultados.

El sistema deberá procesar concurrentemente solicitudes de clientes, aunque se admitirá la posibilidad de limitar la cantidad de encargos gestionados en simultáneo.

Se desarrollarán protocolos propios tanto para la preservación de datos, como para su remoción cuando ya no sean necesarios. 

No formará parte del alcance de éste trabajo la recuperación ante fallas de hardware de almacenamiento catastróficas, ni la preservación de conexiones o restauración del trabajo ante una eventual reconexión.

Se relegará a un middleware de terceros las garantías vinculadas al manejo de mensajes, como ser preservación de mensajes ante la falla de uno de los extremos o la preservación del órden.

Los nodos del sistema se concentrarán en containers. Podrá utilizarse la API de Docker para el inicio, reinicio y detenimiento de containers, no así para obtener información del estado del sistema.



## Arquitectura de Software

Para la ejecución de las tareas con alto throughput se consideró adecuado el planteo de una arquitectura del tipo pipeline, cuyas colas intermedias son gestionadas por un middleware de mensajes externo.

Bastó con acompañar los mensajes con un id de query para separar el estado local de cada nodo y sumar una ganancia en paralelismo por la intercalación de operaciones entre solicitudes clientes.

Los clientes se comunican por un socket TCP a un único punto de entrada y salida del sistema (y un único punto de falla). En una primer fase ingestan al sistema entrada por entrada los datos a procesar y  luego quedan a la espera de mensajes de respuesta.

Para el monitoreo del estado de los contenedores se dispuso un cluster de "health-monitors" con comportamiento homogéneo, en donde un lider electo visita secuencialmente los servicios a monitorear  (incluído el cluster de health-monitoring) y sus respaldos se preparan para tomar su lugar ante su caída. En cada nodo existe un proceso de prueba de vida ajeno al proceso principal, que consulta periódicamente su estado y responde a las consultas del health monitor apropiadamente. Dada su simpleza y tamaño, estos mensajes se intercambian por sockets UDP.



## Objetivos y Restricciones

*Hablar de hash*



## Vista Lógica

![](./imgs/dag.png)

*DAG global de tareas*

El DAG previo muestra una división lógica de tareas, sus dependencias y el flujo de datos (se excluyen señales de control). Se observa como solo un subconjunto de campos de los datos de entrada son necesarios para dar respuesta a las consultas planteadas y como la mayoría de dichos campos pueden descartarse tras su uso. Existe  una correspondencia uno a uno entre etapas del pipeline y tareas, con la salvedad del filtrado de videos trending en todos los países por 21 días, que finalmente se redujo a una sola etapa, para disminuir redundancia de operaciones.



*Incluir diag. estados de health-monitor*





![](./imgs/clases.png)

*Diagrama de clases del middleware*

Se encapsuló la lógica de recepción y envío de mensajes entre canales y sockets en una capa de middleware que cada etapa del pipeline consumía. El diagrama presenta la jerarquía de clases interna de los filtros del middleware. _ChannelQueue y _TCPQueue ocultan los detalles del modo en que se serializan y transmiten los mensajes, mientras que _BaseFilter reúne el compartamiento común a todo filtro, como ser la administración de las colas y el procesamiento de señales. Finalmente ChannelChannelFilter, TCPChannelFilter y ChannelTCPFilter abstráen los detalles más delicados de la inicialización de las colas, ej: manejo de la conexión con RabbitMQ.



## Vista de Procesos

*Mejorar mismo caso*

![](./imgs/secuencia.png)

*Diagrama de secuencia flujo de día máximo*



El flujo del cálculo del día máximo permite destacar aspectos relevantes del protocolo general de comunicación. El contenido específico de los mensajes no es el foco de éste diagrama, es suficiente conocer que existen mensajes de datos y de control. Los primeros corresponden al negocio, mientras que los segundos pueden ser del tipo config o eof. La señal de eof no es un capricho, se requiere su emisión tanto para resetear los filtros que presentan estado, como para concluir operaciones potencialmente infinitas. Más aún, todo retorno de los cálculos, por la naturaleza del pipeline, es opcional y diferido. A continuación se ahondará en el automensaje de Max Day Filter. 



*Levantamiento de estado de archivo*



![](./imgs/actividades_recu.png)

*Diagrama de actividades para broadcast entre copias*



La comunicación de información de control resultó ser un gran desafío de diseño, pues no solo debía llegar a todas las copias de un componente, sino que también debía respetar el órden de mensajes junto con los mensajes de datos. Aunque se evaluó utilizar canales separados de broadcast internos, finalmente se optó por un protocolo que trabaja solo con la cola de entrada ,que es compartida entre control y datos.

El diagrama explica claramente el protocolo, pero no da cuenta de porqué está garantizada su terminación y su buen desempeño. Todo lo anterior esta dado por el fairness que provee RabbitMQ y el tipo de operaciones del negocio, cuyo tiempo de ejecución es casi independiente de los datos de entrada y por ende parejo entre las copias. Por ejemplo, cuando se recibe la señal de eof, todo el resto de las copias podrían tener algunos mensajes pendientes por procesar y entonces la misma copia desencolará y encolará el mensaje de eof una y otra vez, pero tras este corto tiempo se liberarán progresivamente el resto de los nodos y gracias al fairness el mensaje de control tenderá a transmitirse en roundrobin, llegando solo a las copias necesarias, hasta que todos hayan recibido el mensaje. 



## Vista de Desarrollo

![](./imgs/paquetes.png)

*Diagrama de paquetes de request_listener*



Como se adelantó en la sección anterior, casi todos los componentes del sistema se adhieren a la estructura de paquetes enseñada en el diagrama: Hacen uso de un filtro base del middleware, que se encarga de la declaración de colas y la interacción con ellas y que al recibir un mensaje invoca una callback especificada en su construcción.

Actualmente, request_listener es el único que emplea colas TCP. En tal caso el middleware hace uso de módulos de transmisión y comunicación confiable de datos por sockets.  Server.py encapsula solo la lógica de escucha y aceptación de conexiones entrantes. 







## Vista Física

![](./imgs/robustez.png)



*Diagrama de robustez*

 

Como puede observarse, la mayoría de los filtros están pensados para permitir su escalamiento. Max day agg y Request Listener son la excepción. Request Listener es un nodo crítico, no solo necesita ser único, sino que además su eventual falla puede dejar al resto de los componentes en un estado inválido. De modo que la caída de Request Listener conlleva el reinicio total del sistema. Por el contrario, Max day agg procesa un volumen de datos igual a la cantidad de copias de Max day filter y solo se activa intermitentemente. En el presente es también un punto de falla, pero podría solventarse fácilmente empleando una variable "máximo actual" en modo archivo. 





![](./imgs/despliegue.png)

*Diagrama de despliegue*

Claramente existe gran dependencia del middleware de colas, que en este caso es RabbitMQ. Por lo demás, cada nodo puede ser desplegado independientemente. Por la implementación, actualmente las copias de un mismo filtro deben estar en el mismo dispositivo (pues comparten archivos por volúmenes de docker), pero realmente ninguna de las tareas requiere acceso a la totalidad de los datos. Si se garantiza la afinidad de los datos, sea mediante un sistema de archivos distribuído o sencillamente con un routekey que dependa de un atributo de los datos (ej: video_id), entonces esta restricción sobre el escalamiento desaparecería.



## Escenarios

![](./imgs/casos_de_uso.png)

*Diagrama de casos de uso*



La interacción con el sistema es más bien simple, los clientes se conectan secuencialmente con el servidor e ingresan los datos a procesar con el fin de obtener el día con mayor cantidad de vistas, videos con tag "funny" populares y los thumbnails de videos trending. Luego esperan su resultado. El sistema no se detiene por si mismo, pero un administrador puede interrumpirlo en cualquier momento a través de señales.



## Tamaño y Rendimiento





## Ejecución

Para ejecutar el sistema debe colocarse el archivo `config.ini`  provisto en la raíz del repositiorio, en la carpeta **.data** (crear si no existe). Crear también dentro de **.data** un directorio **datasets** con los archivos csv de países que el cliente procesará. Asegurarse de que los dos caracteres iniciales del nombre de los archivos sean únicos.

Iniciar el sistema con `sh run.sh` y detenerlo cuando acabe el procesamiento con `sh stop.sh`.

El cliente guardará los archivos resultantes de la ejecución en **.data/output**.