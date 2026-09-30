# Informe

## Datos del estudiante

Estudiante: Nicolás Ángel Garófalo  
Padrón: 100952

## Coordinación entre Sums

Antes de explicar la implementación final, se explicará resumidamente las ideas descartadas y el por qué, ya que explican la implementación final.

### Idea 1 (Finalización de mensajes pendientes usando prefetch=N):

Consistía en enviar el _EOF_ mediante un exchange entre Sums. Esto provocaba una race condition: los nodos recibían el _EOF_ por el exchange antes de procesar los datos pendientes y enviaba frutas a los aggregators antes de tiempo. Intentar procesar una cantidad fija según el prefetch = N de RabbitMQ post _EOF_ tampoco funcionó, ya que se contempló el caso de esperar mensajes inexistentes derivaría en un bloqueo del sum (no hay garantía de que realmente tenga N mensajes pendientes).

### Idea 2 (Mensajes con IDs únicos):

Consistía en asignar un ID único a cada mensaje desde el messageHandler para que un Sum _coordinador_ (el que recibe el _EOF_ primero) rastree los IDs procesados por los demas Sums. Se descartó por problemas de escalabilidad: guardar en memoria los N IDs procesados por cada sum no escala ante millones de registros.

### Idea final:

Se mantuvo la idea del exchange, y se cambió la lógica de ID por mensaje a un contador en messageHandler. De esta manera, el sum que recibe el mensaje de _EOF_ original ahora cuenta con el total de mensajes emitidos. Al recibir dicho mensaje:

1. El sum se autoproclama coordinador, guarda la cantidad total de mensajes enviados, y se encarga de enviar el EOF a todos los otros sums por el exchange (`EofFromSum`).

2. Cuando estos sums reciben el EOF por exchange, envían un mensaje con la cantidad de mensajes que procesaron hasta ese momento (`ProcessedBySum`).

3. El sum coordinador guarda la cantidad de mensajes procesados por sum.

4. Por cada `ProcessedBySum` recibido, valida si se cumple la condición de que el total de todos los mensajes procesados por los sums sea el total de mensajes emitidos.

5. Si se cumple la condición, el sum coordinador ordena a los demás sums que pueden enviar sus frutas a los aggregators (`Flush`).

#### Casos borde
- Si todavía no se cumple la condición, cada sum seguirá notificando el conteo de dicho cliente al coordinador (cada vez que procesa un nuevo mensaje de dicho cliente), y dicho coordinador validará mensaje a mensaje, hasta que se alcance la condición de corte.  
- Si hay 1 solo sum en la infraestructura (tests iniciales), apenas el sum se define coordinador, chequea que la condición de corte se cumpla, y procede a enviar el flush a los aggregators.

El punto positivo de esta implementación es que el envío de mensaje procesado se reduce ya que únicamente se envía uno a uno a partir de haber recibido el EOF y no antes. La cantidad de mensajes pendientes por procesar se presupone baja una vez recibido EOF.


## Coordinación entre sums - aggregators - joiner

El criterio definido para que cada sum sepa a cual aggregator enviar sus frutas es el siguiente: 

1. El nombre de la fruta se hashea, obteniendo un valor único por fruta.
2. El valor único se le aplica el módulo por la cantidad de aggregators, obteniendo un valor de 0 a aggregationAmount - 1, es decir, un aggregatorId.
3. Se envía el registro de la fruta al aggregatorId calculado.
4. Cuando no queda nada mas para enviar, se envía un EOF a todos los aggregators.

Cuando los aggregators reciben `sumAmount` mensajes EOF (vinculados a ese `clientId`), arma su top de frutas parcial, y se lo envía al joiner.

El joiner realiza el mismo razonamiento: Cuando recibe `aggregationAmount` mensajes de un `clientId` específico, calcula el top de frutas final y se lo envía al output.


## Escalabilidad del sistema

La escalabilidad por clientes está contemplada porque todos los mensajes enviados a los sums tienen un clientId único generado en `MessageHandler`. A su vez, tanto los sums, aggregators y el joiner cuentan con maps con clave clientId para gestionar el estado de cada cliente independientemente (para guardar la cantidad de mensajes procesados, gestión de frutas, EOFs recibidos, etc.).

## Mejoras futuras

- El principal problema de la implementación actual es que el coordinador es un _single point of failure_ en caso de caída de nodo. Sin embargo, implementar las mismas comunicaciones pero con todos los sums implicaba una sobrecarga de mensajes que consideré menos performante que la actual. Implementar un algoritmo de reasignación de coordinador en base a caída también es una solución, aunque la consideré fuera de scope para el TP.
- También se podría mejorar un poco más el protocolo interno (`inner.go`) debido a que, si bien se mejoraron las serializaciones, 
- Se podría cambiar la implementación del middleware para que el sum pueda enviar mensajes a un aggregator o un sum específico, como por ejemplo hacer un SendTo.