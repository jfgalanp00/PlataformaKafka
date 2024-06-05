from hdfs import InsecureClient
from kafka import KafkaConsumer
import time
import re
import os

# Datos de conexión
HDFS_HOSTNAME = '172.17.10.30'

HDFSCLI_PORT = 9870

HDFSCLI_CONNECTION_STRING = f'http://{HDFS_HOSTNAME}:{HDFSCLI_PORT}'

contador=0
ficheroCreado=False
# En nuestro caso, al no usar Kerberos, creamos una conexión no segura
#hdfs_client = InsecureClient(HDFSCLI_CONNECTION_STRING,user=None)
hdfs_client = InsecureClient(HDFSCLI_CONNECTION_STRING)


# Consumidor Kafka
consumer = KafkaConsumer(
    '2024_145_temperatura',
    enable_auto_commit=True,
    bootstrap_servers=['172.17.10.35:9092','172.17.10.34:9092','172.17.10.33:9092'])

for m in consumer:
    
    json_value=str(m.value)
    fecha=json_value.split('\\')[0]
    fecha=fecha.split('"')[1]

    valor_sucio=json_value.split("\\")[6]
    valor_sucio=re.findall(r'\d+', valor_sucio)
    temperatura = ''.join(valor_sucio)

    registro=f'{fecha};{temperatura}'
    print(registro)

    if contador<=60: 

        if ficheroCreado==False:
           
            fecha_actual=time.localtime()
            fecha_actual_con_hora = time.strftime("%Y-%m-%d %H-%M temperatura", fecha_actual)
            nom_fichero = f"{fecha_actual_con_hora}.csv"

            with open(nom_fichero, 'a') as archivo:
                    archivo.write(f'{registro}\n')
                
            ficheroCreado=True  
        contador+=1

        with open(nom_fichero, 'a') as archivo:
                    archivo.write(f'{registro}\n')
        
    else:

        hdfs_client.write(f"/Pro_IOT/145/temperatura/{nom_fichero}", nom_fichero)
        os.remove(nom_fichero)
    