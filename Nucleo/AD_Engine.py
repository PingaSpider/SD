import json
import socket
from kafka import KafkaProducer, KafkaConsumer



# Direcciones y puertos
HOST = 'localhost'
PORT = 5555
FILENAME_DRONES = 'drones_registry.json'
KAFKA_IP = 'localhost'
KAFKA_PORT = 9092
END_SHOW_TOPIC = 'end_show'
HOST_BROKER = 'localhost'
PORT_BROKER = 9092
FILENAME_FIGURAS= 'AwD_figuras.json'
FILENAME_ACTUALIZACIONES = 'last_updates.json'


#################################################
# Funciones para el Engine AUTHENTICATION
#################################################
# Leer drones desde JSON
def load_drones():
    try:
        with open(FILENAME_DRONES, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return []

drones_registered = load_drones()

def handle_connection(conn, addr):
    print("Conectado por", addr)
    data = conn.recv(1024).decode()
    drone_data = json.loads(data)
    id_dron = drone_data["id"]
    token = drone_data["token"]
    
    response = {"status": "error", "message": "Dron no registrado"}  # Inicializa la respuesta como error
    
    for dron in drones_registered:
        if dron["id"] == id_dron:
            if dron["token"] == token:
                print(f"Dron {token} autenticado exitosamente.")
                response = {"status": "success", "message": "Autenticado"}
                break  # Termina el bucle una vez que se autentica el dron
            else:
                response = {"status": "error", "message": "Token incorrecto"}
                print(f"Token incorrecto.")
                break  # Termina el bucle si el token es incorrecto
    
    conn.sendall(json.dumps(response).encode())  # Envía la respuesta después de recorrer la lista de drones

#################################################
#Funciones para el Engine SHOW
#################################################
# Inicializar el producer de Kafka
def start_show():
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Cargar el archivo JSON
    with open(FILENAME_FIGURAS, 'r') as file:
        data = json.load(file)

    # Suponiendo que <FIGURA> es una clave en el JSON
    figure_data = data['<FIGURA>']

    # Enviar los mensajes a Kafka
    for drone_info in figure_data:
        message = {
            'ID_DRON': drone_info['<ID_DRON>'],
            'COORDENADAS': f"{drone_info['<COORDENADA_X_DESTINO>']},{drone_info['<COORDENADA_Y_DESTINO>']}"
        }
        producer.send('engine-to-drones', value=message)

    # Asegurarse de que todos los mensajes se envían
    producer.flush()
    producer.close()

#################################################
#Funciones para escuhar los Drones
#################################################
def start_listening():
    # Inicializar el consumidor de Kafka
    consumer = KafkaConsumer(
        'drons_to_engine',
        group_id='engine',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        process_message(message.value)
        #limpiar el comsumidor
        consumer.commit()

#LISTO
def load_last_updates():
    try:
        with open(FILENAME_ACTUALIZACIONES, 'r') as file:
            data = json.load(file)
            if isinstance(data, dict):  # Si es un diccionario, lo convierte en una lista
                return [data]
            else:
                return data
    except FileNotFoundError:
        return []

#LISTO
def save_drones(drones):
    with open(FILENAME_ACTUALIZACIONES, 'w') as file:
        json.dump(drones, file, indent=4)

#LISTO
def process_message(message):
    # Extraer datos del mensaje
    ID_DRON = message['ID_DRON']
    COORDENADA_X_ACTUAL = message['COORDENADA_X_ACTUAL']
    COORDENADA_Y_ACTUAL = message['COORDENADA_Y_ACTUAL']
    ESTADO_ACTUAL = message['ESTADO_ACTUAL']
    
    drones = load_last_updates()
    dron_found = False

    # Publicar el mensaje en pantalla
    print(f"Mensaje recibido del dron con ID: {ID_DRON}")

    # Buscar si el dron ya está registrado y actualizar sus datos si es necesario
    for dron in drones:
        if dron["ID_DRON"] == ID_DRON:
            dron["COORDENADA_X_ACTUAL"] = COORDENADA_X_ACTUAL
            dron["COORDENADA_Y_ACTUAL"] = COORDENADA_Y_ACTUAL
            dron["ESTADO_ACTUAL"] = ESTADO_ACTUAL
            dron_found = True
            print(f"Dron {ID_DRON} actualizado exitosamente.")
            break  # Terminar el bucle una vez que se encuentra y actualiza el dron

    # Si el dron no fue encontrado en la lista, añadirlo
    if not dron_found:
        drones.append({
            "ID_DRON": ID_DRON,
            "COORDENADA_X_ACTUAL": COORDENADA_X_ACTUAL,
            "COORDENADA_Y_ACTUAL": COORDENADA_Y_ACTUAL,
            "ESTADO_ACTUAL": ESTADO_ACTUAL
        })
        print(f"Nuevo dron registrado en el archivo de actualizaciones. ID: {ID_DRON}")

    save_drones(drones)  # Guardar la lista actualizada de drones






def start_engine():
    # Implementar lógica de inicio (por ejemplo, leer fichero y enviar instrucciones a drones)
    print("Engine started. Waiting for figure file...")
    # ... (código adicional para manejar el fichero y enviar instrucciones a drones)


"""def main():
    while True:
        choice = input("Select option (start/stop): ").lower()
        if choice == "start":
            start_engine()
        elif choice == "stop":
            #stop_engine()
            break
        else:
            print("Invalid option. Please select 'start' or 'stop'.")"""

#main de prueba
def main():  
    start_listening()

if __name__ == "__main__":
    main()




