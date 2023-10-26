import json
import socket
from kafka import KafkaConsumer, KafkaProducer



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

# Crear el productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    # Leer el archivo JSON y obtener las instrucciones
    with open(FILENAME_FIGURAS, 'r') as file:
        data = json.load(file)
        
        for figura in data["figuras"]:
            nombre_figura = figura["Nombre"]
            for drone in figura["Drones"]:
                drone_id = drone["ID"]
                posicion = drone["POS"]

                # Formatear el mensaje como un diccionario
                mensaje = {
                    'figura': nombre_figura, 
                    'posicion': posicion
                }

                # Publicar instrucciones en el tópico del dron específico
                producer.send(f'drone-{drone_id}', mensaje)

except Exception as e:
    print(f"Ocurrió un error: {e}")

finally:
    # Cerrar el productor
    producer.close()


def start_engine():
    # Implementar lógica de inicio (por ejemplo, leer fichero y enviar instrucciones a drones)
    print("Engine started. Waiting for figure file...")
    # ... (código adicional para manejar el fichero y enviar instrucciones a drones)


def main():
    while True:
        choice = input("Select option (start/stop): ").lower()
        if choice == "start":
            start_engine()
        elif choice == "stop":
            stop_engine()
            break
        else:
            print("Invalid option. Please select 'start' or 'stop'.")

if __name__ == "__main__":
    main()





