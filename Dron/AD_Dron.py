import socket
import json
import threading
from kafka import KafkaConsumer, KafkaProducer



#IP y puerto del Registry
HOST_REGISTRY = 'localhost'
PORT_REGISTRY = 4444

#IP y puerto del Engine
HOST_ENGINE = 'localhost'
PORT_ENGINE = 5555

#IP y puerto del Broker/Bootstrap-server del gestor de colas
HOST_BROKER = 'localhost'
PORT_BROKER = 9092

# Estados posibles del dron
STATES = ["waiting", "moving", "landed"]

class Dron:
    # Variable de clase para mantener el ID del próximo dron
    next_id = 1
    
    def __init__(self):
        # Asigna el siguiente ID disponible al dron y luego incrementa el contador
        self.id = Dron.next_id
        Dron.next_id += 1
        self.token = None  # Inicialmente, el dron no tiene un token hasta que se registre
        self.state = STATES[0] # Inicialmente, el dron está en estado "waiting"
        self.position = (1,1) # Inicialmente, el dron está en la posición (1,1)
    
    #Define el destructor del dron (no imprima nada en el destructor)
    def destroy(self):
        pass
    
    def register(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST_REGISTRY, PORT_REGISTRY))
            reg_msg = {'id': self.id}
            s.sendall(json.dumps(reg_msg).encode())
            
            data = s.recv(1024)
            if not data:  # Si no se recibe respuesta, imprimir un error y retornar False
                print("No se recibió respuesta del Registry.")
                return False
            
            try:
                response = json.loads(data.decode())
                if response['status'] == 'success':
                    self.token = response['token']
                    print(f"Dron {self.id} registrado exitosamente con el token: {self.token}")
                    return True
                else:
                    if response['message'] == 'Ya registrado':
                        print(f"Dron {self.id} ya estaba registrado.")
                    else:
                        print(f"Error al registrar el Dron {self.id}.")
                        return False
            except json.JSONDecodeError:  # Si hay un error al decodificar el JSON, manejarlo
                print("Respuesta del Registry no es un JSON válido.")
                return False


    def authenticate(self):
        # Crear un socket para comunicarse con Engine
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST_ENGINE, PORT_ENGINE))
            # Crear mensaje de autenticación
            auth_msg = {
                'id': self.id,
                'token': self.token
            }
            # Enviar mensaje al Engine
            s.sendall(json.dumps(auth_msg).encode())

            # Recibir respuesta del Engine
            data = s.recv(1024)
            if not data:  # Si no se recibe respuesta, imprimir un error y retornar False
                print("No se recibió respuesta del Engine.")
                return False
            
            response = json.loads(data.decode())

            try:
                response = json.loads(data.decode())
                if response['status'] == 'success':
                    print(f"Dron {self.id} autenticado exitosamente en el Engine!")
                    return True
                else:
                    print(f"Error de autenticación del Dron {self.id}.")
                    return False
            except json.JSONDecodeError:  # Si hay un error al decodificar el JSON, manejarlo
                print("Respuesta del Registry no es un JSON válido.")
                return False
def mover_dron_a(x, y):
    # Código para mover el dron a las coordenadas (x, y).
    # Esto depende de cómo esté programado el dron y qué bibliotecas o SDKs estés utilizando.
    pass
            
    #################################################
    #Comunicacion del dron con el Engine
    #################################################
    # El dron necesita saber su propio ID para suscribirse al tópico correcto.
drone_id = "ID_DEL_DRON"
def listen_to_kafka():
    consumer = KafkaConsumer(f'drone-{drone_id}', bootstrap_servers='localhost:9092')
    for message in consumer:
        # Extraer las coordenadas x e y del mensaje.
        x, y = message.value.decode('utf-8').split(',')
    
    # Convertir x e y en números enteros (si es necesario).
    x = int(x)
    y = int(y)

    # Aquí es donde el dron actuaría en base a las coordenadas recibidas.
    # Por ejemplo, podría ser una función que mueva el dron a esas coordenadas.
    mover_dron_a(x, y)

def menu():
    dron = Dron()
    while True:
        print("1. Registrar el dron en el Registry")
        print("2. Autenticar el dron con el Engine")
        print("3. Comenzar a escuchar instrucciones")
        print("4. Salir")
        opcion = int(input("Seleccione una opcion: "))

        if opcion == 1:
            dron.register()
        elif opcion == 2:
            dron.authenticate()
        elif opcion == 3:
            # Comienza a escuchar en un hilo separado
            kafka_thread = threading.Thread(target=listen_to_kafka)
            kafka_thread.start()
        elif opcion == 4:
            print("Saliendo del programa.")
            break  # Esto terminará el bucle while
        else:
            print("Opcion invalida.")

if __name__ == '__main__':
    menu()
