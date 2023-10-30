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

    #Definir los getters y setters para los atributos del dron
    def get_id(self):
        return self.id
    
    def get_token(self):
        return self.token
    
    def get_state(self):
        return self.state
    
    def get_position(self):
        return self.position
    
    def set_id(self, id):
        self.id = id
    
    def set_token(self, token):
        self.token = token
    
    def set_state(self, state):
        self.state = state
    
    def set_position(self, position):
        self.position = position
    
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


#################################################
#Comunicacion del Engine con el Dron
#################################################
    # El dron necesita saber su propio ID para suscribirse al tópico correcto.
    # Inicializar el consumidor de Kafka
    def recive_data(self):
        consumer = KafkaConsumer(
            'drones_to_engine',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        # Función que procesa el mensaje y llama al método run
        def process_message(message):
            # Extraer datos del mensaje
            ID_DRON = message['ID_DRON']
            COORDENADA_X_ACTUAL = message['COORDENADA_X_ACTUAL']
            COORDENADA_Y_ACTUAL = message['COORDENADA_Y_ACTUAL']
            ESTADO_ACTUAL = message['ESTADO_ACTUAL']
            
            # Si el ID del dron en el mensaje coincide con el ID del dron actual, procesar el mensaje
            if ID_DRON == self.id:
                # Aquí puedes llamar a tu método run con los datos extraídos
                # Por ejemplo: run(ID_DRON, COORDENADA_X_ACTUAL, COORDENADA_Y_ACTUAL, ESTADO_ACTUAL)
                pass
        
        # Consumir mensajes de Kafka
        for message in consumer:
            process_message(message.value)

        
#LISTO
#################################################
#Comunicacion del Dron con el Engine
#################################################
# Método para enviar una actualización al Engine
    def send_update(self):
        # Crear el productor de Kafka
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Preparar el mensaje en formato JSON
        message = {
            'ID_DRON': self.id,
            'COORDENADA_X_ACTUAL': self.position[0],
            'COORDENADA_Y_ACTUAL': self.position[1],
            'ESTADO_ACTUAL': self.state
        }
        
        # Enviar el mensaje al tópico drones_to_engine
        producer.send('drons_to_engine', value=message)
        
        
        # Cerrar el productor
        producer.close()
        
        # Emitir el mensaje en pantalla
        print("Mensaje de Actualización enviado con ID de Dron: {self.id} ")



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
            kafka_thread = threading.Thread(target=dron.listen)
            kafka_thread.start()
        elif opcion == 4:
            print("Saliendo del programa.")
            break  # Esto terminará el bucle while
        else:
            print("Opcion invalida.")

def main():
    
    # Crear un nuevo dron con un id aleatorio entre 1 y 100
    dron = Dron()
    dron.set_id(89)
    dron.set_position((23,33))
    dron.send_update()
    
    # Enviar nuevamente la actualización
    #dron.send_update()

if __name__ == '__main__':
    #menu()
    main()