import socket
import json
import threading
import random
import time
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
STATES = ["WAITING", "MOVING", "LANDED"]
COLORS = ["rojo", "verde"]
class Dron:
    
    def __init__(self):
        # Asigna el siguiente ID disponible al dron y luego incrementa el contador
        self.id = 0
        self.token = None  # Inicialmente, el dron no tiene un token hasta que se registre
        self.state = STATES[0] # Inicialmente, el dron está en estado "waiting"
        self.position = (1,1) # Inicialmente, el dron está en la posición (1,1)
        self.color = COLORS[0] # Inicialmente, el dron está en estado "waiting"
        self.showState = True
    
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

    #LISTO
    # Método para registrar el dron en el Registry
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
                        self.token = response['token']
                        print(f"Dron {self.id} ya estaba registrado.")
                    else:
                        print(f"Error al registrar el Dron {self.id}.")
                        return False
            except json.JSONDecodeError:  # Si hay un error al decodificar el JSON, manejarlo
                print("Respuesta del Registry no es un JSON válido.")
                return False


    #Metodo para autenticar el dron con el Engine (usar el token) y
    def autenticate(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST_ENGINE, PORT_ENGINE))
                reg_msg = {'id': self.id, 'token': self.token}
                s.sendall(json.dumps(reg_msg).encode())
                
                data = s.recv(1024)
                if not data:  # Si no se recibe respuesta, imprimir un error y retornar False
                    print("No se recibió respuesta del Engine.")
                    return False
                
                try:
                    response = json.loads(data.decode())
                    if response['status'] == 'success':
                        print(f"Autenticado exitosamente")
                        return True
                    else:
                        print(f"Error al Autenticar.")
                        return False
                    
                except json.JSONDecodeError:  # Si hay un error al decodificar el JSON, manejarlo
                    print("Respuesta del Registry no es un JSON válido.")
                    return False
        except Exception as e:
            print(f"Error en la conexión con el Engine: {e}")
            return False


#################################################
#Comunicacion del Engine con el Dron
#################################################

#LISTO
# El dron necesita saber su propio ID para suscribirse al tópico correcto.
    def recive_data(self):
        try:
            # Inicializar el consumidor de Kafka
            consumer = KafkaConsumer(
                'engine_to_drons',
                group_id=f"dron_group_{self.id}",
                bootstrap_servers='localhost:9092',
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            # Consumir mensajes de Kafka
            for message in consumer:
                self.process_message(message)
                consumer.commit()
        except Exception as e:
            print(f"Error en la conexión de Kafka: {e}")
            # Aquí puedes cambiar al estado que hayas decidido para el dron.
    
    # Función que procesa el mensaje y llama al método run
    def process_message(self,message):
        data = message.value
        if data['ID_DRON'] == self.id:
            ID_DRON = data['ID_DRON']
            COORDENADAS = data['COORDENADAS'].split(",")
            COORDENADA_X_OBJETIVO = int(COORDENADAS[0])
            COORDENADA_Y_OBJETIVO = int(COORDENADAS[1])
            # Si el ID del dron en el mensaje coincide con el ID del dron actual, procesar el mensaje
            self.run((COORDENADA_X_OBJETIVO, COORDENADA_Y_OBJETIVO))
    
    # Método para mover el dron un paso hacia el objetivo
    def move_one_step(self, target_position):
        # Descomponer las coordenadas actuales y objetivo
        x_current, y_current = self.position
        x_target, y_target = target_position

        # Determinar la dirección del movimiento en el eje X
        if x_current < x_target:
            x_current += 1
        elif x_current > x_target:
            x_current -= 1

        # Determinar la dirección del movimiento en el eje Y
        if y_current < y_target:
            y_current += 1
        elif y_current > y_target:
            y_current -= 1

        # Actualizar la posición del dron
        self.position = (x_current, y_current)

    # Método para mover el dron a las coordenadas objetivo    
    def run(self, target_position):
    # Moverse hacia la posición objetivo un paso a la vez
        self.state = STATES[1]
        self.color = COLORS[0]
        while self.position != target_position:
            self.move_one_step(target_position)
            print(f"P:{self.position}, S: {self.state}, M: {target_position}")
            
            # Enviar una actualización al engine después de cada movimiento
            time.sleep(0.5)
            self.send_update()

            # Pausa de un segundo
            time.sleep(1)
            if self.showState == False:
                break
        
        if self.showState == False:
            self.backTobase()
        
        self.state = STATES[2]
        self.color = COLORS[1]
        self.send_update()
        self.send_confirmation()
    
    def backTobase(self):
        self.state = STATES[1]
        self.showState = True
        self.run((1, 1))  # Corregido aquí
        print("BACK TO BASE")
        self.showState = True
        self.state = STATES[0]
        
    
    def endShow(self):
        #Crear consumidor con el topic end_show y el id del dron
        consumer = KafkaConsumer(
            'end_show',
            group_id=f"dron_group_{self.id}",
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        # Consumir mensajes de Kafka
        for message in consumer:
            data = message.value
            # message = {'END_SHOW': 'True'}
            if data['END_SHOW'] == 'True':
                self.showState = False
                self.backTobase()
                print("ENDING SHOW")
                break
            consumer.commit()
        consumer.close()

        print("SHOW END")

        #Volver al menu
        self.menu()

    

#################################################


        
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
            'ESTADO_ACTUAL': self.state,
            'COLOR': self.color
        }
        
        # Enviar el mensaje al tópico drones_to_engine
        producer.send('drons_to_engine', value=message)
        
        
        # Cerrar el productor
        producer.close()
        
        # Emitir el mensaje en pantalla
        print("SEND_UPDATE ID de Dron: ", self.id)
    
    # Método para enviar una confirmación al Engine
    def send_confirmation(self):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        message = {
            'ID_DRON': self.id,
            'STATUS': self.state,
            'COORDENADAS': f"{self.position[0]},{self.position[1]}",
            'COLOR': self.color       
        }
        producer.send('listen_confirmation', value=message)
        producer.close()  # Asegúrate de cerrar el productor cuando hayas terminado.

        print(f"SEND_CONFIRMATION: {self.state}")
        print("Esperando instrucciones...")
####################################################################
    def run_dron(self):
        # Iniciar los métodos en hilos separados
        thread1 = threading.Thread(target=self.recive_data)
        thread2 = threading.Thread(target=self.send_update)
        thread3 = threading.Thread(target=self.send_confirmation)
        thread4 = threading.Thread(target=self.endShow)

        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()




    def menu(self):

        while True:
            print()
            print()
            print("Seleccione una opcion:")
            print("1. Registrar el dron en el Registry")
            print("2. Autenticar el dron con el Engine")
            print("3. Comenzar a escuchar instrucciones")
            print("4. Salir")
            opcion = int(input("Seleccione una opcion: "))

            if opcion == 1:
                self.register()
            elif opcion == 2:
                self.autenticate()
            elif opcion == 3:
                self.run_dron()        
            elif opcion == 4:
                print("Saliendo del programa.")
                #salir del programa
                exit()
            else:
                print("Opcion invalida.")

def main():
    dron = Dron()
    print("Selecciona un ID de dron entre 1 y 100: ")
    dron.set_id(int(input()))
    # Crear un nuevo dron con un id aleatorio entre 1 y 100
    dron.menu()
    
    # Enviar nuevamente la actualización
    #dron.send_update()

if __name__ == '__main__':
    #menu()
    main()