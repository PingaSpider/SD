import json
import socket
import time
import threading
import os
from Mapa import Mapa
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
class Engine:

    def __init__(self):
        self.confirmed_drones = set()
        self.mapa = Mapa()

    
    def update_map(self, message):
        # Extraer los datos del mensaje
        drone_id = message['ID_DRON']
        x = message['COORDENADA_X_ACTUAL']
        y = message['COORDENADA_Y_ACTUAL']
        color = message['COLOR']
        # Actualizar la posición en el mapa con los datos del dron
        self.mapa.update_position(x, y, drone_id, color)



    def display_map(self):
        while True:
            self.mapa.display()
            time.sleep(1)
        
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
    def start_show(self):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092', 
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        try:

            # Cargar el archivo JSON
            with open(FILENAME_FIGURAS, 'r') as file:
                data = json.load(file)
            
            # Iterar sobre las figuras en el JSON y enviar información al dron
            for figure in data['figuras']:
                #saber la cantidad de drones que se necesitan para la figura y guardarla en una variable
                drones_needed = len(figure['Drones'])
                for drone_info in figure['Drones']:
                    message = {
                        'ID_DRON': drone_info['ID'],
                        'COORDENADAS': drone_info['POS']
                    }
                    producer.send('engine_to_drons', value=message)
                    #asegurarse del que el productor solo mande el mensaje una vez
                    producer.flush()
                while not self.all_drones_confirmed(drones_needed):
                    time.sleep(1)  # espera un segundo y vuelve a verificar
                    print("Esperando confirmaciones...")

                # Una vez que todos los drones han confirmado, limpia el conjunto para la siguiente figura
                self.confirmed_drones.clear()

        except Exception as e:
            print(f"Error: {e}")
            # Aquí puedes manejar errores adicionales o emitir un mensaje según lo necesites.
        
        # Asegurarse de que todos los mensajes se envían
        producer.flush()
        producer.close()

    def all_drones_confirmed(self,drones_needed):
            print(f"CONFIRMED DRONES: {len(self.confirmed_drones)} - DRONES NEEDED: {drones_needed}")
            return len(self.confirmed_drones) == drones_needed

    def listen_for_confirmations(self):
        consumer = KafkaConsumer('listen_confirmation',
                                    bootstrap_servers='localhost:9092',
                                    value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        for message in consumer:
                if message.value['STATUS'] == 'LANDED':
                    drone_id = message.value['ID_DRON']
                    self.confirmed_drones.add(drone_id)
                    
        
        consumer.commit()
        consumer.close()
           
    #################################################
    #Funciones para escuhar los Drones
    #################################################
    def start_listening(self):
        # Inicializar el consumidor de Kafka
            consumer = KafkaConsumer('drons_to_engine',
                                        group_id='engine',
                                        bootstrap_servers='localhost:9092',
                                        auto_offset_reset='earliest',
                                        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

            for message in consumer:
                self.process_message(message.value)

            consumer.commit()
            consumer.close()

    #LISTO
    def load_last_updates(self):
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
    def save_drones(self,drones):
        with open(FILENAME_ACTUALIZACIONES, 'w') as file:
            json.dump(drones, file, indent=4)

    #LISTO
    def process_message(self,message):
        # Extraer datos del mensaje
        #print(f"Received message structure: {message}")
        ID_DRON = message['ID_DRON']
        COORDENADA_X_ACTUAL = message['COORDENADA_X_ACTUAL']
        COORDENADA_Y_ACTUAL = message['COORDENADA_Y_ACTUAL']
        ESTADO_ACTUAL = message['ESTADO_ACTUAL']
        
        drones = self.load_last_updates()
        dron_found = False

        # Buscar si el dron ya está registrado y actualizar sus datos si es necesario
        for dron in drones:
            if dron["ID_DRON"] == ID_DRON:
                dron["COORDENADA_X_ACTUAL"] = COORDENADA_X_ACTUAL
                dron["COORDENADA_Y_ACTUAL"] = COORDENADA_Y_ACTUAL
                dron["ESTADO_ACTUAL"] = ESTADO_ACTUAL
                dron_found = True
                #Terminar el bucle una vez que se actualiza el dron
                break

        # Si el dron no fue encontrado en la lista, añadirlo
        if not dron_found:
            drones.append({
                "ID_DRON": ID_DRON,
                "COORDENADA_X_ACTUAL": COORDENADA_X_ACTUAL,
                "COORDENADA_Y_ACTUAL": COORDENADA_Y_ACTUAL,
                "ESTADO_ACTUAL": ESTADO_ACTUAL
            })
            print(f"Nuevo dron registrado en el archivo de actualizaciones. ID: {ID_DRON}")


        #Si el estado actual del dron es LANDED O FLYING, actualizar el mapa
        if ESTADO_ACTUAL == "MOVING" or ESTADO_ACTUAL == "LANDED":
            self.update_map(message)


        self.save_drones(drones)  # Guardar la lista actualizada de drones


    def start_engine(self):
        # Iniciar los métodos en hilos separados
            thread1 = threading.Thread(target=self.start_show)
            thread2 = threading.Thread(target=self.start_listening)
            thread3 = threading.Thread(target=self.listen_for_confirmations)
            thread4 = threading.Thread(target=self.display_map)
        
            thread4.start()
            thread1.start()
            thread2.start()
            thread3.start()

            thread4.join()
            thread1.join()
            thread2.join()
            thread3.join()
            


#main de prueba
def main():  
    try:
        engine = Engine()
        engine.start_engine()
        # Otros llamados o lógica necesaria
    except KeyboardInterrupt:
        print("Deteniendo el motor y limpiando...")
        engine.stop_engine()  # Suponiendo que tienes un método para detener el motor
        # Aquí puedes realizar cualquier limpieza necesaria antes de cerrar el programa
    finally:
        print("Programa terminado.")


if __name__ == "__main__":
    main()




