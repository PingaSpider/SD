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
CLIMATE_SERVICE_HOST = 'localhost'
CLIMATE_SERVICE_PORT = 7777
TEMPERATURE_COMFORTABLE_LOW = 17
TEMPERATURE_COMFORTABLE_HIGH = 25
TEMPERATURE_COLD_LIMIT = 5
TEMPERATURE_HOT_LIMIT = 38  


#################################################
# Funciones para el Engine AUTHENTICATION
#################################################
# Leer drones desde JSON
class Engine:

    def __init__(self):
        self.confirmed_drones = set()
        self.mapa = Mapa()
        self.city = None

    
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
            time.sleep(0.5)
        
    
    def load_drones(self):
        try:
            with open(FILENAME_DRONES, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return []
    

    def wrap_coordinates(self, x, y):
        """Envuelve las coordenadas según la geometría esférica del mapa."""
        #Si la coordenada x es 0 se envuelve a la derecha
        # Si la coordenada x es 20 se envuelve a la izquierda
        # Si la coordenada y es 0 se envuelve hacia abajo
        # Si la coordenada y es 20 se envuelve hacia arriba
        if x == 0:
            x = self.mapa.size - 2
        elif x == self.mapa.size - 1:
            x = 1
        if y == 0:
            y = self.mapa.size - 2
        elif y == self.mapa.size - 1:
            y = 1
        return x, y

    def handle_connection(self,conn, addr):
        print("Conectado por", addr)
        data = conn.recv(1024).decode()
        drone_data = json.loads(data)
        id_dron = drone_data["id"]
        token = drone_data["token"]
        drones_registered = self.load_drones()

        # Inicializar la variable response por adelantado
        response = {"status": "error", "message": "Dron no autenticado"}
        
        for dron in drones_registered:
            if dron["id"] == id_dron:
                if dron["token"] == token:
                    print(f"Dron {id_dron} autenticado exitosamente.")
                    response = {"status": "success", "message": "Autenticado"}
                    break  # Termina el bucle una vez que se autentica el dron
                else:
                    response = {"status": "error", "message": "Token incorrecto"}
                    print(f"Token incorrecto.")
                    break  # Termina el bucle si el token es incorrecto
        
        conn.sendall(json.dumps(response).encode())  # Envía la respuesta después de recorrer la lista de drones       

    
    def autenticate(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen()
            print("Engine escuchando en", PORT)
            
            while True:  # Mantener el Registry escuchando indefinidamente
                conn, addr = s.accept()
                with conn:
                    self.handle_connection(conn, addr)


    #LISTO
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
                    x_str, y_str = drone_info['POS'].split(',')
                    x = int(x_str)
                    y = int(y_str)
                    
                    # Usando wrap_coordinates para ajustar las coordenadas
                    x, y = self.wrap_coordinates(x, y)
                    
                    # Actualizar la posición en el drone_info
                    drone_info['POS'] = f"{x},{y}"
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
                print("Todos los drones han confirmado.")
                time.sleep(5)  # Espera un segundo antes de enviar la siguiente figura

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

    #LISTO    
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

    #LISTO
    #################################################
    #Comunicacion con el Servidor de Clima
    #################################################
    def get_weather(self, city):
        # Comunicarse con el servicio de clima y obtener la temperatura
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CLIMATE_SERVICE_HOST, CLIMATE_SERVICE_PORT))
            s.sendall(city.encode())
            data = s.recv(1024)
            weather_data = json.loads(data.decode())
            return weather_data

    def perform_action_based_on_weather(self, city):
        weather_data = self.get_weather(city)
        temperature = weather_data["temperature"]
        
        if temperature != "Unknown":
            print(f"La temperatura en {city} es de {temperature} grados.")
            
            # Si la temperatura está en el rango agradable, el show continúa.
            if TEMPERATURE_COMFORTABLE_LOW <= temperature <= TEMPERATURE_COMFORTABLE_HIGH:
                print("Las condiciones climáticas son agradables, el show continúa.")
            # Si la temperatura es demasiado fría o demasiado caliente, se recomienda terminar el show.
            elif temperature < TEMPERATURE_COLD_LIMIT or temperature > TEMPERATURE_HOT_LIMIT:
                print("Advertencia: condiciones climáticas extremas detectadas.")
                # Aquí enviaría la alerta al sistema relevante para tomar acción
                # Por ejemplo, podría ser un método que envía un mensaje a otro componente
                self.send_weather_alert(city, temperature)
            else:
                print("Las condiciones climáticas están fuera de lo ideal, pero no son extremas.")
        else:
            print("Datos del clima desconocidos.")

    def send_weather_alert(self, city, temperature):
        # Envía una alerta a un sistema o componente que maneja las operaciones del show
        print(f"Alerta: Condiciones climáticas no adecuadas en {city}. Temperatura: {temperature} grados.")
        # Aquí se podría implementar lógica para enviar una alerta real
        # Por ejemplo, a través de un socket a otro sistema o usando otro mecanismo de comunicación

    def check_weather_periodically(self, city):
        """Función que chequea el clima cada 10 segundos."""
        while True:
            # Aquí iría la lógica para chequear el clima
            print(f"Chequeando el clima para la ciudad: {city}")
            self.perform_action_based_on_weather(city)
            time.sleep(10)

    #LISTO
    def start_engine(self):
        # Iniciar los métodos en hilos separados

            # Asegúrate de obtener la ciudad antes de iniciar el hilo meter en self.ciudad
            self.city = input("Ingrese la ciudad: ")



            show_thread = threading.Thread(target=self.start_show)
            dron_update = threading.Thread(target=self.start_listening)
            dron_landed_confirmation = threading.Thread(target=self.listen_for_confirmations)
            map_display = threading.Thread(target=self.display_map)
            weather_thread = threading.Thread(target=self.check_weather_periodically, args=(self.city,))

            # Esto asegura que el hilo se cierra cuando el programa principal termina.
            show_thread.daemon = True
            dron_update.daemon = True
            dron_landed_confirmation.daemon = True
            map_display.daemon = True
            weather_thread.daemon = True  
        
            show_thread.start()
            dron_update.start()
            map_display.start()
            dron_landed_confirmation.start()
            weather_thread.start()

            

            show_thread.join()
            dron_update.join()
            dron_landed_confirmation.join()
            map_display.join()
            weather_thread.join()
    

            
            
    def menu(self):
        print("Bienvenido al motor de la aplicación")
        print("Seleccione una opción: ")
        print("1. Iniciar el motor")
        print("2. Salir")
        option = input("Ingrese la opción: ")
        if option == "1":
            self.start_engine()
        elif option == "2":
            print("Saliendo del motor...")
            exit()
        else:
            print("Opción inválida")
            self.menu()

#main de prueba
def main():  
    try:
        engine = Engine()
        engine.menu()
        # Otros llamados o lógica necesaria
    except KeyboardInterrupt:
        print("Deteniendo el motor y limpiando...")
        engine.stop_engine()  # Suponiendo que tienes un método para detener el motor
        # Aquí puedes realizar cualquier limpieza necesaria antes de cerrar el programa
    finally:
        print("Programa terminado.")


if __name__ == "__main__":
    main()




