# Drone.py
import socket
import json
import time
import colorama
from colorama import Fore, init
init(autoreset=True)


class Drone:
    STATES = ["waiting", "moving", "landed"]
    PULSE_INTERVAL = 10  # 10 segundos

    def __init__(self, drone_id, engine_ip="localhost", engine_port=5555, registry_port=4444):
        self.drone_id = drone_id
        self.position = (1, 1)
        self.destination = None  # Se establecerá cuando el Engine lo indique
        self.state = "waiting"
        self.engine_ip = engine_ip
        self.engine_port = engine_port
        self.registry_port = registry_port

    def _send_data(self, data, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if port == self.engine_port:
                s.connect((self.engine_ip, self.engine_port))
            else:
                s.connect((self.engine_ip, self.registry_port))
            s.sendall(json.dumps(data).encode('utf-8'))

    def set_destination(self, x, y):
        self.destination = (x, y)
        self.state = "moving"
    
    def move_step(self):
        if not self.destination:
            print("No destination set")
            return

        x_diff = self.destination[0] - self.position[0]
        y_diff = self.destination[1] - self.position[1]

        move_x = 0
        move_y = 0

        if x_diff != 0:
            move_x = 1 if x_diff > 0 else -1
        elif y_diff != 0:
            move_y = 1 if y_diff > 0 else -1
        
        # Actualizar posición
        self.position = (self.position[0] + move_x, self.position[1] + move_y)
        
        # Verificar si ha llegado a su destino
        if self.position == self.destination:
            self.state = "landed"

        # Continuación de la clase Drone...
    
    def listen_for_commands(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.engine_ip, self.engine_port))
            s.listen()
            print(f"Drone {self.drone_id} listening for commands on {self.engine_port}...")
            conn, addr = s.accept()
            with conn:
                print(f"Connected by: {addr}")
                data = conn.recv(1024)
                if data:
                    command = json.loads(data.decode('utf-8'))
                    if 'destination' in command:
                        x, y = command['destination']
                        self.set_destination(x, y)
                        while self.state != "landed":
                            self.move_step()
                            print(f"Drone {self.drone_id} moved to position {self.position}")
                            time.sleep(1)  # Espera un segundo antes de mover de nuevo
                        self.state = "waiting"
                        print(f"Drone {self.drone_id} has landed at destination.")
    
    def run(self):
        while True:
            self.listen_for_commands()

    def print_status(self):
        if self.state == "waiting":
            print(Fore.BLUE + f"Drone {self.drone_id} is waiting.")
        elif self.state == "moving":
            print(Fore.RED + f"Drone {self.drone_id} is moving to {self.destination}. Current position: {self.position}.")
        elif self.state == "landed":
            print(Fore.GREEN + f"Drone {self.drone_id} has landed at {self.destination}.")
    def set_state(self, state):
        self.state = state
        color_mapping = {
            "waiting": "blue",
            "moving": "red",
            "landed": "green"
        }
        self.color = color_mapping[state]

    def move(self, x, y):
        self.x = x
        self.y = y
        self.set_state("moving")

    def land(self):
        self.set_state("landed")

    def listen_for_pulse(self):
        last_pulse_time = time.time()
        while True:
            time.sleep(PULSE_INTERVAL / 2)  # Simulación, en realidad se manejaría con un socket
            current_time = time.time()
            if current_time - last_pulse_time >= PULSE_INTERVAL:
                self.handle_connection_loss()
            last_pulse_time = current_time

    def handle_connection_loss(self):
        print("Pérdida de conexión detectada.")
        self.restart_and_authenticate()

    def restart_and_authenticate(self):
        self.x = 1
        self.y = 1
        self.set_state("waiting")
        self.authenticate_with_engine()

    def authenticate_with_engine(self):
        try:
            self.engine_socket.connect(('localhost', 5555))
            print("Dron autenticado.")
            # Aquí podrías enviar/recibir mensajes específicos para la autenticación si es necesario.
        except socket.error:
            print("Error al autenticar con el engine.")



if __name__ == "__main__":
    drone = Drone("drone_1")
    drone.run()


    
