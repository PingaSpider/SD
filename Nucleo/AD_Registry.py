import json
import socket
import uuid

# Direcciones y puertos
HOST = 'localhost'
PORT = 4444
FILENAME = 'drones_registry.json'

# Cargar drones desde el archivo JSON
def load_drones():
    try:
        with open(FILENAME, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return []

# Guardar lista de drones en el archivo JSON
def save_drones(drones):
    with open(FILENAME, 'w') as file:
        json.dump(drones, file)

# Registrar un nuevo dron
def register_dron(token):
    drones = load_drones()
    if token not in drones:
        drones.append(token)
        save_drones(drones)
        return True
    return False

import uuid
import json

FILENAME = 'drones_registry.json'

def load_drones():
    try:
        with open(FILENAME, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return []

def save_drones(drones):
    with open(FILENAME, 'w') as file:
        json.dump(drones, file)

def get_token_for_dron(dron_id):
    drones = load_drones()
    
    # Buscar si el dron ya está registrado
    for dron in drones:
        if dron["id"] == dron_id:
            return dron["token"]
    
    # Si no está registrado, generar un nuevo token único
    existing_tokens = [dron["token"] for dron in drones]
    new_token = str(uuid.uuid4())
    while new_token in existing_tokens:
        new_token = str(uuid.uuid4())

    # Añadir el nuevo dron y su token al registro
    drones.append({"id": dron_id, "token": new_token})
    save_drones(drones)
    
    return new_token

def register_dron(dron_id):
    drones = load_drones()
    if dron_id not in drones:
        return True
    return False

def handle_connection(conn, addr):
    print("Conectado por", addr)
    
    # Recibe el ID del dron desde la conexión
    id_dron = conn.recv(1024).decode()
    
    # Obtiene el token asociado al ID del dron
    token = get_token_for_dron(id_dron)
    
    # Verifica si el dron ya está registrado
    if token is not None:
        print(f"Dron {token} ya estaba registrado.")
        response = {"status": "error", "message": "Ya registrado"}
    else:
        # Registra el dron y obtiene un nuevo token
        new_token = register_dron(id_dron)
        print(f"Dron {new_token} registrado exitosamente.")
        response = {"status": "success", "token": new_token, "message": "Registrado"}
    
    # Envía la respuesta al dron
    conn.sendall(json.dumps(response).encode())



# Creación de socket y vinculación
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("Registry escuchando en", PORT)
    
    while True:  # Mantener el Registry escuchando indefinidamente
        conn, addr = s.accept()
        with conn:
            handle_connection(conn, addr)

