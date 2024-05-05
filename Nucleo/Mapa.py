import colorama

class Mapa:

    def __init__(self, size=20):
        self.size = size
        # Crear un mapa completamente en blanco primero
        self.grid = [[' ' for _ in range(size)] for _ in range(size)]
        
        # Luego, establece los bordes con puntos
        for i in range(size):
            self.grid[i][0] = '.'
            self.grid[i][size-1] = '.'
            self.grid[0][i] = '.'
            self.grid[size-1][i] = '.'
        
        self.drones_positions = {}  # Nuevo diccionario para llevar el seguimiento de las posiciones de los drones y su color
        self.dron_solapado = {} #Nuevo diccionario para llevar el seguimiento de los drones que se encuentran en la misma posicion

    def update_position(self, new_x, new_y, id, color):
        # Verifica si la posición está dentro del mapa
        if new_x < 0 or new_x >= self.size or new_y < 0 or new_y >= self.size:
            raise ValueError("Posición inválida")

        # Verifica si el dron ya está en el mapa y obtiene su posición anterior
        old_position = self.drones_positions.get(id)
        if old_position:
            old_x, old_y, _ = old_position
            # Si había un dron solapado en la posición anterior, lo vuelve a colocar 
            if (old_y, old_x) in self.dron_solapado:  # Cambia a (old_y, old_x)
                overlapped_id = self.dron_solapado.pop((old_y, old_x))
                if overlapped_id != id:
                    overlapped_color = self.drones_positions[overlapped_id][2]
                    self.place_drone(old_x, old_y, overlapped_id, overlapped_color)
            else:
                # Limpia la posición anterior si no hay solapamiento
                self.grid[old_y][old_x] = ' '  # Cambia a [old_y][old_x]

        # Verifica si hay un dron solapado con diferente ID en la nueva posición
        for other_id, (drone_x, drone_y, _) in self.drones_positions.items():
            if drone_x == new_x and drone_y == new_y and other_id != id:
                self.dron_solapado[(new_y, new_x)] = other_id  # Cambia a (new_y, new_x)

        # Actualiza el diccionario de posiciones de drones con la nueva posición y color
        self.drones_positions[id] = (new_x, new_y, color)

        # Llama a place_drone para actualizar la posición visualmente
        self.place_drone(new_x, new_y, id, color)

    def place_drone(self, x, y, id, color):
        # Convertir el ID a una cadena de dos caracteres, rellenando con un espacio a la izquierda si es necesario
        id_str = f"{id:2}"

        # Aplicar los códigos de color de colorama
        color_code = colorama.Back.RED if color == "rojo" else colorama.Back.GREEN
        text_color = colorama.Fore.WHITE if color == "rojo" else colorama.Fore.BLACK

        # Construir la representación del dron con el color y asegurarse de que todo sea de ancho fijo
        drone_representation = f"{color_code}{text_color}{id_str}{colorama.Style.RESET_ALL}"

        # Colocar la representación formateada del dron en la cuadrícula
        self.grid[y][x] = drone_representation




    def display(self):
        print()
        print()
        for row in self.grid:
            # Convertir todos los elementos de la fila a cadena
            print(' '.join(str(item) for item in row))
        print()


