import colorama

class Mapa:
    def __init__(self, size=20):
        self.size = size
        self.grid = [['.' for _ in range(size)] for _ in range(size)]
        self.drones_positions = {}  # Nuevo diccionario para llevar el seguimiento de las posiciones de los drones
        self.id_solapado_verde = {} # Nuevo diccionario para llevar el seguimiento de las posiciones de los drones

    def update_position(self,new_x, new_y,id, color):
        # Cheque si la posición está dentro del mapa
        if new_x < 0 or new_x >= self.size or new_y < 0 or new_y >= self.size:
            raise ValueError("Posición inválida")
        # Chequea si el dron ya está en el mapa
        if id not in self.drones_positions:
            self.drones_positions[id] = (new_x, new_y)
            # Actualiza con el color correspondiente
            if color == "rojo":
                self.grid[new_x][new_y] = f"{colorama.Back.RED}{colorama.Fore.WHITE}{id}{colorama.Style.RESET_ALL}"
            elif color == "verde":
                self.grid[new_x][new_y] = f"{colorama.Back.GREEN}{colorama.Fore.BLACK}{id}{colorama.Style.RESET_ALL}"
            else:
                self.grid[new_x][new_y] = id
            return
        # Borra la posición anterior del dron si existe
        if id in self.drones_positions:
            old_x, old_y = self.drones_positions[id]
            self.grid[old_x][old_y] = '.'
        # Chequea si hay un dron en la nueva posición y su color es verde   
        if self.grid[new_x][new_y] != '.' and self.grid[new_x][new_y][0] == colorama.Back.GREEN:
                #guardamo en variable global la id del dron que estaba en el diccionario de drones solapados
                self.id_solapado[id] = self.grid[new_x][new_y]
                # Actualiza a amarillo si hay un dron en la nueva posición
                self.grid[new_x][new_y] = f"{colorama.Back.YELLOW}{colorama.Fore.BLACK}{id}{colorama.Style.RESET_ALL}"
        
        else:
            # Actualiza con el color correspondiente
            if color == "rojo":
                self.grid[new_x][new_y] = f"{colorama.Back.RED}{colorama.Fore.WHITE}{id}{colorama.Style.RESET_ALL}"
            elif color == "verde":
                self.grid[new_x][new_y] = f"{colorama.Back.GREEN}{colorama.Fore.BLACK}{id}{colorama.Style.RESET_ALL}"
            else:
                self.grid[new_x][new_y] = id

        # Actualiza el diccionario de posiciones de drones con la nueva posición
        self.drones_positions[id] = (new_x, new_y)

    def display(self):
        print()
        print()
        for row in self.grid:
            # Convertir todos los elementos de la fila a cadena
            print(' '.join(str(item) for item in row))
        print()

def main():
    
    # Inicializar la biblioteca colorama
    colorama.init(autoreset=True)

    # Ejemplo de uso:
    mapa = Mapa()
    mapa.update_position(1, 1,'A',  'rojo')
    mapa.update_position(2, 2,'B',  'verde')
    mapa.update_position( 1, 1,'C', 'verde')  # Esto hará que la posición (1,1) tenga un fondo azul
    mapa.update_position( 3, 3,'A', 'rojo')  # Mover el dron 'A' a una nueva posición y limpiar la antigua
    mapa.display()

if __name__ == '__main__':
    main()



