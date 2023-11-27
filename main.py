# Librerias necesarias
import os
import pydgraph

# Importando model
from model import *
# Importando populate
from populate import *

# -------------------------- Haciendo la conexion con la base de datos -------------------------- #

# URI de Dgraph
dgraph_uri = os.getenv("DGRAPH_URI", "localhost:9080")

# ---------------------------------------- Función Menu ---------------------------------------- #

def print_menu():
    options = {
        1: "Crear Data",
        2: "Obtener Aeropuertos",
        3: "Obtener Conexiones de un Aeropuerto",
        4: "Obtener Conexion más lejana",
        5: "Obtener Conexion más cercana",
        6: "Cantidad de Conexiones que salen de un Aeropuerto",
        7: "Cantidad de Conexiones que llegan a un Aeropuerto",
        8: "Eliminar Datos"
    }
    print("---------- MENU ----------")
    for key, value in options.items():
        print(f"{key}. {value}")
    
# ---------------------------------------- Función Main ---------------------------------------- #

def main():
    while True:
        # Creando el cliente
        client_stub = pydgraph.DgraphClientStub(dgraph_uri)
        client = pydgraph.DgraphClient(client_stub)
        # Creación de la instancia de ModeloAeropuertos
        modelo = ModeloAeropuertos(client)

        # Establecer el esquema en la base de datos
        modelo.set_schema()
        print_menu()
        option = int(input("Ingrese una opción: "))

        if option == 1:
            # Agregando los aeropuertos
            modelo.add_multiple_airports(aeropuertos)
            # Agregando las conexiones
            modelo.add_multiple_connections(conexiones)
            print("\t- Datos creados exitosamente\n\n")

        elif option == 2:
            resp = modelo.get_all_airports()
            print('\t', resp)
            print("\n")

        elif option == 3:
            airport = input("\tIngrese el ID del aeropuerto: ")
            resp = modelo.get_airport_connections(airport)
            print('\t', resp)
            print("\n")
        
        elif option == 4:
            airport = input("\tIngrese el ID del aeropuerto: ")
            resp = modelo.get_furthest_conection(airport)
            print('\t', resp)
            print("\n")
        
        elif option == 5:
            airport = input("\tIngrese el ID del aeropuerto: ")
            resp = modelo.get_closest_conection(airport)
            print('\t', resp)
            print("\n")

        elif option == 6:
            airport = input("\tIngrese el ID del aeropuerto: ")
            resp = modelo.get_count_conection(airport)
            print('\t', resp)
            print("\n")
        
        elif option == 7:
            airport = input("\tIngrese el ID del aeropuerto: ")
            resp = modelo.get_count_conection_to(airport)
            print('\t', resp)
            print("\n")
        
        elif option == 8:
            resp = input("\t¿Está seguro que desea eliminar los datos? (SI/NO): ")
            if resp == "SI":
                modelo.drop_all(resp)
                print("\t- Datos eliminados exitosamente")
                print("\n")
            else:
                print("\t- Cancelando eliminación de datos")
                print("\n")

        else:
            client_stub.close()
            break
    
# ---------------------------------------- Ejecución Main ---------------------------------------- #

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f'Error: {e}')
