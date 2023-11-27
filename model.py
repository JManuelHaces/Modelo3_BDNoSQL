# Importando librerias necesarias
import json
import pydgraph

# Importando el schema
from schema import schema

# ------------------------------------------- Clase ------------------------------------------- #

class ModeloAeropuertos:
    def __init__(self, client: pydgraph.DgraphClient) -> None:
        """
        Clase que usa la base de datos Dgraph para almacenar los datos de los aeropuertos.
        :param client: Cliente de Dgraph.
        """
        # Poniendo el cliente en la clase
        self.client = client
    
    def set_schema(self) -> None:
        """
        Método que crea el schema de la base de datos.
        """
        # Alterando el schema
        (self.client).alter(pydgraph.Operation(schema=schema))

    def add_airport(self, airport_id: str, airport_name: str, city: str, country: str) -> None:
        """
        Método para agregar un aeropuerto a la base de datos.
        :param airport_id: ID único del aeropuerto.
        :param airport_name: Nombre del aeropuerto.
        :param city: Ciudad del aeropuerto.
        :param country: País del aeropuerto.
        """
        txn = self.client.txn()
        try:
            aeropuerto = {
                'uid': '_:aeropuerto',
                'dgraph.type': 'Aeropuerto',
                'airport_id': airport_id,
                'airport_name': airport_name,
                'city': city,
                'country': country
            }
            txn.mutate(set_obj=aeropuerto)
            txn.commit()
        finally:
            txn.discard()
    
    def add_multiple_airports(self, airports_list: list) -> None:
        """
        Método para agregar múltiples aeropuertos a la base de datos.
        :param airports_list: Lista de diccionarios, cada uno representando un aeropuerto.
        """
        for airport in airports_list:
            self.add_airport(airport['airport_id'], airport['airport_name'], airport['city'], airport['country'])

    def add_connection(self, from_airport_id: str, to_airport_id: str, distance: float) -> None:
        """
        Método para agregar una conexión entre dos aeropuertos.
        :param from_airport_id: ID del aeropuerto de origen.
        :param to_airport_id: ID del aeropuerto de destino.
        :param distance: Distancia entre los aeropuertos.
        """
        # Consulta para obtener los UIDs de ambos aeropuertos
        query = """
        {
            a(func: eq(airport_id, "%s")) {
                uid_a: uid
            }
            b(func: eq(airport_id, "%s")) {
                uid_b: uid
            }
        }
        """ % (from_airport_id, to_airport_id)

        txn = self.client.txn()
        try:
            # Ejecutar la consulta
            response = txn.query(query)
            data = json.loads(response.json.decode('utf-8'))

            # Verificar que ambos aeropuertos existen y obtener sus UIDs
            if 'a' in data and data['a'] and 'b' in data and data['b']:
                uid_a = data['a'][0]['uid_a']
                uid_b = data['b'][0]['uid_b']

                # Crear la mutación usando los UIDs obtenidos
                mutation = """
                <{0}> <connection> <{1}> (distance = {2}) .
                """.format(uid_a, uid_b, distance)

                # Ejecutar la mutación
                txn.mutate(set_nquads=mutation)
                txn.commit()
            else:
                print("Uno o ambos aeropuertos no encontrados.")
        finally:
            txn.discard()
    
    def add_multiple_connections(self, connections_list: list) -> None:
        """
        Método para agregar múltiples conexiones entre aeropuertos.
        :param connections_list: Lista de diccionarios, cada uno representando una conexión.
        """
        for connection in connections_list:
            self.add_connection(connection['from'], connection['to'], connection['distance'])
    
    def drop_all(self, confirmacion: str) -> None:
        """
        Método para eliminar todos los datos de la base de datos.
        :param confirmacion: Confirmación de la eliminación de los datos.
        """
        if confirmacion == "SI":
            self.client.alter(pydgraph.Operation(drop_all=True))
        else:
            print("No se eliminaron los datos.")

    def get_all_airports(self) -> None:
        """
        Método para obtener todos los aeropuertos de la base de datos.
        """
        query = """
        {
            aeropuertos(func: type(Aeropuerto)) {
                airport_id
                airport_name
                city
                country
            }
        }
        """
        response = self.client.txn(read_only=True).query(query)
        return json.loads(response.json)['aeropuertos']

    def get_airport_connections(self, airport_id: str) -> None:
        """
        Método para obtener las conexiones de un aeropuerto específico junto con la distancia.
        :param airport_id: ID del aeropuerto del cual se quieren obtener las conexiones.
        :return: Lista de conexiones y distancias.
        """
        query = """
        {
            aeropuerto(func: eq(airport_id, "%s")) {
                connection @facets(distance) {
                    airport_id
                    airport_name
                }
            }
        }
        """ % airport_id

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(query)
            data = json.loads(response.json)
            # Verificar si se encontraron conexiones
            if 'aeropuerto' in data and data['aeropuerto']:
                return data['aeropuerto'][0]['connection']
            else:
                return []
        finally:
            txn.discard()
    
    def get_furthest_conection(self, airport_id: str) -> None:
        """
        Método para obtener la conexión más larga.
        :param airport_id: ID del aeropuerto del cual se quieren obtener las conexiones.
        """
        query = """
        {
            aeropuerto(func: eq(airport_id, "%s")) {
                connection @facets(distance) {
                    airport_id
                    airport_name
                }
            }
        }
        """ % airport_id

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(query)
            data = json.loads(response.json)
            # Verificar si se encontraron conexiones
            if 'aeropuerto' in data and data['aeropuerto']:
                # Obtener la conexión más larga
                return max(data['aeropuerto'][0]['connection'], key=lambda x: x['connection|distance'])
            else:
                return []
        finally:
            txn.discard()
    
    def get_closest_conection(self, airport_id: str) -> None:
        """
        Método para obtener la conexión más corta.
        :param airport_id: ID del aeropuerto del cual se quieren obtener las conexiones.
        """
        query = """
        {
            aeropuerto(func: eq(airport_id, "%s")) {
                connection @facets(distance) {
                    airport_id
                    airport_name
                }
            }
        }
        """ % airport_id

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(query)
            data = json.loads(response.json)
            # Verificar si se encontraron conexiones
            if 'aeropuerto' in data and data['aeropuerto']:
                # Obtener la conexión más corta
                return min(data['aeropuerto'][0]['connection'], key=lambda x: x['connection|distance'])
            else:
                return []
        finally:
            txn.discard()

    def get_count_conection(self, airport_id: str) -> None:
        """
        Método para obtener la cantidad de conexiones de un aeropuerto.
        :param airport_id: ID del aeropuerto del cual se quieren obtener las conexiones.
        """
        response = self.get_airport_connections(airport_id)
        return f'Cantidad de conexiones que tiene el {airport_id}: {len(response)}'
    
    def get_count_conection_to(self, airport_id: str) -> None:
        """
        Método para obtener la cantidad de conexiones que llegan a un aeropuerto.
        :param airport_id: ID del aeropuerto del cual se quieren obtener las conexiones.
        """
        query = """
        {
            aeropuerto(func: eq(airport_id, "%s")) {
                ~connection @facets(distance) {
                    airport_id
                    airport_name
                }
            }
        }
        """ % airport_id

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(query)
            data = json.loads(response.json)
            # Verificar si se encontraron conexiones
            if 'aeropuerto' in data and data['aeropuerto']:
                return f'Cantidad de conexiones que llegan al {airport_id}: {len(data["aeropuerto"][0]["~connection"])}'
            else:
                return []
        finally:
            txn.discard()
