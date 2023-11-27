schema = """
type Aeropuerto {
    airport_id
    airport_name
    city
    country
    connection
}

airport_id: string @index(exact) .
airport_name: string @index(fulltext) .
city: string @index(fulltext) .
country: string @index(fulltext) .
connection: [uid] @reverse .
distance: float .
"""
