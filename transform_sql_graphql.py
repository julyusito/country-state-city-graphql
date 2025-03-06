#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import re
import os
import csv

# Cantidades esperadas
EXPECTED_COUNTRIES = 246
EXPECTED_STATES = 4091
EXPECTED_CITIES = 47940

# Para almacenar las filas descartadas (que no cumplen las columnas esperadas)
SKIPPED_ROWS = {
    'countries': [],
    'states': [],
    'cities': []
}

def parse_sql_file(sql_file):
    """
    Lee el contenido de <sql_file> y retorna (table_name, columns, values):
      - table_name (str): "cities", "countries" o "states".
      - columns (list[str]): las columnas declaradas en el INSERT (p.ej. ["id","name","state_id"]).
      - values (list[list[str]]): todas las filas parseadas (sin filtrar).

    Flujo:
      1) Detecta "CREATE TABLE" (soporta IF NOT EXISTS y backticks).
      2) Busca las sentencias "INSERT INTO <table> (col1, col2, ...) VALUES (...)"
         y extrae todos los bloques de tuplas.
      3) Usa extract_tuples_respecting_quotes(...) para dividir las tuplas a nivel superior,
         respetando paréntesis dentro de comillas.
      4) Usa split_by_comma_robust(...) para dividir cada tupla en columnas (respetando comillas escapadas).
    """

    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1) Encontrar CREATE TABLE
    create_table_match = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(',
        content,
        re.IGNORECASE | re.DOTALL
    )
    if not create_table_match:
        raise ValueError(f"No se encontró CREATE TABLE en {sql_file}")

    table_name = create_table_match.group(1)

    # 2) Buscar INSERT INTO <table> (col1, col2, ...) VALUES ...
    insert_regex = re.compile(
        rf"INSERT\s+INTO\s+`?{table_name}`?\s*\(([^)]*)\)\s*VALUES\s*(.*?);",
        re.IGNORECASE | re.DOTALL
    )

    all_columns = None
    all_values = []

    for match in insert_regex.finditer(content):
        columns_str = match.group(1).strip()  
        values_block = match.group(2).strip()

        # Quitar backticks en columns
        cols = [c.strip().strip('`') for c in columns_str.split(',')]
        if all_columns is None:
            all_columns = cols
        # Podrías chequear si all_columns != cols en caso de que cambie a mitad de archivo

        # 3) Extraer las tuplas principales del bloque "VALUES (...)"
        tuple_strings = extract_tuples_respecting_quotes(values_block)

        for tup_str in tuple_strings:
            # Ej: "774, 'Cocos (Keeling) Islands', 46"
            row_values = split_by_comma_robust(tup_str)
            # Limpieza final
            clean_row = [v.strip().strip("'").strip('"') for v in row_values]
            all_values.append(clean_row)

    return table_name, all_columns, all_values


def extract_tuples_respecting_quotes(values_block):
    """
    Dado algo como:
      "(1, 'Afghanistan'),(2, 'Albania'),(3, 'Algeria')"
    extrae cada tupla de nivel superior ignorando las comas que separan
    dichas tuplas.

    Retorna una lista de strings, por ejemplo:
      [
        "1, 'Afghanistan'",
        "2, 'Albania'",
        "3, 'Algeria'"
      ]
    que luego se separan por comas con split_by_comma_robust.
    """
    results = []
    current = []
    paren_depth = 0
    in_single_quote = False
    in_double_quote = False
    backslash_count = 0

    i = 0
    length = len(values_block)
    while i < length:
        char = values_block[i]

        if char == '\\':
            backslash_count += 1
            current.append(char)
            i += 1
            continue

        # Manejo de comillas
        if char == "'" and not in_double_quote:
            if backslash_count % 2 == 0:
                in_single_quote = not in_single_quote
            current.append(char)
            backslash_count = 0
        elif char == '"' and not in_single_quote:
            if backslash_count % 2 == 0:
                in_double_quote = not in_double_quote
            current.append(char)
            backslash_count = 0

        # Manejo de paréntesis
        elif char == '(' and not in_single_quote and not in_double_quote:
            paren_depth += 1
            current.append(char)
            backslash_count = 0
        elif char == ')' and not in_single_quote and not in_double_quote:
            paren_depth -= 1
            current.append(char)
            backslash_count = 0

            # Si paren_depth llegó a 0, terminamos una tupla
            if paren_depth == 0:
                tuple_str = ''.join(current)
                # Ej: "(1, 'Afghanistan')"
                # Quitamos el '(' y ')' exteriores
                if tuple_str.startswith('(') and tuple_str.endswith(')'):
                    tuple_str = tuple_str[1:-1]
                results.append(tuple_str.strip())
                current = []

                # Ahora, SALTAR comas/espacios sueltos fuera de paréntesis
                i += 1
                while i < length:
                    # mira el siguiente char
                    nxt = values_block[i]
                    # si vemos '(' => es la siguiente tupla
                    if nxt == '(':
                        # no lo consumimos, para el siguiente loop lo trata
                        break
                    # si es una coma o espacio, lo saltamos
                    elif nxt in [',', ' ', '\n', '\t', '\r']:
                        i += 1
                        continue
                    else:
                        # si es otra cosa, la ignoramos o la pegamos
                        # en general, no debería pasar
                        i += 1
                continue  # saltar el resto del while, que es i += 1
        else:
            # algún caracter normal
            current.append(char)
            backslash_count = 0

        i += 1

    return results



def split_by_comma_robust(value_string):
    """
    Dado un string como "774, 'Cocos (Keeling) Islands', 46",
    lo divide en ["774", "'Cocos (Keeling) Islands'", "46"], 
    respetando comillas (simples/dobles), comillas escapadas con backslash,
    y no rompiendo por comas dentro de comillas.
    """
    result = []
    current = []
    in_single_quote = False
    in_double_quote = False
    backslash_count = 0

    for char in value_string:
        if char == '\\':
            backslash_count += 1
            current.append(char)
        else:
            if char == "'" and not in_double_quote:
                if backslash_count % 2 == 0:
                    in_single_quote = not in_single_quote
                current.append(char)
                backslash_count = 0
            elif char == '"' and not in_single_quote:
                if backslash_count % 2 == 0:
                    in_double_quote = not in_double_quote
                current.append(char)
                backslash_count = 0
            elif char == ',':
                # solo separa si estamos fuera de comillas
                if not in_single_quote and not in_double_quote:
                    val = ''.join(current)
                    result.append(val)
                    current = []
                else:
                    current.append(char)
                backslash_count = 0
            else:
                current.append(char)
                backslash_count = 0

    if current:
        val = ''.join(current)
        result.append(val)

    return result


def main():
    """
    Uso:
      python transformar.py cities.sql countries.sql states.sql

    Genera:
      - schema.graphql
      - appsync_mutations.graphql
      - processed_cities.csv
      - processed_states.csv
      - processed_countries.csv
    """
    if len(sys.argv) < 4:
        print("Uso: python transformar.py <cities.sql> <countries.sql> <states.sql>")
        sys.exit(1)

    files = sys.argv[1:4]
    table_data = {
        'cities': None,
        'countries': None,
        'states': None
    }

    # Parsear cada archivo
    for fpath in files:
        tname, cols, vals = parse_sql_file(fpath)
        lt = tname.lower()
        if lt not in table_data:
            print(f"[WARNING] La tabla '{tname}' no es cities/countries/states. Se omite.")
        else:
            table_data[lt] = (cols, vals)

    # Ver si falta algo
    missing = [t for t in ('cities','countries','states') if table_data[t] is None]
    if missing:
        raise ValueError(f"No se pudo parsear la(s) tabla(s): {missing}")

    # Extraer
    city_columns, city_values = table_data['cities']
    country_columns, country_values = table_data['countries']
    state_columns, state_values = table_data['states']

    # Filtrar filas que no cumplan con la cantidad de columnas
    country_values_ok = []
    for row in country_values:
        if len(row) < 2:
            print(f"[WARNING] Fila inválida en countries (2 col). Omitida: {row}")
            SKIPPED_ROWS['countries'].append(row)
        else:
            country_values_ok.append(row)

    state_values_ok = []
    for row in state_values:
        if len(row) < 3:
            print(f"[WARNING] Fila inválida en states (3 col). Omitida: {row}")
            SKIPPED_ROWS['states'].append(row)
        else:
            state_values_ok.append(row)

    city_values_ok = []
    for row in city_values:
        if len(row) < 3:
            print(f"[WARNING] Fila inválida en cities (3 col). Omitida: {row}")
            SKIPPED_ROWS['cities'].append(row)
        else:
            city_values_ok.append(row)

    country_values = country_values_ok
    state_values = state_values_ok
    city_values = city_values_ok

    # Cantidades finales
    num_countries = len(country_values)
    num_states = len(state_values)
    num_cities = len(city_values)

    print("\nRegistros parseados finales (válidos):")
    print(f" - Countries: {num_countries} (esperados {EXPECTED_COUNTRIES})")
    print(f" - States   : {num_states}   (esperados {EXPECTED_STATES})")
    print(f" - Cities   : {num_cities}   (esperados {EXPECTED_CITIES})")

    # Ver diferencias
    diff_countries = num_countries - EXPECTED_COUNTRIES
    diff_states = num_states - EXPECTED_STATES
    diff_cities = num_cities - EXPECTED_CITIES

    # Reportar
    check_diff_and_report('countries', diff_countries)
    check_diff_and_report('states', diff_states)
    check_diff_and_report('cities', diff_cities)

    # 1) Generar y guardar schema.graphql
    schema_text = generate_schema_graphql()
    with open("schema.graphql", "w", encoding="utf-8") as f:
        f.write(schema_text)

    # 2) Generar y guardar appsync_mutations.graphql
    mutations_text = generate_appsync_mutations(country_values, state_values, city_values)
    with open("appsync_mutations.graphql", "w", encoding="utf-8") as f:
        f.write(mutations_text)

    # 3) Guardar en archivos CSV los registros procesados
    save_data_to_csv("processed_countries.csv", country_columns, country_values)
    save_data_to_csv("processed_states.csv", state_columns, state_values)
    save_data_to_csv("processed_cities.csv", city_columns, city_values)

    print("\nArchivos generados:")
    print(" - schema.graphql")
    print(" - appsync_mutations.graphql")
    print(" - processed_countries.csv")
    print(" - processed_states.csv")
    print(" - processed_cities.csv")
    print("¡Proceso terminado con éxito!")


def check_diff_and_report(table_name, diff):
    """
    Dado un 'diff' = (num_parseados - num_esperados) y el nombre de la tabla,
    imprime un mensaje sobre la diferencia y muestra (si corresponde)
    las filas omitidas.
    """
    if diff == 0:
        print(f"\nNo hay diferencias en {table_name}: se obtuvo la cantidad esperada.")
        return

    if diff < 0:
        print(f"\n[WARNING] Faltan {-diff} registros en {table_name} para llegar a lo esperado.")
        omitted = SKIPPED_ROWS[table_name]
        if omitted:
            print(f"Se omitieron {len(omitted)} filas en {table_name}. Estas son:")
            for row in omitted:
                print("   ", row)
        else:
            print("No se omitieron filas, tal vez tu .sql es incompleto.")
    else:
        # diff > 0
        print(f"\n[WARNING] Se parsearon {diff} registros DE MÁS en {table_name}.")


def generate_schema_graphql():
    """
    Retorna un string con el contenido de 'schema.graphql' para
    countries, states, cities, más tus otros tipos.
    """
    return """# Este schema.graphql define los tipos para 'countries', 'states' y 'cities'.

input AMPLIFY { globalAuthRule: AuthRule = { allow: public } } # FOR TESTING ONLY!

type countries @model {
  id: ID!
  name: String!
  states: [states] @hasMany(indexName: "byCountry", fields: ["id"])
}

type states @model {
  id: ID!
  name: String!
  countryID: ID! @index(name: "byCountry")
  country: countries @belongsTo(fields: ["countryID"])
  cities: [cities] @hasMany(indexName: "byState", fields: ["id"])
}

type cities @model {
  id: ID!
  name: String!
  stateID: ID! @index(name: "byState")
  state: states @belongsTo(fields: ["stateID"])
}
"""


def generate_appsync_mutations(country_values, state_values, city_values):
    """
    Retorna un string con:
      1) Definiciones de mutaciones 'createManyCountries', 'createManyStates', 'createManyCities'
      2) Ejemplos de llamadas a dichas mutaciones con la data parseada.
    """
    definitions = """
# Mutaciones personalizadas para 'bulk insert'. 
# Requieren resolvers/lambdas custom en AppSync que hagan la inserción masiva.

type Mutation {
  createManyCountries(input: [CreateCountryInput!]!): [countries] @function(name: "BatchCreateCountries")
  createManyStates(input: [CreateStateInput!]!): [states] @function(name: "BatchCreateStates")
  createManyCities(input: [CreateCityInput!]!): [cities] @function(name: "BatchCreateCities")
}

input CreateCountryInput {
  id: ID!
  name: String!
}

input CreateStateInput {
  id: ID!
  name: String!
  countryID: ID!
}

input CreateCityInput {
  id: ID!
  name: String!
  stateID: ID!
}
"""

    c_bulk = generate_bulk_countries(country_values)
    s_bulk = generate_bulk_states(state_values)
    ci_bulk = generate_bulk_cities(city_values)

    calls = f"""
# === EJEMPLO DE LLAMADAS ===
mutation CreateAllCountries {{
  createManyCountries(input: [{c_bulk}]) {{
    id
    name
  }}
}}

mutation CreateAllStates {{
  createManyStates(input: [{s_bulk}]) {{
    id
    name
    countryID
  }}
}}

mutation CreateAllCities {{
  createManyCities(input: [{ci_bulk}]) {{
    id
    name
    stateID
  }}
}}
"""
    return definitions + "\n\n" + calls


def generate_bulk_countries(country_values):
    """
    Se espera [id, name].
    """
    items = []
    for row in country_values:
        cid, cname = row[0], row[1]
        item_str = f'{{ id: "{cid.strip()}", name: "{escape_gql_string(cname.strip())}" }}'
        items.append(item_str)
    return ", ".join(items)


def generate_bulk_states(state_values):
    """
    Se espera [id, name, country_id].
    """
    items = []
    for row in state_values:
        sid, sname, country_id = row[0], row[1], row[2]
        item_str = f'{{ id: "{sid.strip()}", name: "{escape_gql_string(sname.strip())}", countryID: "{country_id.strip()}" }}'
        items.append(item_str)
    return ", ".join(items)


def generate_bulk_cities(city_values):
    """
    Se espera [id, name, state_id].
    """
    items = []
    for row in city_values:
        cid, cname, stid = row[0], row[1], row[2]
        item_str = f'{{ id: "{cid.strip()}", name: "{escape_gql_string(cname.strip())}", stateID: "{stid.strip()}" }}'
        items.append(item_str)
    return ", ".join(items)


def escape_gql_string(value):
    """
    Escapa comillas dobles y backslashes para no romper la sintaxis GraphQL.
    """
    return value.replace('\\', '\\\\').replace('"', '\\"')


def save_data_to_csv(filename, columns, rows):
    """
    Guarda los 'rows' (lista de listas) en un CSV con encabezado 'columns'.
    """
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if columns:
            writer.writerow(columns)
        writer.writerows(rows)
    print(f"Se generó {filename} con {len(rows)} registros procesados.")


if __name__ == "__main__":
    main()

