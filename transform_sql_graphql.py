#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import re
import csv

# Cantidades esperadas para cada tabla
EXPECTED_COUNTRIES = 246
EXPECTED_STATES = 4121
EXPECTED_CITIES = 48356

# Para almacenar filas descartadas que no cumplan columnas mínimas
SKIPPED_ROWS = {
    'countries': [],
    'states': [],
    'cities': []
}

def parse_sql_file(sql_file):
    """
    Lee el archivo .sql y retorna (table_name, columns, values):
      - table_name (str): detectado en CREATE TABLE, p.ej. "countries"
      - columns (list[str]): columnas parseadas del primer INSERT
      - values (list[list[str]]): filas (id, name, [country_id]...) de todos los INSERT

    Usa extract_tuples_respecting_quotes(...) para separar tuplas que contengan paréntesis
    dentro de comillas, y split_by_comma_robust(...) para no romper con comas dentro de comillas.
    """

    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1) Encontrar CREATE TABLE <table_name>
    create_table_match = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(',
        content,
        re.IGNORECASE | re.DOTALL
    )
    if not create_table_match:
        raise ValueError(f"No se encontró CREATE TABLE en {sql_file}")

    table_name = create_table_match.group(1).lower()  # p.ej. "countries"

    # 2) Buscar los INSERT INTO <table_name> (col1,col2,...) VALUES (...);
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
        # Extraer tuplas
        tuple_strings = extract_tuples_respecting_quotes(values_block)

        for tup_str in tuple_strings:
            row_values = split_by_comma_robust(tup_str)
            # Limpieza de comillas sobrantes
            clean_row = [v.strip().strip("'").strip('"') for v in row_values]
            all_values.append(clean_row)

    return table_name, all_columns, all_values


def extract_tuples_respecting_quotes(values_block):
    """
    Dado un string tras 'VALUES', p.ej:
      "(1, 'X (hola)'), (2, 'y'), (3, 'z (t)')"
    extrae cada tupla sin confundir paréntesis dentro de comillas.

    Retorna lista de strings, cada uno con lo que hay dentro de (...).
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
            elif char == '(' and not in_single_quote and not in_double_quote:
                paren_depth += 1
                current.append(char)
                backslash_count = 0
            elif char == ')' and not in_single_quote and not in_double_quote:
                paren_depth -= 1
                current.append(char)
                backslash_count = 0

                if paren_depth == 0:
                    # Cerró una tupla de nivel superior
                    tuple_str = ''.join(current)
                    if tuple_str.startswith('(') and tuple_str.endswith(')'):
                        tuple_str = tuple_str[1:-1]
                    results.append(tuple_str.strip())
                    current = []

                    i += 1
                    # Saltar comas/espacios que separan tuplas
                    while i < length:
                        nxt = values_block[i]
                        if nxt == '(':
                            break
                        elif nxt in [',',' ','\n','\t','\r']:
                            i += 1
                            continue
                        else:
                            # cualquier otro char suelto
                            i += 1
                    continue
            else:
                current.append(char)
                backslash_count = 0

        i += 1

    return results


def split_by_comma_robust(value_string):
    """
    Separa "1, 'X, Y', 2" en ["1", "'X, Y'", "2"],
    respetando comillas y backslash.
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
                # separa solo si estamos fuera de comillas
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


def cleanup_field(value: str) -> str:
    """
    Reglas de limpieza personalizadas.
    Ajusta si necesitas un tratamiento distinto.
    """
    # Ejemplo:
    #  1) Si inicia con \' => quitarlo
    if value.startswith("\\'"):
        value = value[2:]
    #  2) Si termina con \ => quitarlo
    if value.endswith("\\"):
        value = value[:-1]
    #  3) Reemplazar cualquier \' en el medio por un espacio
    value = value.replace("\\'", " ")
    #  4) Eliminar dobles backslash
    value = value.replace("\\\\", "")

    return value


def save_data_to_csv(filename, columns, rows):
    """
    Guarda las filas (rows) en un CSV con encabezado (columns).
    Aplica cleanup_field a cada valor y los convierte a mayúscula.
    """
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if columns:
            # Pasar las columnas a mayúsculas
            columns_upper = [col.upper() for col in columns]
            writer.writerow(columns_upper)
        for row in rows:
            # Pasar cada campo por cleanup_field y luego a mayúsculas
            cleaned_row = [cleanup_field(col).upper() for col in row]
            writer.writerow(cleaned_row)

    print(f"Se generó {filename} con {len(rows)} registros procesados.")



def main():
    if len(sys.argv) < 4:
        print("Uso: python transformar.py <cities.sql> <countries.sql> <states.sql>")
        sys.exit(1)

    # Recibimos 3 archivos, en cualquier orden
    files = sys.argv[1:4]

    # Guardamos la data parseada
    # (countries, states, cities) se indexan con sus nombres en minúsculas
    table_data = {
        'countries': None,
        'states': None,
        'cities': None
    }

    for fpath in files:
        tname, cols, vals = parse_sql_file(fpath)  # tname = "countries", "states" or "cities"
        if tname not in table_data:
            print(f"[WARNING] La tabla '{tname}' no es countries/states/cities. Se ignora.")
        else:
            table_data[tname] = (cols, vals)

    # Ver si falta algo
    missing = [t for t in ('countries','states','cities') if table_data[t] is None]
    if missing:
        raise ValueError(f"No se pudo parsear las tablas: {missing}")

    # Extraer data
    country_columns, country_values = table_data['countries']
    state_columns, state_values = table_data['states']
    city_columns, city_values = table_data['cities']

    # Filtrar filas que no cumplan col. mínimas
    # countries -> 2 col (id,name)
    # states -> 3 col (id,name,country_id)
    # cities -> 3 col (id,name,state_id)
    cv_ok = []
    for row in country_values:
        if len(row) < 2:
            SKIPPED_ROWS['countries'].append(row)
        else:
            cv_ok.append(row)
    country_values = cv_ok

    sv_ok = []
    for row in state_values:
        if len(row) < 3:
            SKIPPED_ROWS['states'].append(row)
        else:
            sv_ok.append(row)
    state_values = sv_ok

    civ_ok = []
    for row in city_values:
        if len(row) < 3:
            SKIPPED_ROWS['cities'].append(row)
        else:
            civ_ok.append(row)
    city_values = civ_ok

    # Contar
    num_countries = len(country_values)
    num_states = len(state_values)
    num_cities = len(city_values)

    print("\nRegistros parseados finales (válidos):")
    print(f" - Countries: {num_countries} (esperados {EXPECTED_COUNTRIES})")
    print(f" - States   : {num_states}   (esperados {EXPECTED_STATES})")
    print(f" - Cities   : {num_cities}   (esperados {EXPECTED_CITIES})")

    # Generar schema.graphql con NOMBRES cambiados (ctlnbpais, ctlnbestado, ctlnbciudad)
    schema_text = generate_schema_graphql()
    with open("schema.graphql", "w", encoding="utf-8") as f:
        f.write(schema_text)

    # Guardar CSV
    save_data_to_csv("processed_countries.csv", country_columns, country_values)
    save_data_to_csv("processed_states.csv", state_columns, state_values)
    save_data_to_csv("processed_cities.csv", city_columns, city_values)

    print("\nArchivos generados:")
    print(" - schema.graphql")
    print(" - processed_countries.csv")
    print(" - processed_states.csv")
    print(" - processed_cities.csv")
    print("¡Proceso terminado con éxito!")


def generate_schema_graphql():
    """
    Retorna un schema que en vez de 'countries', 'states', 'cities' use:
      - ctlnbpais
      - ctlnbestado
      - ctlnbciudad

    con la lógica @model y sus relaciones. Se cambian nombres de campos, indexName, etc.
    """
    return """# Este schema.graphql define las tablas con los nuevos nombres en DynamoDB/AppSync

input AMPLIFY { globalAuthRule: AuthRule = { allow: public } } # FOR TESTING ONLY!

type ctlnbpais @model {
  id: ID!
  nombre: String!
  estados: [ctlnbestado] @hasMany(indexName: "byPais", fields: ["id"])
}

type ctlnbestado @model {
  id: ID!
  nombre: String!
  paisID: ID! @index(name: "byPais")
  pais: ctlnbpais @belongsTo(fields: ["paisID"])
  ciudades: [ctlnbciudad] @hasMany(indexName: "byEstado", fields: ["id"])
}

type ctlnbciudad @model {
  id: ID!
  nombre: String!
  estadoID: ID! @index(name: "byEstado")
  estado: ctlnbestado @belongsTo(fields: ["estadoID"])
}

"""


if __name__ == "__main__":
    main()
