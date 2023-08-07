import psycopg2

def delete_matches():
    conn = psycopg2.connect(database="postgres", user="postgres", password="admin", host="host.docker.internal", port="5432")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tecnica_ml")
    ids_tabla = cursor.fetchall()
    conn.close()

    with open("plugins/operators/resultados.tsv", "r") as file:
        lines = file.readlines()

    lines_actualizadas = []
    for line in lines:
        id_linea = line.strip().split("\t")[0]  # Suponiendo que el id está en la primera columna del archivo
        if id_linea not in [str(row[0]) for row in ids_tabla]:
            lines_actualizadas.append(line)

    with open("plugins/operators/resultados.tsv", "w") as file:
        file.writelines(lines_actualizadas)
