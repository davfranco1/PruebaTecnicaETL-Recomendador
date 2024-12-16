# --------- Creación ---------

query_creacion_contenido = '''
    CREATE TABLE IF NOT EXISTS contenido (
        id_contenido VARCHAR PRIMARY KEY,
        nombre VARCHAR,
        id_tipo INT,
        id_director INT,
        FOREIGN KEY (id_director) REFERENCES directores(id_director)
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );
'''


query_creacion_caracteristicas = '''
    CREATE TABLE IF NOT EXISTS caracteristicas (
        id_contenido VARCHAR,
        guion VARCHAR,
        anio INT,
        mes VARCHAR,
        duracion VARCHAR,
        calificacion FLOAT,
        argumento VARCHAR,
        FOREIGN KEY (id_contenido) REFERENCES contenido(id_contenido)
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );
'''


query_creacion_tipo_contenido = '''
    CREATE TABLE IF NOT EXISTS tipo_contenido (
        id_tipo INT PRIMARY KEY,
        nombre VARCHAR
    );
'''


query_creacion_directores = '''
    CREATE TABLE IF NOT EXISTS directores (
        id_director INT PRIMARY KEY,
        nombre VARCHAR
    );
'''

# --------- Inserción ---------

query_inser_contenido = '''
    INSERT INTO contenido (id_contenido, nombre, id_tipo, id_director) 
    values (%s, %s, %s, %s);
'''

query_inser_caracteristicas = '''
    INSERT INTO caracteristicas (id_contenido, guion, anio, mes, duracion, calificacion, argumento) 
    values (%s, %s, %s, %s, %s, %s, %s);
'''

query_inser_tipo_contenido = '''
    INSERT INTO tipo_contenido (id_tipo, nombre)
    values (%s, %s);
'''

query_inser_directores = '''
    INSERT INTO directores (id_director, nombre)
    values (%s, %s);
'''

