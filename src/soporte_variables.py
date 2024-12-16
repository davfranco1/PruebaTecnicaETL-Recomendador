# --------- Creación ---------

query_creacion_distritos = '''
    create table if not exists distritos(
        id_distrito INT primary key,
        nombre VARCHAR(100),
        geometry GEOMETRY(MultiPolygon, 4326)
        );'''

query_creacion_airbnb= '''
    create table if not exists airbnb(
        id_airbnb SERIAL primary key,
        id_distrito INT
            references distritos(id_distrito)
            on update cascade
            on delete cascade,
        precio INT,
        descripcion VARCHAR,
        geometry GEOMETRY(Point, 4326)
        );'''

query_creacion_idealista='''
    create table if not exists idealista(
        id_idealista SERIAL primary key,
        id_distrito INT
            references distritos(id_distrito)
            on update cascade
            on delete cascade,
        precio INT,
        tipo VARCHAR,
        planta VARCHAR,
        tamanio INT,
        habitaciones INT,
        banios INT,
        direccion VARCHAR,
        descripcion VARCHAR,
        geometry GEOMETRY(Point, 4326)    
    );'''

query_creacion_redpiso='''
    create table if not exists redpiso(
        id_redpiso SERIAL primary key,
        id_distrito INT
            references distritos(id_distrito)
            on update cascade
            on delete cascade,
        precio INT,
        descripcion VARCHAR
    );'''

query_creacion_ingreso_hogar='''
    create table if not exists ingresos_hogar(
        id_ingreso_hogar SERIAL primary key,
        id_distrito INT
            references distritos(id_distrito)
            on update cascade
            on delete cascade,
        periodo INT,
        total FLOAT
    );'''

query_creacion_poblacion='''
    create table if not exists poblacion(
        id_poblacion SERIAL primary key,
        id_distrito INT
            references distritos(id_distrito)
            on update cascade
            on delete cascade,
        periodo INT,
        espanioles INT,
        extranjeros INT,
        total INT
    );'''

# --------- Inserción ---------

query_inser_distritos = '''
    INSERT INTO distritos (id_distrito, nombre, geometry) 
    values (%s, %s, ST_GeomFromText(%s, 4326));
'''

query_inser_airbnb = '''
    INSERT INTO airbnb (id_distrito, precio, descripcion, geometry) 
    values (%s, %s, %s, ST_GeomFromText(%s, 4326));
'''

query_inser_idealista = '''
    INSERT INTO idealista (id_distrito, precio, tipo, planta, tamanio, habitaciones, banios, direccion, descripcion, geometry)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));
'''

query_inser_redpiso = '''
    INSERT INTO redpiso (id_distrito, precio, descripcion)
    values (%s, %s, %s);
'''

query_inser_ingreso_hogar = '''
    INSERT INTO ingresos_hogar (id_distrito, periodo, total)
    values (%s, %s, %s);
'''

query_inser_poblacion = '''
    INSERT INTO poblacion (id_distrito, periodo, espanioles, extranjeros, total)
    values (%s, %s, %s, %s, %s);
'''
