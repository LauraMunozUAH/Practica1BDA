-- CUESTIÓN 0
SELECT pg_reload_conf();
-- Mostrar todos los parámetros para comprobar si esta bien cambiado
SELECT
    current_setting('log_statement')       AS log_statement,
    current_setting('logging_collector')   AS logging_collector,
    current_setting('log_duration')        AS log_duration,
    current_setting('log_line_prefix')     AS log_line_prefix;

--CUESTIÓN 1
--DROP SCHEMA cuestiones CASCADE ;
CREATE SCHEMA IF NOT EXISTS cuestiones;
CREATE TABLE IF NOT EXISTS cuestiones.estudiantes (
    estudiante_id SERIAL PRIMARY KEY,
    nombre VARCHAR(40),
    codigo_carrera INT,
    edad INT,
    indice INT
);

COPY cuestiones.estudiantes(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER false);

--CUESTIÓN 2
SELECT
    COUNT(*) AS num_filas,
    pg_relation_size('cuestiones.estudiantes') AS tamano_bytes,
    8192 AS tamano_bloque,
    COUNT(*) / CEIL(pg_relation_size('cuestiones.estudiantes')::numeric / 8192) AS filas_por_bloque_real
FROM cuestiones.estudiantes;

--CUESTIÓN 3
-- Encontrar los estudiantes con índice 500
SELECT COUNT(*) AS num_tuplas
FROM cuestiones.estudiantes
WHERE indice = 500;

SELECT pg_relation_size('cuestiones.estudiantes') / 8192 AS bloques; -- comprobar número de bloques total


EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM cuestiones.estudiantes
WHERE indice = 500;

SELECT *
FROM pg_stat_user_tables
WHERE relname = 'estudiantes';

SELECT *
FROM pg_stats
WHERE tablename = 'estudiantes';


SELECT
    attname,
    n_distinct,
    most_common_vals,
    most_common_freqs
FROM pg_stats
WHERE tablename = 'estudiantes'
  AND attname = 'indice';

--CUESTIÓN 4
-- Repetir la consulta de la cuestión 3 y comparar
SHOW shared_buffers; -- comprobar tamaño

--CUESTIÓN 5
-- Crear tabla ordenada por el campo índice
CREATE TABLE IF NOT EXISTS cuestiones.estudiantes2 (
    estudiante_id SERIAL,
    nombre VARCHAR(40),
    codigo_carrera INT,
    edad INT,
    indice INT
);

COPY cuestiones.estudiantes2(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER false);

CREATE INDEX IF NOT EXISTS idx_estudiantes2_indice
ON cuestiones.estudiantes2(indice);

SELECT indexname
FROM pg_indexes
WHERE schemaname = 'cuestiones'; -- comprobamos que se ha creado el índice

CLUSTER cuestiones.estudiantes2 USING idx_estudiantes2_indice;

SELECT pg_relation_size('cuestiones.estudiantes2') / 8192 AS bloques; -- comprobar número de bloques total

--CUESTIÓN 6
-- Encontrar los estudiantes con índice 500
SELECT COUNT(*) AS num_tuplas
FROM cuestiones.estudiantes2
WHERE indice = 500;


EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM cuestiones.estudiantes2
WHERE indice = 500;

SELECT *
FROM pg_stat_user_tables
WHERE relname = 'estudiantes2';

SELECT *
FROM pg_stats
WHERE tablename = 'estudiantes2';


SELECT
    attname,
    n_distinct,
    most_common_vals,
    most_common_freqs
FROM pg_stats
WHERE tablename = 'estudiantes2'
  AND attname = 'indice';

-- CUESTIÓN 7
DELETE FROM cuestiones.estudiantes
WHERE estudiante_id IN (
    SELECT estudiante_id
    FROM cuestiones.estudiantes
    ORDER BY random()
    LIMIT 5000000
);

SELECT pg_relation_size('cuestiones.estudiantes') / 8192 AS bloques;

SELECT n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'estudiantes';


-- CUESTIÓN 8
INSERT INTO cuestiones.estudiantes (nombre, codigo_carrera, edad, indice)
VALUES ('Roberto Pérez', 10, 22, 450);

SELECT ctid, *
FROM cuestiones.estudiantes
WHERE nombre = 'Roberto Pérez'; -- buscar la dirección física de la fila
