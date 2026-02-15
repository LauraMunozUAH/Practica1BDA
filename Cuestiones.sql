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

DROP INDEX cuestiones.idx_estudiantes2_indice;
CREATE INDEX IF NOT EXISTS idx_estudiantes2_indice
ON cuestiones.estudiantes2(indice);

CLUSTER cuestiones.estudiantes2 USING cuestiones.estudiantes2.idx_estudiantes2_indice;

-- comprobar número de bloques total