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
SELECT *
FROM cuestiones.estudiantes
WHERE indice = 500;
