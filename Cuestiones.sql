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
    indice_academico INT
);

COPY cuestiones.estudiantes(nombre, codigo_carrera, edad, indice_academico)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);
