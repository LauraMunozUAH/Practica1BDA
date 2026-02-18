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
----Consulta de estudiantes con índice 500
SELECT *
FROM cuestiones.estudiantes
WHERE indice = 500;

-- Número de tuplas encontradas
SELECT COUNT(*) AS num_tuplas
FROM cuestiones.estudiantes
WHERE indice = 500;

-- Tamaño total de la tabla en bloques
SELECT pg_relation_size('cuestiones.estudiantes') / 8192 AS bloques; -- comprobar número de bloques total
--Bloques realmente leídos durante la consulta
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM cuestiones.estudiantes
WHERE indice = 500;

--Estadísticas de la tabla
SELECT *
FROM pg_stat_user_tables
WHERE relname = 'estudiantes';
--Estadísticas de columnas
SELECT *
FROM pg_stats
WHERE tablename = 'estudiantes';
--Estadísticas específicas del campo indice
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
CREATE TABLE estudiantes2 AS
SELECT *
FROM cuestiones.estudiantes
ORDER BY indice;
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


--CUESTIÓN 9
VACUUM FULL cuestiones.estudiantes;
ANALYZE cuestiones.estudiantes;
--Comprobación antes y después
SELECT pg_relation_size('cuestiones.estudiantes') / 8192 AS bloques;
-- Comprobamos que no hay dead_tup
SELECT n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'estudiantes';


--CUESTIÓN 10
CREATE TABLE cuestiones.estudiantes3 (
    estudiante_id SERIAL,
    nombre VARCHAR(40),
    codigo_carrera INT,
    edad INT,
    indice INT
) PARTITION BY HASH (codigo_carrera);
DO $$
BEGIN
  FOR i IN 0..19 LOOP
    EXECUTE format(
      'CREATE TABLE cuestiones.estudiantes3_p%s
       PARTITION OF cuestiones.estudiantes3
       FOR VALUES WITH (MODULUS 20, REMAINDER %s);',
      i, i
    );
  END LOOP;
END $$;

COPY cuestiones.estudiantes3(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv);

--Ver cuantos bloques ocupa cada partición
SELECT
    relname,
    pg_relation_size(oid) / 8192 AS bloques
FROM pg_class
WHERE relname LIKE 'estudiantes3_p%';


--CUESTIÓN 11
----Consulta de estudiantes con índice 500
SELECT *
FROM cuestiones.estudiantes3
WHERE indice = 500;

-- Número de tuplas encontradas
SELECT COUNT(*) AS num_tuplas
FROM cuestiones.estudiantes3
WHERE indice = 500;

-- Tamaño total de la tabla en bloques
SELECT pg_relation_size('cuestiones.estudiantes3') / 8192 AS bloques; -- comprobar número de bloques total
--Bloques realmente leídos durante la consulta
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM cuestiones.estudiantes3
WHERE indice = 500;

--Estadísticas de la tabla
SELECT *
FROM pg_stat_user_tables
WHERE relname = 'estudiantes3';
--Estadísticas de columnas
SELECT *
FROM pg_stats
WHERE tablename = 'estudiantes3';
--Estadísticas específicas del campo indice
SELECT
    attname,
    n_distinct,
    most_common_vals,
    most_common_freqs
FROM pg_stats
WHERE tablename = 'estudiantes3'
  AND attname = 'indice';


--CUESTIÓN 12
DROP TABLE IF EXISTS cuestiones.estudiantes CASCADE;
DROP TABLE IF EXISTS cuestiones.estudiantes2 CASCADE;
DROP TABLE IF EXISTS cuestiones.estudiantes3 CASCADE;

CREATE TABLE IF NOT EXISTS cuestiones.estudiantes (
    estudiante_id SERIAL,
    nombre VARCHAR(40),
    codigo_carrera INT,
    edad INT,
    indice INT
);

COPY cuestiones.estudiantes(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER false);

-- Y ahora ordenar físicamente:
CREATE TABLE estudiantes2 AS
SELECT *
FROM estudiantes
ORDER BY indice;


--CUESTIÓN 13
CREATE INDEX IF NOT EXISTS idx_estudiantes2_id
ON cuestiones.estudiantes2(estudiante_id);
--Comprobamos que se ha creado el índice
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'cuestiones';
--Obtenemos su identrificador interno
SELECT oid, relname
FROM pg_class
WHERE relname = 'idx_estudiantes2_id';
--Tamaño del índice
SELECT pg_size_pretty(
    pg_relation_size('idx_estudiantes2_id')
);
--Número de bloques
SELECT pg_relation_size('idx_estudiantes2_id') / 8192 AS bloques;
--Analizamos la estructura interna del arbol
--Número de niveles del árbol
SELECT relname, relpages AS bloques, reltuples AS tuplas
FROM pg_class
WHERE relname = 'idx_estudiantes2_id';
--Tuplas por bloque
SELECT reltuples / relpages AS tuplas_por_bloque
FROM pg_class
WHERE relname = 'idx_estudiantes2_id';


--CUESTIÓN 14
SELECT avg(pg_column_size(t.*)) AS tamano_promedio
FROM cuestiones.estudiantes2 t;

--CUESTIÓN 15
CREATE INDEX idx_estudiantes2_id_hash
ON cuestiones.estudiantes2 USING HASH (estudiante_id);

--Inspecionamos su existencia
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname='cuestiones' AND indexname='idx_estudiantes2_id_hash';

--Inspeccionamos su OID
SELECT oid, relname
FROM pg_class
WHERE relname='idx_estudiantes2_id_hash';

--Tamaño físico del índice y el número de bloques que ocupa
SELECT pg_size_pretty(pg_relation_size('idx_estudiantes2_id_hash')) AS tamaño,
       pg_relation_size('idx_estudiantes2_id_hash')/8192 AS bloques;

--Conocer el número de cajones y la distribución de tuplas por cajón
SELECT relname, relpages AS bloques, reltuples AS tuplas
FROM pg_class
WHERE relname='idx_estudiantes2_id_hash';

--CLUSTER cuestiones.estudiantes2 USING idx_estudiantes2_indice;