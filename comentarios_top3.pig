-- Cargar archivo CSV desde HDFS
pedidos = LOAD '/user/cloudera/pedidos/comentarios_pedidos.csv'
    USING PigStorage(',')
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- Extraer solo los comentarios
comentarios = FOREACH pedidos GENERATE comentario;

-- Separar cada comentario en palabras individuales
palabras = FOREACH comentarios GENERATE FLATTEN(TOKENIZE(comentario)) AS palabra;

-- Convertir a minúsculas para evitar duplicados por mayúsculas
palabras_min = FOREACH palabras GENERATE LOWER(palabra) AS palabra;

-- Agrupar por palabra
agrupado = GROUP palabras_min BY palabra;

-- Contar cuántas veces aparece cada palabra
conteo = FOREACH agrupado GENERATE group AS palabra, COUNT(palabras_min) AS cantidad;

-- Ordenar de mayor a menor
ordenadas = ORDER conteo BY cantidad DESC;

-- Tomar las 3 más repetidas
top3 = LIMIT ordenadas 3;

-- Mostrar resultado
DUMP top3;
