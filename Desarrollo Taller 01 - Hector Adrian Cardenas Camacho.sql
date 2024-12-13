-- Databricks notebook source

 --##Creacion de esquema o base de datos

CREATE DATABASE datapath_taller01

--Nos aseguramos de que usamos el esquema creado
use datapath_taller01


-- ##Creacion de primera tabla externa principal con enablechanged activado


create table detalle_pedido (idPedido INT, dni INT, idproducto STRING, monto double) location '/dbfs/user/hive/warehouse/datapath_taller01.db'
comment 'Table save changes'
tblproperties (delta.enablechangeDataFeed = true);


describe extended detalle_pedido

-- Insertar registros de ejemplo

INSERT INTO detalle_pedido Values (1000, 7676, "PRO01", 1000);
INSERT INTO detalle_pedido Values (1000, 7676, "PRO02", 500);
INSERT INTO detalle_pedido Values (1000, 7676, "PRO03", 500);
INSERT INTO detalle_pedido Values (1001, 7678, "PRO05", 300);
INSERT INTO detalle_pedido Values (1001, 7678, "PRO07", 600);
INSERT INTO detalle_pedido Values (1001, 7678, "PRO08", 700);


select * from detalle_pedido

-- ----------

select * from table_changes('datapath_taller01.detalle_pedido',0 )

--  ----------

-- ##Creacion tabla CTAS

create table pedidos_ctas location '/dbfs/FileStore/tables/external/pedidos_ctas'
SELECT idPedido, dni, '09-12-2024' as fecha ,sum(monto) as total FROM detalle_pedido group by idPedido, dni, fecha 

-- ----------

select * from pedidos_ctas

------------

-- MAGIC %md
-- MAGIC ##Creacion de vista que se usara para actualizar tabla detalle_pedido

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW temp_update_pedidos
    (idPedido COMMENT 'Id unico', dni, idproducto, monto)
    COMMENT 'Vista de actualizacion de pedidos'
  AS VALUES
  (1000, 7676, "PRO01", 1600),
  (1001, 7678, "PRO05", 600),
  (1001, 7678, "PRO09", 100)

-- COMMAND ----------

select * from temp_update_pedidos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Actualizamos la tabla con un MERGE

-- COMMAND ----------

MERGE INTO detalle_pedido dp
USING temp_update_pedidos tdp
ON dp.idPedido = tdp.idPedido and dp.idproducto = tdp.idproducto
WHEN MATCHED
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC volvemos a ejecutar el select para comprobar de que los datos se modificaron

-- COMMAND ----------

select * from detalle_pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Una vez que se modificaron los datos ahora tenemos que actualizar el total de la tabla pedidos_ctas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Con table CTAS

-- COMMAND ----------

create or replace table pedidos_ctas location '/dbfs/FileStore/tables/external/pedidos_ctas'
SELECT idPedido, dni, '09-12-2024' as fecha ,sum(monto) as total FROM detalle_pedido group by idPedido, dni, fecha 

-- COMMAND ----------

select * from pedidos_ctas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Desarrollo de Preguntas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##1.Crear tabla con version anterior de pedidos 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anter de crear , verificamos cuantas versiones tenemos 

-- COMMAND ----------

describe history pedidos_ctas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC visualizamos la version 0 que es la que utilizaremos para crear la tabla

-- COMMAND ----------

select * from pedidos_ctas version as of 0

-- COMMAND ----------

create or replace table pedidos_ctas_ver_ant location '/dbfs/FileStore/tables/external/pedidos_ctas_ver_ant'
select * from pedidos_ctas version as of 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC visualizamos la nueva tabla creada con la version 0

-- COMMAND ----------

select * from pedidos_ctas_ver_ant 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##2.Crear tabla donde se guarde todos los movimientos que han ocurrido sobre la tabla detalle pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para visualizar todos los cambios ocurridos utilizaremos table_changes 

-- COMMAND ----------

select * from table_changes('datapath_taller01.detalle_pedido',0 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Se crea la tabla que guardara los movimiento de modificaciones

-- COMMAND ----------

create table auditoria_pedidos location '/dbfs/FileStore/tables/external/auditoria_pedidos' 
select * from table_changes('datapath_taller01.detalle_pedido',0 )

-- COMMAND ----------

select * from auditoria_pedidos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##3. Realizar limpieza de los logs para las tablas de pedido_ctas y detalle pedido usando Vaccum retain 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Logs tabla detalle_pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC visualizamos con display y dbutils los logs de la tabla detalle_pedido

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/dbfs/user/hive/warehouse/datapath_taller01.db'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Compactamos los parquet

-- COMMAND ----------

optimize detalle_pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verificamos que se haya añadido un nuevo registro que es el que contiene los registros anteriores

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/dbfs/user/hive/warehouse/datapath_taller01.db'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Una vez que verificamos que se haya creada el nuevo parquet , procedemos a limpiar los logs con vacuum

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Setemos la siguiente propiedad para que se puedan eliminar

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum datapath_taller01.detalle_pedido retain 0 hours

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Una vez que se haya ejecutado con exito , verificamos que se hayan eliminado y que haya quedado solo el que contiene a todos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/dbfs/user/hive/warehouse/datapath_taller01.db'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Logs tabla pedido_ctas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC visualizamos con display y dbutils los logs de la tabla pedidos_ctas y cuenta con 2 parquet 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/dbfs/FileStore/tables/external/pedidos_ctas'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Compactamos con optimize

-- COMMAND ----------

optimize pedidos_ctas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verificamos que se haya añadido un nuevo registro que es el que contiene los registros anteriores, pero en este caso aun no se añade porque faltan o seria necesario más parquets para que pueda crearse uno que contenga al resto

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/dbfs/FileStore/tables/external/pedidos_ctas'))
