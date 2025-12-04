/* Recordar que los elementos de configuracion minimos para activar la replicacion logica son 
  
  - wal_level = logical => Al escoger logical postgres agrega mas metadatos al WAL, de forma de que pueda ser reconstruido el Query
  - max_wal_senders = 10 => Define el numero de maximos de Background procees que puedan estar enviando WAL via rep logica 
  - max_replication_slots = 10 => Define el numero maximo de slot que pueden existir debe coincidir o ser menor al numero maximo de wal_senders

*/

/* Verificar configuracion de la replica logica */
SELECT name, setting, unit,short_desc FROM pg_settings 
WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots') ORDER BY name DESC;


SELECT setting FROM pg_settings WHERE name = 'wal_level';


/*Crear Slot de Replicacion  como comentario es importante acotar que no solo se pasa el nombre del slot sino tambien el plugin de replicacion que se desea usar */
SELECT slot_name FROM pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

/* Eliminar Slot De Replicacion El Slot de replicacion es lo  que permite a Postgres trackear donde un consumidor  de la replica
 se encuentra, usando el LSN  (Log Sequence Number) al menos que se configure un Max Wal Size, postgres mantendra en disco los WAL 
desde el ultimo LSN reportado por el consumidor de la replica logica, es por ello que es necesario que el consumidor (replica) emita un ack 
de los WAL para que estos puedan ser borrados  Standby Status Update messages, que incluyen el LSN (Log Sequence Number) 
hasta el cual el standby (o el cliente de replicación) ha recibido y procesado los datos.

*/
SELECT pg_drop_replication_slot('cdc_slot');

/* Comprobar estado  de la replica */

SELECT * FROM pg_stat_replication;




/* Crea la publicacion le dice a postgres que para ciertos parametro o esquemas */

CREATE PUBLICATION <name> FOR ...


CREATE PUBLICATION my_publication FOR ALL TABLES;


CREATE PUBLICATION my_publication FOR ALL TABLES;


CREATE PUBLICATION test_pb FOR TABLE sygateway.usurios, sygateway.transaction WITH (publish = 'insert, update');

/*Eliminar Publicacion */

DROP PUBLICATION IF EXISTS nombre_de_publicacion;


/* Listar Publicaciones */

SELECT 
    pubname AS publication_name,
    schemaname AS schema_name,
    tablename AS table_name
FROM 
    pg_publication_tables;


/*Alterar identidad de la replica de una tabla esto permite que al momento de replicar logicamente 
en UPDATE Y DELETES se pase la informacion de la tabla antes y despues del query */ 

ALTER TABLE <table> REPLICA IDENTITY FULL -- AGREGAR;


ALTER TABLE <esquema.tabla> REPLICA IDENTITY DEFAULT --REMOVER;



/* Verificar si el slot de replicacion logica existe, recordar que el slot de replicacion es lo que permite que un 
 proceso de replicacion se conecte a la bd y recibe los WAL, este slot tambien es quien mantiene el status del ultimo WAL 
 consumido por el proceso de replicacion
 */

SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=<nombre_slot>)


/* Crear SLOT de Replicacion */

SELECT slot_name FROM pg_create_logical_replication_slot(<nombre>, <decode_plugin>)


/*CREAR SUBSCRIPCION */


CREATE SUBSCRIPTION mi_suscripcion
CONNECTION 'host=publicador_host dbname=publicador_db user=replication_user password=su_password'
PUBLICATION pub_tablas_clientes, pub_tablas_pedidos;