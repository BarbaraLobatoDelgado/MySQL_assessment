/*
Proyecto "Creacion de una base de datos"
Asignatura: Bases de datos SQL
Máster en Big Data & Data Science con aplicaciones al comercio, 
empresas y finanzas (modalidad online)
Autora del script: Barbara Lobato Delgado
Script ejecutado en MySQL Workbench (v8.0) 
*/


/* ---------------------------- */
/* PARTE 1: DEFINICION DE DATOS */
/* ---------------------------- */


/* 1. Creacion de base de datos */

-- Si la BBDD existe previamente, se borrar para evitar errores 
DROP DATABASE IF EXISTS bbdd_apps;
-- Se crea la BBDD de nuevo...  
CREATE DATABASE bbdd_apps;
-- ... e indicamos a MySQL que la vamos a utilizar 
USE bbdd_apps;

-- Si existiesen las siguiente tablas, se borran para evitar errores
DROP TABLE IF EXISTS EMPRESA;
DROP TABLE IF EXISTS EMPLEADO;
DROP TABLE IF EXISTS hay;
DROP TABLE IF EXISTS APP;
DROP TABLE IF EXISTS realiza;
DROP TABLE IF EXISTS TIENDA_APPS;
DROP TABLE IF EXISTS contiene;
DROP TABLE IF EXISTS USUARIO;
DROP TABLE IF EXISTS descarga;
 
-- Indicar la codificacion de las cadenas de caracteres
SET NAMES utf8mb4;


/* 2. Creacion de tablas correspondientes a entidades y relaciones */

CREATE TABLE EMPRESA (
codigo_empresa INT AUTO_INCREMENT PRIMARY KEY,
nombre_empresa VARCHAR(40) NOT NULL,
pais_impuestos VARCHAR(15),
anno_creacion YEAR,
email_empresa VARCHAR(50) UNIQUE NOT NULL,
pagina_web VARCHAR(50) UNIQUE
);

-- Entidad EMPLEADO
CREATE TABLE EMPLEADO (
dni_empleado CHAR(9) PRIMARY KEY,
calle VARCHAR(50),
numero INT,
codigo_postal NUMERIC(5 , 0 ),
email_empleado VARCHAR(50) UNIQUE,
telefono_empleado NUMERIC(9 , 0 ) UNIQUE
);

-- Relacion "hay" 
CREATE TABLE hay (
codigo_empresa INT NOT NULL,
dni_empleado CHAR(9) NOT NULL,
fecha_inicio_empleo DATE NOT NULL,
fecha_fin_empleo DATE NOT NULL,
PRIMARY KEY (codigo_empresa , dni_empleado , fecha_inicio_empleo , fecha_fin_empleo),
FOREIGN KEY (codigo_empresa)
REFERENCES EMPRESA (codigo_empresa)
ON DELETE CASCADE ON UPDATE CASCADE,
FOREIGN KEY (dni_empleado)
REFERENCES EMPLEADO (dni_empleado)
ON DELETE CASCADE ON UPDATE CASCADE
);

-- Forzar que la fecha de inicio en un empleo sea anterior a la de fin
ALTER TABLE hay
ADD CONSTRAINT concordancia_de_fechas_empleos 
CHECK(fecha_fin_empleo > fecha_inicio_empleo);

-- Entidad APP
CREATE TABLE APP (
nombre_app varchar(20) NOT NULL,
codigo_app numeric(7,0) PRIMARY KEY,
fecha_inicio_desarrollo Date,
fecha_fin_desarrollo Date,
categoria_app varchar(100),
espacio_memoria_mb numeric(4,2),
precio real,
/* Este atributo proviene de la relacion "crea" */
codigo_empresa_creadora int NOT NULL, 
FOREIGN KEY (codigo_empresa_creadora) REFERENCES EMPRESA (codigo_empresa)
/* Si se borra una empresa, eliminar tambien las apps que crearon */
ON DELETE CASCADE
/* Si se modifica el codigo de la empresa creadora, este se actualiza */
ON UPDATE CASCADE,
/* Este atributo proviene de la relacion "dirige" */
dni_director_app char(9),
FOREIGN KEY (dni_director_app) REFERENCES EMPLEADO (dni_empleado)
/* Si se borra el empleado que dirige la app, se introduce un valor null
Aunque se borre al empleado, no deja de existir la app */
ON DELETE SET NULL
/* Si cambia el empleado que dirige la app, actualizar esta tabla */
ON UPDATE CASCADE
);

-- Forzar que la fecha de inicio del desarrollo de la app sea anterior a la de fin
ALTER TABLE APP
ADD CONSTRAINT concordancia_de_fechas_desarrollo
CHECK(fecha_fin_desarrollo > fecha_inicio_desarrollo);

-- Relacion "realiza"
CREATE TABLE realiza (
num_registro_realiza int auto_increment PRIMARY KEY,
codigo_app numeric(7,0) NOT NULL,
codigo_empresa int NOT NULL,
dni_empleado char(9) NOT NULL, 
FOREIGN KEY (codigo_app) REFERENCES APP (codigo_app)
/* Si se borra una app, borrar registro correspondiente en esta tabla */
ON DELETE CASCADE
/* Si se actualiza el codigo de una app, actualizar en esta tabla */
ON UPDATE CASCADE, 
FOREIGN KEY (codigo_empresa) REFERENCES EMPRESA (codigo_empresa)
/* Si se elimina una empresa, se borran los registros de empleados que han 
creado un app en dicha empresa */
ON DELETE CASCADE
/* Si se actualiza el codigo de la empresa, actualizarlo en esta tabla */
ON UPDATE CASCADE,
FOREIGN KEY (dni_empleado) REFERENCES EMPLEADO (dni_empleado)
/* Si se elimina un empleado, eliminar su registro de esta tabla */
ON DELETE CASCADE
/* Si se actualiza un empleado, actualizarlo en esta tabla */
ON UPDATE CASCADE
);

-- Entidad TIENDA_APPS
CREATE TABLE TIENDA_APPS (
nombre_tienda_app varchar(20) PRIMARY KEY,
empresa_gestiona varchar(20) unique NOT NULL, 
dir_web varchar(100)
);

-- Relacion "contiene"
CREATE TABLE contiene (
codigo_app numeric(7,0) NOT NULL,
nombre_tienda_app varchar(20) NOT NULL,
PRIMARY KEY (codigo_app, nombre_tienda_app),
FOREIGN KEY (codigo_app) REFERENCES APP (codigo_app)
/* Si se borra una app, se eliminan sus registros de esta tabla */
ON DELETE CASCADE
/* Si se actualiza el codigo de una app, se actualizan los registros de esta tabla */
ON UPDATE CASCADE,
FOREIGN KEY (nombre_tienda_app) REFERENCES TIENDA_APPS (nombre_tienda_app)
/* Si se borra la tienda de apps, deben borrarse todos sus registros */
ON DELETE CASCADE
/* De igual manera, si se actualiza el nombre de la tienda de apps, 
debe actualizarse esta tabla */
ON UPDATE CASCADE
); 

-- Entidad USUARIO
CREATE TABLE USUARIO (
numero_cuenta VARCHAR(40) PRIMARY KEY,
nombre_usuario VARCHAR(50) NOT NULL,
dir_usuario VARCHAR(70) NOT NULL,
pais_usuario VARCHAR(20) NOT NULL
);

-- Relacion "descarga"
CREATE TABLE descarga (
codigo_app numeric(7,0) NOT NULL,
numero_cuenta varchar(40) NOT NULL, 
tfn_usuario varchar(20) NOT NULL,
fecha_descarga Date NOT NULL, 
puntuacion_app enum('1', '2', '3', '4', '5'),
comentario_app varchar(500),
/* Al definir esta clave primaria, se impone la restriccion de que un usuario 
no puede descargar dos veces la misma app */
PRIMARY KEY (codigo_app, tfn_usuario), 
FOREIGN KEY (codigo_app) REFERENCES APP (codigo_app)
ON DELETE CASCADE 
ON UPDATE CASCADE, 
FOREIGN KEY (numero_cuenta) REFERENCES USUARIO (numero_cuenta)
ON DELETE CASCADE
ON UPDATE CASCADE
);


/* ------------------------------ */
/* PARTE 2: MANIPULACION DE DATOS */
/* ------------------------------ */

/* 3. Carga de datos en tablas */

/* Para saber en que directorio debemos poner los 
ficheros csv a cargar , ejecuto  el siguiente comando */
SELECT @@GLOBAL.secure_file_priv; -- El directorio es "/var/lib/mysql-files/ 

-- Cargar archivo con los datos de EMPRESA
LOAD DATA INFILE '/var/lib/mysql-files/datos_empresa.csv'
INTO TABLE EMPRESA
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

-- Cargar archivo con los datos de EMPLEADO
LOAD DATA INFILE '/var/lib/mysql-files/datos_empleados.csv'
INTO TABLE EMPLEADO
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

-- Cargar archivo con los datos de "hay"
LOAD DATA INFILE '/var/lib/mysql-files/datos_relacion_hay.csv'
INTO TABLE hay
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
/* Se desea que la fecha de fin de empledo de algunos 
empleados muestren la fecha actual, para dar a entender 
que su contrato no ha terminado */
(codigo_empresa, dni_empleado, fecha_inicio_empleo, @fecha_fin_empleo)
SET
fecha_fin_empleo = IF(@fecha_fin_empleo = '2021-10-31', curdate(), @fecha_fin_empleo)
;

-- Cargar archivo con los datos de APP
LOAD DATA INFILE '/var/lib/mysql-files/datos_apps.csv'
INTO TABLE APP
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Cargar archivo con los datos "realiza"
LOAD DATA INFILE '/var/lib/mysql-files/datos_relacion_realiza.csv'
INTO TABLE realiza
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Cargar archivo con los datos de TIENDA_APPS
LOAD DATA INFILE '/var/lib/mysql-files/datos_tiendas_apps.csv'
INTO TABLE TIENDA_APPS
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

-- Cargar archivo para la relacion "contiene"
LOAD DATA INFILE '/var/lib/mysql-files/datos_relacion_contiene.csv'
INTO TABLE contiene
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Cargar archivo con los datos de USUARIO
LOAD DATA INFILE '/var/lib/mysql-files/datos_usuario.csv'
INTO TABLE USUARIO
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Cargar archivo con los datos de "descarga"
LOAD DATA INFILE '/var/lib/mysql-files/datos_relacion_descarga.csv'
INTO TABLE descarga
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

/* Tambien pueden insertarse registros manualmente; se muestran algunos ejemplos */
-- Inserto primero los datos de un nuevo usuario
INSERT INTO USUARIO (numero_cuenta, nombre_usuario, dir_usuario, pais_usuario)
VALUES ('DK5826710117879345', 'Ferdinand Dam', 'Nedergaardshaven 1A, 7. sal.', 'Dinamarca');

-- Inserto ahora una descarga que hace este usuario
INSERT INTO descarga (
codigo_app, 
numero_cuenta, 
tfn_usuario, 
fecha_descarga, 
puntuacion_app, 
comentario_app
) 
VALUES (
7412154, 
'DK5826710117879345', 
'+48 32 04 28 15', 
'2021-01-06', 
NULL, 
NULL
);

/* 4. Realizacion de consultas */

/* i) Fechas en que se realizan mas descargas de apps */
SELECT distinct(fecha_descarga), count(fecha_descarga) as num_descargas
FROM descarga
GROUP BY fecha_descarga
ORDER BY num_descargas desc;

/* ii) Pais de los usuarios que mas aplicaciones han descargado */
SELECT tfn_usuario, count(codigo_app) as num_apps_descargadas, pais_usuario
FROM descarga INNER JOIN USUARIO ON descarga.numero_cuenta = USUARIO.numero_cuenta
GROUP BY tfn_usuario, pais_usuario
ORDER BY num_apps_descargadas desc 
LIMIT 4;

/* iii) Puntuacion media de cada una de las apps */
SELECT APP.nombre_app, avg(descarga.puntuacion_app) as puntuacion_media_app 
FROM APP, descarga
WHERE APP.codigo_app = descarga.codigo_app
GROUP BY APP.nombre_app
ORDER BY puntuacion_media_app desc;

/* iv) Trigger para almacenar informacion sobre
la actualizacion del precio de una app (fecha y usuario) */
-- Eliminar si existe previamente
DROP TABLE IF EXISTS APP_LOG;

-- Crear tabla APP_LOG
CREATE TABLE APP_LOG (
nombre_app varchar(20) NOT NULL,
codigo_app numeric(7,0) NOT NULL,
precio_actualizado real, 
momento_actualizacion datetime, 
usuario varchar(50),
PRIMARY KEY (codigo_app, momento_actualizacion, usuario)
);

-- Eliminar el trigger si existe previamente
DROP TRIGGER IF EXISTS log_cambio_precio_app;

-- Creacion del trigger
DELIMITER $$
CREATE TRIGGER log_cambio_precio_app AFTER UPDATE 
ON APP FOR EACH ROW
BEGIN
INSERT INTO APP_LOG 
VALUES (
OLD.nombre_app,
OLD.codigo_app,
NEW.precio,
NOW(), 
CURRENT_USER()
);
END$$ 
DELIMITER ;

-- Actualizamos el precio de una app
UPDATE APP 
SET precio = 1.99
WHERE codigo_app = 1116589; 

/* v) Vista para mostrar nombre y pais de los usuarios que han puntuado y comentado las apps
sin mostrar informacion sensible (numero de cuenta, telefono, direccion...)
Se muestran primero las apps mejor puntuadas */
-- Eliminar la vista si existe previamente
DROP VIEW IF EXISTS opinion_usuarios;

CREATE VIEW opinion_usuarios (nombre_usuario, pais_usuario, puntuacion_app, comentario_app) as 
SELECT nombre_usuario, pais_usuario, puntuacion_app, comentario_app
FROM USUARIO INNER JOIN descarga ON USUARIO.numero_cuenta = descarga.numero_cuenta
WHERE puntuacion_app IS NOT NULL AND comentario_app IS NOT NULL
ORDER BY puntuacion_app desc;

/* vi) Longitud media de los comentarios de apps  */
SELECT avg(char_length(comentario_app)) as media_caracteres_comentario
FROM descarga
WHERE comentario_app IS NOT NULL;

/* vii) Empleado que mas tiempo ha trabajado y en que empresa */
SELECT  hay.dni_empleado, 
EMPRESA.nombre_empresa,
DATEDIFF(hay.fecha_fin_empleo, hay.fecha_inicio_empleo)/365 as annos_trabajados
FROM EMPRESA, hay
WHERE EMPRESA.codigo_empresa = hay.codigo_empresa
ORDER BY annos_trabajados desc
LIMIT 1;

/* viii) Tiendas de apps que mas apps contiene */
SELECT nombre_tienda_app, count(codigo_app) as num_apps
FROM contiene
GROUP BY nombre_tienda_app
ORDER BY num_apps desc
LIMIT 1;

/* ix) Usuarios que han descargado apps multicategoria, 
nombre de la app y categorias */
SELECT nombre_usuario, nombre_app, categoria_app 
FROM APP INNER JOIN descarga 
ON APP.codigo_app = descarga.codigo_app
INNER JOIN USUARIO ON descarga.numero_cuenta = USUARIO.numero_cuenta
-- Los nombres de las categorias estan separados por ";"
-- De esta forma podemos seleccionar las apps multicategoria
WHERE categoria_app LIKE '%;%';

/* x) Usuarios que han descargado apps de pago y nombre y precio de dichas apps */
SELECT nombre_usuario, nombre_app, precio
FROM USUARIO INNER JOIN APP INNER JOIN descarga 
ON USUARIO.numero_cuenta = descarga.numero_cuenta AND APP.codigo_app = descarga.codigo_app
WHERE precio > 0;

/* xi) Crear vista para mostrar informacion sobre el desarrollo de unas apps:
tiempo de desarrollo en dias, nombre de la empresa desarrolladora, 
numero de empleados que han trabajado en ella, dni del director de 
la app y empresa desarrolladora*/
-- Eliminar la vista si existe previamente
DROP VIEW IF EXISTS info_desarrollo_apps;

CREATE VIEW info_desarrollo_apps (
nombre_app, 
dias_en_desarrollo, 
nombre_empresa,
num_empleados,
dni_director_app
) AS SELECT 
nombre_app, 
DATEDIFF(fecha_fin_desarrollo, fecha_inicio_desarrollo) as dias_en_desarrollo, 
nombre_empresa,
count(dni_empleado) as num_empleados,
dni_director_app
FROM APP INNER JOIN realiza INNER JOIN EMPRESA
ON APP.codigo_app = realiza.codigo_app AND APP.codigo_empresa_creadora = EMPRESA.codigo_empresa
GROUP BY nombre_app, dias_en_desarrollo, nombre_empresa, dni_director_app 
ORDER BY dias_en_desarrollo asc;

/* xii) Espacio de memoria minimo, medio y maximo que ocupan las apps */
SELECT min(espacio_memoria_mb) as min_espacio, 
round(avg(espacio_memoria_mb)) as espacio_memoria_mb_medio, 
max(espacio_memoria_mb) as max_espacio
FROM APP;

/* xiii) Empresas que han tenido mas de 5 empleados en orden descendente */
SELECT nombre_empresa, count(dni_empleado) AS num_desarrolladores
FROM EMPRESA INNER JOIN hay ON EMPRESA.codigo_empresa = hay.codigo_empresa
GROUP BY nombre_empresa
HAVING num_desarrolladores > 5
ORDER BY num_desarrolladores desc;

/* xiv) Obtener nombre de usuario(s) que no ha(n) descargado ninguna app*/
SELECT nombre_usuario
FROM USUARIO
WHERE numero_cuenta NOT IN (SELECT numero_cuenta FROM descarga);

/* xv) Obtener empleados que tengan valores nulos en al menos uno de los campos*/
SELECT *
FROM EMPLEADO
WHERE concat(
calle, 
numero, 
codigo_postal, 
email_empleado, 
telefono_empleado
) IS NULL;

/* ---------------- FIN DEL SCRIPT ---------------- */