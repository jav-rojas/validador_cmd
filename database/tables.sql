-- Selección de db a utilizar (cambiar con proyecto)
USE EOD202012;

-- Borramos tablas 
DROP TABLE IF EXISTS Username;

-- Creación de tables utilizadas en GUI de Validador

-- ************************************** `Username`

CREATE TABLE `Username`
(
 `id`           integer NOT NULL AUTO_INCREMENT ,
 `username`     varchar(45) NOT NULL ,
 `password`     varchar(255) NOT NULL ,
 `email`        varchar(255) NULL ,
 `phonenumber`  varchar(255) NULL ,
 `created_at`   datetime NOT NULL ,
 `updated_at`   datetime NOT NULL ,

PRIMARY KEY (`id`)
) ENGINE=INNODB;

-- Ingresamos valores iniciales

INSERT INTO Username (username,password,email,phonenumber,created_at,updated_at) VALUES ("admin","Microdatos2020","jrojasc@fen.uchile.cl","951166679",now()- INTERVAL 3 HOUR,now()- INTERVAL 3 HOUR);