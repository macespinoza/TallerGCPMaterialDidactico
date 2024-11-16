
CREATE TABLE `labdataflow-356804.conjunto_ejemplo01.tabla_sample_part2`
(
  id INT64,
  nombre STRING,
  apellido STRING,
  fecha DATE
)
PARTITION BY fecha;

/*
bq show  --schema --format=prettyjson conjunto_ejemplo01.tabla_part_hora > tpart.json
cat tpart.json
bq mk --table  conjunto_ejemplo02.tabla_part_hora tpart.json
*/
---- Registrar en tabla
insert into `ci-dev-01-319720.conjunto_standar.tabla_sample1`(id,nombre,apellido,fecha)
(
select 1,"carlos", "farfan",cast("2022-08-10" as date)
union all 
select  3,"Mario", "farfan",CURRENT_DATE()
);

-- create view



---- consulta con sentencia where order group

SELECT * FROM `labdataflow-356804.nyc_citi_bike_trips.citibike_trips` WHERE 
 birth_year < 1980;

 select start_station_id, count(*) as cantidad from `labdataflow-356804.nyc_citi_bike_trips.citibike_trips`
 group by start_station_id;

   select start_station_id, count(*) as cantidad from `labdataflow-356804.nyc_citi_bike_trips.citibike_trips`
 group by start_station_id order by 2 desc;


 --array
with array_flow AS(
  select 1 as cod, [1,2, 6, 5] as dato
  union all select 2, [3,7, 6]
  union all select 3, [10,12, 16, 15, 18])
select 
  cod, d_dato
from 
  array_flow
cross join unnest(array_flow.dato) as d_dato;

select ARRAY
(select 1
  union all select 2
  union all select 3) as val;

select array_to_string(["mario","alex","javier","martin"],"-");

select generate_array(1,5) as numeric;

select generate_array(1,10, 2) as numeric;

select generate_date_array("2022-07-01", "2022-07-08");

select generate_date_array("2022-07-01", "2022-07-08", INTERVAL 2 DAY);

--DML
--insert
insert into `ci-dev-01-319720.conjunto_standar.tabla_sample1`(id,nombre,apellido,fecha)
(
select 1,"carlos", "farfan",cast("2022-08-10" as date)
union all 
select  3,"Mario", "farfan",CURRENT_DATE()
);

insert into `ci-dev-01-319720.conjunto_standar.tabla_sample1`values
(1,"carlos", "farfan",cast("2022-08-10" as date));


insert into `ci-dev-01-319720.conjunto_standar.tabla_sample1`(nombre,apellido) values
("carlos", "farfan");
---update


update `ci-dev-01-319720.conjunto_standar.tabla_sample1`
set nombre ="Miguel",
apellido ="Cotrina"
where id=2;

select * from `ci-dev-01-319720.conjunto_standar.tabla_sample1`
--delete

delete `ci-dev-01-319720.conjunto_standar.tabla_sample1` where id=1; --requiere where  borrar parte de la tabla
truncate table `ci-dev-01-319720.conjunto_standar.tabla_sample1` 
--no dml
drop table `ci-dev-01-319720.conjunto_standar.tabla_sample1` 

--merge

merge tabla_base a
using tabla_new_base b
on a.id = b.id
when matched then
update set ciudad = b.ciudad
when not matched then
insert (id, nombre, apellido, ciudad)
values(id, nombre, apellido, ciudad);

-- funcion cast

select 
  cast(10 as float64),
  cast(10.0 as integer),
  cast(20.4 as integer),
  cast(20.09 as integer),
  cast(current_date() as STRING ) as current_datet

-- function fecha

--date

with table_date as
(select current_date() as current_date_t)
select extract(DAY from current_date_t) -- month --year
FROM table_date;

select 
date_add(date(2022, 7,10), interval 5 day), 
date_sub(date(2022, 7,10), interval 5 day), 
date_diff(date(2022, 7,10), current_date(), day),
date_trunc(current_date(), MONTH);

--datetime

select current_datetime() as current_date_t;
select datetime(2022,7,15,8,7,6)
select extract(week from datetime(2022,7,15,8,7,6))
select datetime_add(datetime(2022,8,16,8,10,0), interval  20 MINUTE) as agreg_time
select date_diff(datetime(2022,8,16,8,10,0), current_datetime(), day)

-- time
select current_time();
select current_time("America/Bogota");
select TIME(8,10,30)
select extract(SECOND from TIME(8,10,30));
select TIME_ADD(current_time(), INTERVAL 10 MINUTE) as time_s;
select time_diff(current_time(), TIME(8,10,30), MINUTE);
-- timestamp

select current_timestamp()
select EXTRACT(minute from current_timestamp() ) 
select STRING(current_timestamp()) as time_actual

-- Funcion String/Math

select length("tres tigres"), ascii("A"), chr(65), lower("Casa"), upper("Casa"), initcap("Casa"),reverse("Casa"), concat("A","B","C")

select ltrim(" avion "),rtrim(" avion "), trim(" avion ")

select rpad("656", 8 , "#"), lpad("656",10, "0")

select 1,split("avion,carro,barco",",")
()
select left("aplicacion", 2) --ap
select right("aplicacion", 2) --on
select starts_with("comida", "co") --true
select starts_with("comida", "rt") --falso

select ends_with("comida", "ida") --true
select ends_with("comida", "idua") --false

select instr("camion", "ami")  -- posicion
select contains_substr("camion", "ami") --true
select substr("camioneta", 1,5) --camio

--ahora mate
-- least valor minimo
--greates  valor maximo
select trunc(10.5), round(10,5), ceil(10.5),floor(10.5), least(2,4,6), greatest(2,4,6), ln(100);

select abs(-4), sign(-5), sqrt(144), power(12,2)

select RAND()*100
select CAST(TRUNC(RAND()*3) AS INTEGER)
select mod(20,3)
--operaciones seguras
select safe_divide(3, 0)
--función definida por el usuario (UDF)

CREATE FUNCTION project1.mydataset.myfunction()
AS (
  (SELECT COUNT(*) FROM project1.mydataset.mytable)
);

--Store procedure

DECLARE d DATE DEFAULT CURRENT_DATE();
SET d = CURRENT_DATE();

CREATE OR REPLACE PROCEDURE schema1.proc1() BEGIN
  SELECT 1/0;
END;

CREATE OR REPLACE PROCEDURE schema1.proc2() BEGIN
  CALL schema1.proc1();
END;

-- Case
CASE product_id
  WHEN 1 THEN
    SELECT CONCAT('Product one');
  WHEN 2 THEN
    SELECT CONCAT('Product two');
  ELSE
    SELECT CONCAT('Invalid product');
END CASE;

-- copy table
CREATE TABLE myproject.mydataset.table1copy
COPY myproject.mydataset.table1;

-- Analisis de esquema

-- Retorna la metadata de un dataset.
SELECT * FROM myDataset.INFORMATION_SCHEMA.TABLES;