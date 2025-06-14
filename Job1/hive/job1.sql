CREATE EXTERNAL TABLE used_cars (
    make_name STRING,
    model_name STRING,
    year INT,
    description STRING,
    price DOUBLE,
    daysonmarket INT,
    city STRING,
    horsepower INT,
    engine_displacement DOUBLE,
    fuel_type STRING,
    transmission STRING,
    mileage INT,
    body_type STRING,
    exterior_color STRING,
    interior_color STRING,
    engine_cylinders INT,
    fuel_tank_volume DOUBLE,
    wheelbase DOUBLE,
    length DOUBLE,
    width DOUBLE,
    height DOUBLE,
    maximum_seating INT,
    owner_count INT,
    salvage STRING,
    has_accidents STRING,
    frame_damaged STRING,
    is_cpo STRING,
    is_new STRING,
    is_oemcpo STRING,
    city_fuel_economy DOUBLE,
    highway_fuel_economy DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/aliyo/input/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE cars_stats (
    make_name STRING,
    model_name STRING,
    num_cars INT,
    min_price DOUBLE,
    max_price DOUBLE,
    avg_price DOUBLE,
    available_years ARRAY<INT>
)
STORED AS ORC;

INSERT OVERWRITE TABLE cars_stats
SELECT
    make_name,
    model_name,
    COUNT(*) AS num_cars,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    ROUND(AVG(price), 2) AS avg_price,
    COLLECT_SET(year) AS available_years
FROM used_cars
GROUP BY make_name, model_name;