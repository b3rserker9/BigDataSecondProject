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


CREATE TABLE city_year_price_report (
    city STRING,
    year INT,
    price_category STRING,
    num_cars INT,
    avg_days_on_market DOUBLE,
    top_words ARRAY<STRING>
)
STORED AS ORC;

-- Step 1: Calcolo statistiche base (numero auto, media daysonmarket, per city, year, fascia prezzo)
WITH price_stats AS (
  SELECT
    city,
    year,
    CASE
      WHEN price > 50000 THEN 'alto'
      WHEN price >= 20000 AND price <= 50000 THEN 'medio'
      ELSE 'basso'
    END AS price_category,
    COUNT(*) AS num_cars,
    AVG(daysonmarket) AS avg_days_on_market
  FROM used_cars
  GROUP BY city, year, price_category
),

-- Step 2: Estrazione e conteggio parole da description
exploded_words AS (
  SELECT
    city,
    year,
    CASE
      WHEN price > 50000 THEN 'alto'
      WHEN price >= 20000 AND price <= 50000 THEN 'medio'
      ELSE 'basso'
    END AS price_category,
    LOWER(word) AS word
  FROM used_cars
  LATERAL VIEW explode(split(description, ' ')) w AS word
),

word_freq AS (
  SELECT
    city,
    year,
    price_category,
    word,
    COUNT(*) AS freq
  FROM exploded_words
  WHERE word != '' -- escludi parole vuote
  GROUP BY city, year, price_category, word
),

-- Step 3: Otteniamo le top 3 parole per ogni city-year-price_category
top_words_ranked AS (
  SELECT
    city,
    year,
    price_category,
    word,
    freq,
    ROW_NUMBER() OVER (PARTITION BY city, year, price_category ORDER BY freq DESC) AS rn
  FROM word_freq
),

top_3_words AS (
  SELECT
    city,
    year,
    price_category,
    COLLECT_LIST(word) AS top_words
  FROM top_words_ranked
  WHERE rn <= 3
  GROUP BY city, year, price_category
)

-- Step 4: Join risultati e output finale
INSERT OVERWRITE TABLE city_year_price_report
SELECT
  p.city,
  p.year,
  p.price_category,
  p.num_cars,
  p.avg_days_on_market,
  t.top_words
FROM price_stats p
LEFT JOIN top_3_words t
  ON p.city = t.city AND p.year = t.year AND p.price_category = t.price_category;