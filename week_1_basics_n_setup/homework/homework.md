# Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


### Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string` :white_check_mark:
- `--idimage string`
- `--idfile string`


### Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3 :white_check_mark:
- 7

## Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

**SOLUTION**
The `ingest_data.py` file is a pipeline that takes the filename and pu the data into postgres

Execution:
1. Execute 
   ```bash
    docker-compose up -d
   ```
2. Build the image
   ```bash
    docker build -t taxi_ingest:v002 . 
   ```
3. Set the URL variable file 1
   ```bash
    URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
   ```
4. Run the image
``` bash
    docker run -it \
    taxi_ingest:v002 --user=root --password=root --host=host.docker.internal --port=5432 --db=ny_taxi --table_name=green_tripdata --url=${URL}
```
5. Set the URL variable file 2 
   URL=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

4. Run the image
``` bash
    docker run -it \
    taxi_ingest:v002 --user=root --password=root --host=host.docker.internal --port=5432 --db=ny_taxi --table_name=zones --url=${URL}
```
## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530 :white_check_mark:
- 17630
- 21090

**QUERY**
````
   SELECT COUNT(*)
      FROM green_tripdata 
      WHERE DATE(lpep_pickup_datetime ) = '2019-01-15' 
      AND DATE(lpep_dropoff_datetime ) = '2019-01-15' 
````
## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15 :white_check_mark:
- 2019-01-10

**QUERY**
````
   SELECT MAX(trip_distance) as max_trip_distance,
         DATE(lpep_pickup_datetime)
   FROM green_tripdata 
   GROUP BY DATE(lpep_pickup_datetime)
   ORDER BY max_trip_distance DESC
   LIMIT 1
````


## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254 :white_check_mark:
- 2: 1282 ; 3: 274

**QUERY**
````
   SELECT passenger_count,
         COUNT(passenger_count) 
   FROM green_tripdata 
   WHERE DATE(lpep_pickup_datetime ) = '2019-01-01' 
   AND passenger_count IN ( 2, 3)
   GROUP BY passenger_count  
````

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza :white_check_mark:

**QUERY**
````
   SELECT zdo."Zone" as DOzone,
         MAX(gt."tip_amount") as "max_tip"	
   FROM green_tripdata gt
   JOIN zones zpu
   ON zpu."LocationID" = gt."PULocationID"
   JOIN zones zdo
      ON zdo."LocationID" = gt."DOLocationID"
   WHERE zpu."Zone" = 'Astoria'
   GROUP BY zdo."Zone"
   ORDER BY MAX(gt."tip_amount") DESC
````

## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET


## Solution

We will publish the solution here