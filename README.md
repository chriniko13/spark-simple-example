### Spark Simple Example

#### Description 

Just a simple example playing with Apache Spark.


#### Infrastructure / Required Infra

* MySQL and MongoDB
    * Execute: `docker-compose up` in order to setup infrastructure.
    * Execute: `docker-compose down` in order to shutdown infrastructure.


#### Examples

* Example 1: 
Reading from csv, do some simple transformations and save to MySQL and MongoDB.

* Example 2:
Playing with Imdb open csv dataset, extract it from tar.gz (see code for more info)
and then run some sample queries, display data and store them to MySQL.
In order to run the example you will need to download the tsv zipped files of Imdb from here: https://www.imdb.com/interfaces/
and put them as they are (zipped) inside resources folder.

* Example 3:
Generates data to a MySQL database with the help of Slick Framework, and then loads 
these data to Spark.

* Example 4: TODO (Spark Streaming)

* Example 5: TODO (Spark GraphX)