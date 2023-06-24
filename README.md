# Mongo-oplog-to-sql

![mongo-oplog-to-sql-flow](assests/Mongo-oplog-to-sql.png)


> There are solutions that are able to store the JSON document in a relational table using PostgreSQL JSON support, but it doesn't solve the real problem of 'how to really use that data'. The system enables parsing of data in MongoDB's oplog and translating it to equivalent SQL statements.

The MongoDB oplog (operation log) is a capped collection that records all write operations that modify data within a MongoDB replica set, allowing for replication and providing a basis for high availability and data recovery.

## Installation

Due to the use of different external systems like MongoDB and PostgreSQL, the installation requires some previous steps. Take a look at out [Quickstart](#quickstart) Section in the documentation.

## Quickstart

1. Docker
    1. We need docker installed and running on your local. 
    2. If you don't have docker refer this [documentation](https://docs.docker.com/engine/install/)

2. Mongo Cluster
    1. We need mongo cluster if we do not provide an input oplog file as it reads the oplog from mongo database. If you have a mongo cluster running you can change the details in .env file . We have provided a `.env.example` file for reference

        1. If you don't have Mongo Cluster set up, you can do so by following the instructions in the repository [mongo-oplog-populator](https://github.com/one2nc/mongo-oplog-populator).

        2. To populate some data in the mongo cluster  follow the instruction in the repository [mongo-oplog-populator](https://github.com/one2nc/mongo-oplog-populator). 
    
    2. This will create a mongo database running in a container and using the populator will generate record which will result in mongo oplog's Which will help to test the mongo-oplog-to-sql testing .

3. Postgres sql
    1. We need postgres to execute the sql if we do not provide an output sql file
    2. If you have postgres running you can change the details in `.env` file. We have provided a `.env.example` file for reference.
    3. If you don't have postgres running locally you can follow the setps for setup provided in the readme

### Development Setup 
This section explain briefly how to setup the development environment.

1. Create a `.env` file and add the configs as provided in the `.env.example` file

2. To setup postgres `make setup`

3. To build a binary use `make build` 

4. To run a binary use `./MongoOplogToSQL` along with the following flags:
    - `-f`: using this flag you can specify the location of mongo-oplog file from where to read the oplogs, if this option is not provided application reads oplog from mongo client
    - `-o`: using this file you can specify the location of a file where you want to write all the generated sql commands, if this option is not provided then all the sql statements generated will be executed on postgres
    - If `-f` is not provided then it will read the data from Mongo database
    - If `-o` is not provided then it execute the generated sql to postgres database

5. To connect to postgres running on docker use `make connect` 

6. To tear down postgres, use `make setup-down`.


### Open Issues/Cases not handled

1. When there is any update made into the foreign table/associated table.

2. When there is deletion made into the foreign table/associated table.

--- 

