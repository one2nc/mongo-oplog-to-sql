# Mongo-oplog-to-sql

# Problem Statement

Write a program which can parse MongoDB operations log (Oplog) and generate equivalent SQL statements.

We have a scenario where an organisation was using MongoDB at the start but now needs to move to a RDBMS database. This data transition can be made easy if we can find a way to convert the JSON documents in MongoDB collections to equivalent rows in relational DB tables. That's the purpose of this program.

The MongoDB server generates the Oplog which is an ordered collection of all the write operations (insert, update, delete) to the MongoDB. Your job is to parse these oplogs and generate equivalent SQL statements.

There’s already an open source tool, [stampede](https://github.com/torodb/stampede), that converts MongoDB oplogs to sql; we are simply attempting to develop an implementation in Go.

A sample MongoDB oplog looks like:

```json
{
  "op" : "i",
  "ns" : "test.student",
  "o" : {
    "_id" : "635b79e231d82a8ab1de863b",
    "name" : "Selena Miller",
    "roll_no" : 51,
    "is_graduated" : false,
    "date_of_birth" : "2000-01-30"
  }
}
```

The main fields in the oplog are:

- op: This indicates the type of operation. It can be `i` (insert), `u` (update), `d` (delete), `c` (command), `n` (no operation). For this implementation, we’ll only care about insert, update and delete operations.
- ns: This indicates the namespace. Namespace consists of database and collection name separated by a `.` In above case, database name is `test` and collection name is `student`.
- o: This indicates the new data for insert or update operation. In above case, a student document is inserted in the collection.

The oplog contains some other fields like version, timestamp, etc. but for our consideration, we can ignore those.

We have divided the problem statement into multiple stories. You’re supposed to implement the stories.

Note: All these stories are dependent on each other. i.e. stories (and their test cases) need to be executed sequentially for it to work.

## Story 1

Parse the insert oplog JSON and convert that into equivalent SQL insert statement. When inserting a record, check if the table exists. If not, create it first.

Here’s the mapping of MongoDB concepts to their equivalent relational database concepts

- Database in MongoDB maps to schema in relational database
- Collection in MongoDB maps to table in relational database
- A single JSON document in MongoDB maps typically to a row in relational database.

Sample Input:

```json
{
  "op" : "i",
  "ns" : "test.student",
  "o" : {
    "_id" : "635b79e231d82a8ab1de863b",
    "name" : "Selena Miller",
    "roll_no" : 51,
    "is_graduated" : false,
    "date_of_birth" : "2000-01-30"
  }
}
```

Expected Output:

```sql
INSERT INTO test.student 
   (_id, date_of_birth, is_graduated, name, roll_no) 
VALUES 
   ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);
```

You can assume that the above oplog is generated by following MongoDB command that inserts some data in student table.

```sql
use test;

db.student.insertOne(
	{
	name: "Selena Miller", 
	roll_no:51, 
	is_graduated:false, 
	date_of_birth: "2000-01-30"
	}
);
```

Assumptions:

- The `op` key indicates the operation. `i` stands for insert operation.
- The oplog contains some other fields like version, timestamp, etc. but for our consideration, we can ignore those. Hence, the above oplog contains only the fields which are relevant for our SQL conversion use case.
- The `ns` key indicates a combination of db name and collection name. In above example, db name is `test` and collection name is `student`. Db name and collection name will be separated by a `.`
- For simplicity, for now, assume that there are no nested objects in the Mongo collection.

Expectations:

- Your code should be generic enough to extract the db and collection name from `ns` field. It should also extract following types from JSON - string, boolean and number. In above example, `name` is a string variable, `roll_no` is a number variable and `is_graduated` is boolean. For now, you can treat `date_of_birth` as string (and not a date field type)

## Story 2

Parse the update oplog JSON and convert that into equivalent SQL update statement.

Sample Input for Setting:

```json
{
   "op":"u",
   "ns":"test.student",
   "o":{
      "$v":2,
      "diff":{
         "u":{
            "is_graduated":true
         }
      }
   },
   "o2": {
      "_id":"635b79e231d82a8ab1de863b"
   }
}
```

Expected Output:

```sql
UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';
```

Sample Input for Un-setting:

```json
{
   "op":"u",
   "ns":"test.student",
   "o":{
      "$v":2,
      "diff":{
         "d":{
            "roll_no":false
         }
      }
   },
   "o2":{
      "_id":"635b79e231d82a8ab1de863b"
   }
}
```

Expected Output:

```sql
UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';
```

Assumptions:

- The `u` in `op` key stands for update operation.
- The `o` field contains the update operation details. In this case, it includes the following subfields:
    - The `$v` field specifies the protocol version used for the update operation. In this case, the value is 2.
    - The `diff` field represents the changes being made to the document. In this case, it contains the following subfield:
        - `u`: modifies a field of a document and sets the value of a field in the document.
        - `d`: removes a field from a document.
- The key `o2` represents the row identifier or the WHERE clause field in SQL
- For simplicity, assume that the `_id` would always be the updation criteria
- Assume that no new column will be added via this update operation for now.
- Assume that the table and the data exist from before (which can be done manually).

## Story 3

Parse the delete oplog JSON and convert that into equivalent SQL delete statement.

Sample Input:

```json
{
  "op" : "d",
  "ns" : "test.student",
  "o" : {
    "_id" : "635b79e231d82a8ab1de863b"
  }
}
```

Expected Output:

```sql
DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';
```

Assumptions:

- The `d` in `op` key stands for delete.
- The `o` key contains the `_id` of the field to be deleted
- For simplicity, assume that the `_id` would always be the deletion criteria
- Assume that the table exists from before (which can be done manually).

## Story 4 (create table with one oplog entry)

This story is the modification of [Story 1](https://www.notion.so/Mongo-oplog-to-sql-2751153a4218429e95089914f2dd8722?pvs=21). In this story, you’ll parse the same insert oplog JSON from Story 1 and convert it to equivalent SQL statements. However, you’ll also generate the `create schema` and `create table` statements along with `insert into` statement.

Features to implement in this story:

- Generate `CREATE SCHEMA` SQL statement
- Generate `CREATE TABLE` SQL statement
- Generate `INSERT INTO` SQL statement

Sample Input:

```json
{
  "op": "i",
  "ns": "test.student",
  "o": {
    "_id": "635b79e231d82a8ab1de863b",
    "name": "Selena Miller",
    "roll_no": 51,
    "is_graduated": false,
    "date_of_birth": "2000-01-30"
  }
}
```

Expected output:

```sql
CREATE SCHEMA test;

CREATE TABLE test.student
  (
     _id           VARCHAR(255) PRIMARY KEY,
     date_of_birth VARCHAR(255),
     is_graduated  BOOLEAN,
     name          VARCHAR(255),
     roll_no       INTEGER
  );

INSERT INTO test.student (_id, name, roll_no, is_graduated, date_of_birth) VALUES ('635b79e231d82a8ab1de863b', 'Selena Miller', 51, false, '2000-01-30');
```

Assumptions:

- In the above output, the `create table` statement is split into multiple lines. This is done only for readability purpose. You should generate the create table statement in a single line.
- Feel free to modify the code and tests written as part of Story 1. The input to Story 1 and 4 is same, but in the output, we now expect `create schema` and `create table` statements as well.

Expectations:

- You should be able to run all the SQL statement generated by your program into PostgreSQL without any errors.

## Story 5 (create table with multiple oplog entries)

Until now, we were handling only one oplog at a time. However, now we need to handle multiple `insert` oplogs for the same collection. For simplicity, let’s assume that there are no field changes across these two oplogs. The only thing that changes is the value of the fields.

As per previous story, `create schema` and `create table` statements are generated for every `insert` oplog. Now, we need to fix the issue where the `create schema` and `create table` statements are generated only once for each collection.

Sample Input:

```json
[
  {
    "op": "i",
    "ns": "test.student",
    "o": {
      "_id": "635b79e231d82a8ab1de863b",
      "name": "Selena Miller",
      "roll_no": 51,
      "is_graduated": false,
      "date_of_birth": "2000-01-30"
    }
  },
  {
    "op": "i",
    "ns": "test.student",
    "o": {
      "_id": "14798c213f273a7ca2cf5174",
      "name": "George Smith",
      "roll_no": 21,
      "is_graduated": true,
      "date_of_birth": "2001-03-23"
    }
  }
]
```

In the input, there are two `insert` oplogs for the same database and collection. The only difference in the two oplogs is the values of JSON fields. The type and the number of fields are same for both oplogs.

Expected output:

```sql
CREATE SCHEMA test;

CREATE TABLE test.student
  (
     _id           VARCHAR(255) PRIMARY KEY,
     date_of_birth VARCHAR(255),
     is_graduated  BOOLEAN,
     name          VARCHAR(255),
     roll_no       FLOAT
  );

INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51.0);

INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21.0);
```

Assumptions:

- In the above output, the `create table` statement is split into multiple lines. This is done only for readability purpose. You should generate the create table statement in a single line.

Expectations:

- The input to the program has changed from a single oplog JSON to an array of oplogs. Make sure your code is able to handle both.
- You will have to modify the code and tests written as part of Story 4.

## Story 6 (alter table with multiple oplog entries)

The input for this story is very similar to Story 5 above. Except, in the second oplog, there’s a new field -`phone`. Your job is to generate an `alter table`  statement and then generate an `insert into` statement for the second oplog.

Thus, you’ll have to generate SQL statements in the following order:

- Generate `CREATE SCHEMA` SQL statement
- Generate `CREATE TABLE` SQL statement
- Generate `INSERT INTO` SQL statement
- Generate `ALTER TABLE` SQL statement
- Generate `INSERT INTO` SQL statement

Sample Input:

```jsx
[
  {
    "op": "i",
    "ns": "test.student",
    "o": {
      "_id": "635b79e231d82a8ab1de863b",
      "name": "Selena Miller",
      "roll_no": 51,
      "is_graduated": false,
      "date_of_birth": "2000-01-30"
    }
  },
  {
    "op": "i",
    "ns": "test.student",
    "o": {
      "_id": "14798c213f273a7ca2cf5174",
      "name": "George Smith",
      "roll_no": 21,
      "is_graduated": true,
      "date_of_birth": "2001-03-23",
      "phone": "+91-81254966457"
    }
  }
]
```

Expected Output: 

```jsx
CREATE SCHEMA test;

CREATE TABLE test.student
  (
     _id           VARCHAR(255) PRIMARY KEY,
     date_of_birth VARCHAR(255),
     is_graduated  BOOLEAN,
     name          VARCHAR(255),
     roll_no       FLOAT
  );

INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51.0);

ALTER TABLE test.student ADD COLUMN phone VARCHAR(255);

INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no, phone) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21.0, '+91-81254966457');
```

Expectation:

- You will have to modify the code and tests written as part of Story 5.
- Your code should also handle case of more than two oplogs for the same collection.
- Your program should assign null values to columns for which the JSON fields are missing.
- Currently, in the sample input, we are only considering an addition of one field (phone). However, your program should handle addition of any number of new fields and generate those many number of `alter table` statements.

## Instructions

1. You will have to create a new repo in language of your choice (Java, Go, etc)
2. Use the [example-input.json](example-input.json) file as the input to the program.
3. The output should be an `output.sql` file which has the correct SQL statements to prepare the SQL database. You can compare your output with [example-output.sql](example-output.sql). 
4. If something is unclear, try to figure out the solution, given the test cases. If something is still unclear, assume what you think is right and make a note in the readme.

## Evaluation Criteria

1. The code should provide the necessary documentation to run the program.
2. The code should produce correct output for the given input.
3. The code should have necessary tests.
4. Bonus points for code quality and best practices.