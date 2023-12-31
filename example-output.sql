CREATE SCHEMA student;
CREATE TABLE student.students (_id VARCHAR(255) PRIMARY KEY, age FLOAT, name VARCHAR(255), subject VARCHAR(255));
INSERT INTO student.students (_id, age, name, subject) VALUES ('64798c213f273a7ca2cf516a', 25, 'Nathan Lindgren', 'Maths');
INSERT INTO student.students (_id, age, name, subject) VALUES ('64798c213f273a7ca2cf516b', 18, 'Meggie Hoppe', 'English');
CREATE SCHEMA employee;
CREATE TABLE employee.employees (_id VARCHAR(255) PRIMARY KEY, age FLOAT, name VARCHAR(255), position VARCHAR(255), salary FLOAT);
INSERT INTO employee.employees (_id, age, name, position, salary) VALUES ('64798c213f273a7ca2cf516c', 35, 'Raymond Monahan', 'Engineer', 3767.925634753098);
CREATE TABLE employee.address (_id VARCHAR(255) PRIMARY KEY, employees__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255));
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('22f96d13-a022-4b61-8930-71419f07e2b3', '64798c213f273a7ca2cf516c', '32550 Port Gatewaytown', '18399');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('4f56d437-6a44-488b-bdfd-287abf90fa4c', '64798c213f273a7ca2cf516c', '3840 Cornermouth', '83941');
CREATE TABLE employee.phone (_id VARCHAR(255) PRIMARY KEY, employees__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('ba8ba0f2-c450-4601-a110-1d7df9553001', '64798c213f273a7ca2cf516c', '8764255212', '2762135091');
DELETE FROM student.students WHERE _id = '64798c213f273a7ca2cf516a';
INSERT INTO student.students (_id, age, name, subject) VALUES ('64798c213f273a7ca2cf516d', 19, 'Tevin Heathcote', 'English');
INSERT INTO employee.employees (_id, age, name, position, salary) VALUES ('64798c213f273a7ca2cf516e', 37, 'Wilson Gleason', 'Manager', 5042.121824095532);
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('9cc4b335-213b-494e-bb6f-13b3d96ecc6f', '64798c213f273a7ca2cf516e', '481 Harborsburgh', '89799');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('b0687dbc-7e1d-425b-8ff7-a9aaaf7cc5be', '64798c213f273a7ca2cf516e', '329 Flatside', '80872');
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('67d00a41-940d-45c6-ad27-0a7569861e66', '64798c213f273a7ca2cf516e', '7678456640', '8130097989');
INSERT INTO employee.employees (_id, age, name, position, salary) VALUES ('64798c213f273a7ca2cf516f', 31, 'Linwood Wilkinson', 'Manager', 4514.763474407185);
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('90e8765f-6817-42b4-aacb-e4f55b8256f1', '64798c213f273a7ca2cf516f', '96400 Landhaven', '41638');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('24d70b42-5ac8-4bc3-9c8f-02173f44bc5d', '64798c213f273a7ca2cf516f', '3939 Lightburgh', '99747');
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('c12d93c8-bee7-44b2-b557-9f0360949cc8', '64798c213f273a7ca2cf516f', '1075027422', '1641587035');
INSERT INTO student.students (_id, age, name, subject) VALUES ('64798c213f273a7ca2cf5170', 18, 'Camren Thompson', 'Science');
INSERT INTO employee.employees (_id, age, name, position, salary) VALUES ('64798c213f273a7ca2cf5171', 31, 'Meaghan Hettinger', 'Engineer', 6676.956103628756);
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('29b24089-5a06-44cc-af5f-8d5857f20688', '64798c213f273a7ca2cf5171', '51338 Landingbury', '74795');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('3dfb010e-d7e9-46ed-9166-59736b8a84e9', '64798c213f273a7ca2cf5171', '79033 West Locksmouth', '43555');
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('635f43fc-c414-40cb-a2b8-86a4fd03cea6', '64798c213f273a7ca2cf5171', '4613562303', '1889316722');
UPDATE employee.employees SET Age = 23 WHERE _id = '64798c213f273a7ca2cf5171';
ALTER TABLE employee.employees ADD COLUMN phone VARCHAR(255), ADD COLUMN workhours FLOAT, ADD COLUMN address VARCHAR(255);
INSERT INTO employee.employees (_id, age, name, position, salary, workhours) VALUES ('64798c213f273a7ca2cf5172', 20, 'Delta Bahringer', 'Developer', 2980.1271103167737, 6);
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('4f25649b-eb29-4ba3-9884-fa721940ec7a', '64798c213f273a7ca2cf5172', '2787 Trackview', '23598');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('d8af9260-786f-4eb2-9f9d-995f2d04cb8b', '64798c213f273a7ca2cf5172', '33659 South Mountainchester', '45086');
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('6d615612-e5c5-4132-bd4d-5cd3d3ec7fa4', '64798c213f273a7ca2cf5172', '9829848796', '5636590993');
ALTER TABLE student.students ADD COLUMN is_graduated BOOLEAN;
INSERT INTO student.students (_id, age, is_graduated, name, subject) VALUES ('64798c213f273a7ca2cf5173', 20, false, 'Freda Dare', 'Maths');
INSERT INTO student.students (_id, age, is_graduated, name, subject) VALUES ('64798c213f273a7ca2cf5174', 23, true, 'Kamille Jast', 'Maths');
INSERT INTO student.students (_id, age, is_graduated, name, subject) VALUES ('64798c213f273a7ca2cf5175', 19, false, 'Arden Kessler', 'Social Studies');
INSERT INTO employee.employees (_id, age, name, position, salary, workhours) VALUES ('64798c213f273a7ca2cf5176', 29, 'Chyna Kihn', 'Salesman', 6322.655857670963, 4);
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('f2b42ce4-7408-4b1d-a705-5823c9450434', '64798c213f273a7ca2cf5176', '403 Walksfurt', '75756');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('e8a10dd7-1184-42d4-be9c-2502a8b8b9e4', '64798c213f273a7ca2cf5176', '5012 Port Branchberg', '21969');
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('41d5c31d-9fda-4a62-941c-d12d2d548a80', '64798c213f273a7ca2cf5176', '1748534264', '2515301788');
INSERT INTO employee.employees (_id, age, name, position, salary, workhours) VALUES ('64798c213f273a7ca2cf5177', 38, 'Madie Klein', 'Engineer', 9811.365188057007, 5);
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('45cc056d-4c28-4bd7-a2f6-cc7311f99a14', '64798c213f273a7ca2cf5177', '73628 Port Knollchester', '97436');
INSERT INTO employee.address (_id, employees__id, line1, zip) VALUES ('624b68f7-a507-462b-bf9b-07ed7b112b31', '64798c213f273a7ca2cf5177', '93072 Lake Skywayhaven', '87218');
INSERT INTO employee.phone (_id, employees__id, personal, work) VALUES ('9b616c07-7ea0-4603-9d29-887b7a43c246', '64798c213f273a7ca2cf5177', '1498807115', '9172896730');