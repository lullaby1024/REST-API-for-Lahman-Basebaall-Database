# REST API and Data Access Layer Build-up for Lahman Baseball Database

## Introduction
This repository includes the source code for the micro-services for a Flask web application that allow users to explore the 2017 Lahman baseball database. The codes serve for a course project for *COMS W4111 - Introduction to Databases*.

## Deliverables

Differing HTTP methods can be used on the endpoint which map to application create, read, update, and delete (CRUD) operations. Responses will have a JSON format with keys `data` and `links`, where the data section contains the query response data and the links section contains URLs referencing:
  - The current result.
  - The next page in pagination.
  - The previous page in pagination.

### GET
- `/api/<dbname>/<table_name>/<primary_key_value>?fields=f1, f2, f3`.
  - e.g., `http://127.0.0.1:5000/api/lahman2017/appearances/willite01_BOS_1960?fields=G_all,GS`
- `/api/<dbname>/<table_name>/<primary_key>/<table2_name>?query_string`. The query string can contain multiple fields, limit and offset.
  - e.g., `http://127.0.0.1:5000/api/lahman2017/people/willite01/batting?fields=ab,h&yearid=1960`
  
### DELETE and PUT
- `/api/<dbname>/<table_name>/<primary_key>`

### POST
- `/api/<dbname>/<table_name>`
- `/api/<dbname>/<table_name>/<primary_key>/table_name`

## Tests
Feasibility is tested using unit tests and Postman.
- Run `aeneid/tests/unit_test.py` to test the major functions.
- Alternatively, run `aeneid/tests/create_table.sql` with `aeneid/tests/unit_test_manager.py` to create the tables for tests.
- Two json files are also included for outputs of some complex urls in `aeneid/test_results`.
- The following is a sample output.
<img src="https://github.com/lullaby1024/REST_API_Lahman_Baseball_Database/blob/master/aeneid/test_results/POSTMAN_screenshot.png" width="90%">
