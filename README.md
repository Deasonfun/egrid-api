# egrid-api
A simple to use API for running queries on the EPA eGRID datasets. Using the datasets found at: https://www.epa.gov/egrid/download-data

## Simple SQL Queries
Making a query to the API is as simple as making a SQL query. In the GET URL write your SQL statement with underscores in the place of spaces. Use the datasets as an example of what the SQL table looks like.

### Example:
If I wanted to get all the CO2 emissions from the state of Texas in the year 2020, I would GET this:

http://localhost:4000/query/select_stco2an_from_st20_where_pstatabb_=_'TX'

- ST20 is the table in which all the 2020 state data is stored.
- STCO2AN is the column  where all the annual CO2 output is stored.
- PSTATABB is the column where all the state abbreviations are stored.

Use the eGRID datasets as a reference of what the SQL tables look like :^)

## Running the API
To start the API on your machine, run the command:
`go run .`

To build the database run the main file inside the 'build-database' directory:
`cd build-database`
`go run .`
