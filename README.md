[![Build Status](https://travis-ci.com/flygare/QvantelDBConnector.svg?token=B6YLB31LLNNKsSzKXpCe&branch=master)](https://travis-ci.com/flygare/QvantelDBConnector)

# DBConnector
## Description
This service is used to fetch data from Cassandra using Spark and sending it to Graphite using an UDP connection.

## Usage
Run DBConnector:
```
$ sbt run
```

Run just tests:
```
$ sbt test
```

Run the tests with enabled coverage:
```
$ sbt clean coverage test
```

To generate the coverage reports run
```
$ sbt coverageReport
```
