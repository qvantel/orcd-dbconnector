[![Build Status](https://travis-ci.com/flygare/QvantelDBConnector.svg?token=B6YLB31LLNNKsSzKXpCe&branch=master)](https://travis-ci.com/flygare/QvantelDBConnector)

# DBConnector
## Description
This service is used to fetch data from Cassandra using Spark and sending it to Graphite using an UDP connection.

## Changing configuration
The application configuration file can be found at:
```
/src/main/resources/application.conf
```
You can change settings such as Cassandra IP, batch size and update interval.

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
