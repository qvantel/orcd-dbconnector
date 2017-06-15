[![Build Status](https://travis-ci.org/qvantel/orcd-dbconnector.svg?branch=master)](https://travis-ci.org/qvantel/orcd-dbconnector)
[![codecov](https://codecov.io/gh/qvantel/orcd-dbconnector/branch/master/graph/badge.svg)](https://codecov.io/gh/qvantel/orcd-dbconnector)

# DBConnector
## Description
This program is used to fetch data from Cassandra using Spark Cassandra Connector and sending it to Graphite over TCP.

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
