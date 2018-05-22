# Cassandra top
This is a very simple console tool for monitoring column families read/write activities at remote cassandra host.

You can see which column families mostly affects disk utilization in near real time.

![Screenshot](http://i.imgur.com/6rJm3TM.png)

## Usage

`java -jar ctop.jar <path to ctop.properties file>`

Sample properties file is given below

    #ctop confs, defaults to 10 when not given
    ctop.interval.sec=1

    #cassandra confs
    cassandra.host=<cassandraHost>
    cassandra.jmx.port=<jmxPort defaults to 7199 when not given>
    cassandra.jmx.username=<jmxUsername if auth enabled>
    cassandra.jmx.pw=<jmxPassword if auth enabled>
    cassandra.keyspace=<keyspace name or regex>

    #target keyspace in Cassandra, possible values are KEYSPACE, REGEX, ALL
    cassandra.target.type=REGEX

    #metrics reporter confs, possible values are GRAPHITE, CONSOLE and NONE
    metrics.type=GRAPHITE

    #graphite reporter confs
    graphite.prefix=<graphitePrefix and defaults to ctop>
    graphite.host=<graphiteHost>
    graphite.port=<graphitePort and defaults to 2003 when not given>
    graphite.freq=<frequencyInterval in minutes and defaults to 1 when not given>

Default refresh interval is **10** seconds.

The application by defaults prints the metrics on console. When target type is set to KEYSPACE it will print all reads and writes metrics for the given specific keyspace.

When target type is ALL it will print all metrics for all keyspaces including system keyspace.

Cases on a hosted production the keyspaces follow company name prefixed and so REGEX is another option as JMX allows it. Our case all our keyspaces prefixed with **cassks_<keyspacename>** and so the recommended value for cassandra.keyspace is cassks_* and in that case all keyspace data is printed except the system keyspaces.

Graphite and Console metrics publishing is enabled when metrics.type is set to GRAPHITE or CONSOLE. To disable it the value should be NONE.

When enabled for Graphite there are metrics like

- com.pixonic.ctop.metrics.MetricsCollector.totalRead
- com.pixonic.ctop.metrics.MetricsCollector.totalWrite

is pushed as global count and for each specific keyspace metrics like
- <keyspacename>.table.<tablename>.read
- <keyspacename>.table.<tablename>.write

are published to graphiteHost.


Example: `java -jar ctop.jar /Users/johndoe/Documents/ctop.properties`

Use Ctrl+C to exit.

## Build
Java 8 is required.

`mvn package`
