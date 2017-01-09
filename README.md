# Cassandra top
This is a very simple console tool for monitoring column families read/write activities at remote cassandra host.

You can see which column families mostly affects disk utilization in near real time.

![Screenshot](http://i.imgur.com/6rJm3TM.png)

## Usage

`java -jar ctop.jar <host:jmx_port> <key_space> [refresh_interval_sec]`

Default refresh interval is 10 seconds.  

Example: `java -jar ctop.jar c10:7890 PixAPI`

Use Ctrl+C to exit.

## Build
Java 8 is required.

`mvn package`