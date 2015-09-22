# VisualDNA GraphStream Project

This is a prototype in 2 senses:

* Recursive stream processing framework which is included by source and as a project lives [here](https://github.com/michal-harish/donut)
* Streaming BSP equivalent of the Connected Components algorithm implemented ind [VisualDNA Identity Graph](http://stash.visualdna.com/projects/DXP/repos/dxp-spark/browse)

Contents

1. [GraphStream Pipeline Architecture](#architecture)
2. [Configuration](#configuration)
3. [Operations](#operations)
4. [Development](#development)

<a name="architecture">
## GraphStream Pipeline Architecture
</a>

*TODO 2 components which interact via `graphstream` topic, ...*

<a name="configuration">
## Configuration
</a>

Because the application is launched normally in the YARN cluster but *from a client machine*, in the application configuration (typially placed in **/etc/vdna/graphstream/config.properties**) you need `yarn1.site` parameter to point to your local hadoop-yarn configuration files, which in this example are expected to be in **/opt/envs/prod/etc/hadoop**. This configuration can be used on developer macs for launching from IntelliJ (see bleow) or Jenkins or other starting points.

```
# YARN configuration
yarn1.site=/opt/envs/prod/etc/hadoop
yarn1.queue=developers
yarn1.classpath=/opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar
# KAFKA configuration
zookeeper.connect=message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com
kafka.brokers=message-01.prod.visualdna.com:9092,message-02.prod.visualdna.com:9092,message-03.prod.visualdna.com:9092
```

NOTE: The `yarn1.classpath` means that we have already distributed large fat jars of scala-library and kafka so that we can have them here provided and distribute only a relatively thin jar each time we create a YARN launch context. There is a managed sys/scala-deploy repository which automatically distributes any added jar into /opt/scala/ location on every cluster node when Jenkins Global deploy sys/scala-deploy job is run.

<a name="operations">
## Operations
</a>

### Packaging components and submitting them to YARN cluster
```
mvn clean package
```
The maven command above will generate an assembly jar for each component:

1. target/SyncsToGraph.jar
2. target/ConnectedBSP.jar

The jars can be deployed to maven and from there managed by jenkins or if you want to test and have yarn environment on your local machine.
In each case the command is the same:

```
java -cp "`yarn classpath`:target/GraphStream-0.9.jar:<PATH_TO_SCALA_LIBRARY>" <COMPONENT_MAIN_OBJECT> <PATH_TO_GRAPHSTREAM_APP>
```

For example:

```
java -cp "`yarn classpath`:target/GraphStream-0.9.jar:/Users/mharis/.m2/repository/org/scala-lang/scala-library/2.10.4/scala-library-2.10.4.jar" \
 net.imagini.graphstream.syncstransform.SyncsToGraph \
 /etc/vdna/graphstream/config.properties
```

### Brokers configuration
For state topics we require log cleaner enabled on the brokers

```server.properties
log.cleaner.enable=true
```

### Creating normal topic with retention

```bash
./bin/kafka-topics.sh --create --topic graphstream --partitions 24 --replication-factor 1 --config cleanup.policy=delete
```

### Creating a compacted topic
And then creating topic with compact cleanup policy
```bash
./bin/kafka-topics.sh --create --topic graphstate --partitions 24 --replication-factor 1 --config cleanup.policy=compact
```

### Deleting topics

```bash
./bin/kafka-topics.sh --delete --topic graphstream
./bin/kafka-topics.sh --delete --topic graphstate
```


<a name="development">
## Development
</a>
### Configuring IntelliJ and Maven 

The GraphStream project has a direct soruce dependencies which are not standard maven pom-declared dependencies. These help to rapidly develop th underlying Donut and Yarn1 frameworks.

```
graphstream.git (stash.visualdna.com)
     |
     +-- donut.git  (github.com/michal-harish/donut)
            |
            +-- yarn1.git (github.com/michal-harish/yarn1)
```

After cloning the graphstream repo, you need to initialise the submodules:

```
git submodule update --init
cd donut
git submodule update --init
cd ..
```

If you look at the `pom.xml` you'll see a section for `build-helper-maven-plugin` adding the submodule sources:

```
...
<source>donut/yarn1/src/main/java</source>
<source>donut/core/src/main/scala</source>
...
```

This works fine with `mvn` command but unfortunately even the most recent version of IntelliJ still doesn't support build-helper pluging so in order for IntelliJ to see the sources you need to add them manually, once:

* ⌘ Project Structure >
	* donut/core/src/main/scala > right click `source`
	* donut/yarn1/src/main/java > right click `source`

By default, IntelliJ should preserve these added folders on re-import but in case it doesn't:

* ⌘ Preferences > 
	* Build, Excecution, Deployment > Build Tools > Maven > Importing
  		* check 'Keep source and test folders on reimport'


### Launching the application from IntelliJ

For launching the application from within the IntelliJ runtime there are several starting points all which are located in the *test* source net.imagini.graphstream.Launchers.scala.
The reason for test package is that many dependencies are provided and not available without hadoop/yarn environment but provided scope is available in the maven test phase:
There are two components(see [architecture](#architecture) above) and each has 2 different launchers:

1. YarnLaunch - submits the application to the YARN cluster and waits for completion printing any progress - stopping the application will attempt to kill the yarn context as well from the shutdown hook
2. LocalLaunch - is for debugging and doesn't actually submit the application to yarn and all streaming and processing happens locally

### TODOs

- In the recursive example graphstream emit null messages to clear the connections on eviction

