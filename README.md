# storm-tutorial

Example [Apache Storm](https://storm.apache.org/) topology and a walkthrough for running a small cluster on **LXD** (ZooKeeper, Nimbus, Supervisor) on one machine.

## Requirements

- **Java 17** and **Maven 3.8+** on the machine where you build the JAR.
- For the distributed setup: **LXD** and three Ubuntu containers, as described in [INSTALLATION.md](INSTALLATION.md).

## Build

```bash
mvn clean package
```

The shaded application JAR is `target/storm-wordcount-1.0.jar`. Main class: `com.example.WordCountTopology`.

## Topology

Word count demo: a spout emits random sentences, a bolt splits words, a bolt counts per-word totals with fields grouping.

## Cluster setup and running Storm

Follow [INSTALLATION.md](INSTALLATION.md) for ZooKeeper 3.9+, Storm 2.8.7, `storm.yaml`, starting daemons, submitting the topology, the UI, and how to reset or tear down the lab.

## License

This tutorial project is provided as example material; adapt as needed for your use.
