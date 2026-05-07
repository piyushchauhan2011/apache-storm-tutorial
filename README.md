# storm-tutorial

Example [Apache Storm](https://storm.apache.org/) topology and a walkthrough for running a small cluster on **LXD** (ZooKeeper, Nimbus, Supervisor) on one machine.

## Requirements

- **JDK 17** (`java` and `javac` on your `PATH`, or `JAVA_HOME` set). You do **not** need Maven installed: this repo includes the [Maven Wrapper](https://maven.apache.org/wrapper/).
- Outbound HTTPS on first build (downloads Apache Maven 3.9.9 into `~/.m2/wrapper`; use your own mirror via `MVNW_REPOURL` if needed).
- For the distributed setup: **LXD** and three Ubuntu containers, as described in [INSTALLATION.md](INSTALLATION.md).

## Build

```bash
./mvnw clean package
```

On Windows: `mvnw.cmd clean package`.

The shaded application JAR is `target/storm-wordcount-1.0.jar`. Main class: `com.example.WordCountTopology`.

## Topology

Word count demo: a spout emits random sentences, a bolt splits words, a bolt counts per-word totals with fields grouping.

## Cluster setup and running Storm

Follow [INSTALLATION.md](INSTALLATION.md) for ZooKeeper 3.9+, Storm 2.8.7, `storm.yaml`, starting daemons, submitting the topology, the UI, and how to reset or tear down the lab.

## License

This tutorial project is provided as example material; adapt as needed for your use.
