This is a great learning combo — LXD containers give you a mini distributed cluster on your laptop, and Apache Storm is a perfect workload to run across them. Let me give you a full working setup with architecture overview, then the step-by-step guide.

Here's the architecture you'll be building:Now here's the complete setup guide — everything from LXD init to running a Java topology across the containers.

---

## 1. Install and initialize LXD

```bash
sudo snap install lxd
sudo lxd init --auto          # uses default bridge lxdbr0, 10.x.x.0/24

# add your user to the lxd group so you don't need sudo
sudo usermod -aG lxd $USER
newgrp lxd
```

---

## 2. Launch the three containers

```bash
lxc launch ubuntu:22.04 zookeeper
lxc launch ubuntu:22.04 nimbus
lxc launch ubuntu:22.04 supervisor
```

They all get IPs on the same `lxdbr0` bridge, so they can reach each other by container name (LXD sets up DNS automatically on the bridge).

Verify:
```bash
lxc list
```

---

## 3. Install Java and Storm on each container

You only **run** Storm on `nimbus` and `supervisor`, but installing the tarball on the `zookeeper` container too is optional and harmless (handy if you want the `storm` CLI there for debugging).

### Option A — download inside the container (`wget`)

Use when the container has working outbound HTTPS. Repeat for each container (replace `CONTAINER`):

```bash
lxc exec CONTAINER -- bash -c "
  apt-get update -q &&
  apt-get install -y openjdk-17-jdk wget &&
  wget -q https://archive.apache.org/dist/storm/apache-storm-2.8.7/apache-storm-2.8.7.tar.gz -O /tmp/storm.tgz &&
  tar -xzf /tmp/storm.tgz -C /opt &&
  ln -sfn /opt/apache-storm-2.8.7 /opt/storm
"
```

### Option B — download on the host, push with `lxc file push`

Use when you prefer one download, air-gapped-ish setups, or slow/unreliable networking inside instances.

On the **host**:

```bash
STORM_VER=2.8.7
curl -fsSL -o "/tmp/apache-storm-${STORM_VER}.tar.gz" \
  "https://archive.apache.org/dist/storm/apache-storm-${STORM_VER}/apache-storm-${STORM_VER}.tar.gz"
```

Push the archive into each container and extract (replace `CONTAINER` with `nimbus`, `supervisor`, and optionally `zookeeper`):

```bash
CONTAINER=nimbus
lxc file push "/tmp/apache-storm-${STORM_VER}.tar.gz" "${CONTAINER}/tmp/storm.tgz"
lxc exec "${CONTAINER}" -- bash -c "
  apt-get update -q &&
  apt-get install -y openjdk-17-jdk &&
  tar -xzf /tmp/storm.tgz -C /opt &&
  ln -sfn /opt/apache-storm-${STORM_VER} /opt/storm
"
```

`lxc file push` copies into the instance as root; the path `CONTAINER/tmp/storm.tgz` is `/tmp/storm.tgz` inside the guest.

---

## 4. Install ZooKeeper on the zookeeper container

**Storm 2.6+ expects a modern ZooKeeper server (3.5+; use 3.8/3.9).** The `zookeeperd` package on Ubuntu 22.04 ships ZooKeeper **3.4.x**, which is too old: Nimbus and Supervisor fail with `KeeperException$Unimplemented` when registering in ZooKeeper.

Install a current binary release (example: 3.9.3) and run it under systemd:

```bash
lxc file push ./apache-zookeeper-3.9.3-bin.tar.gz zookeeper/tmp/zk.tgz   # download on host first if needed
# Or from inside the container (archive is reliable when dlcdn paths move):
lxc exec zookeeper -- bash -c "
  apt-get update -q && apt-get install -y wget openjdk-17-jdk-headless &&
  wget -q https://archive.apache.org/dist/zookeeper/zookeeper-3.9.3/apache-zookeeper-3.9.3-bin.tar.gz -O /tmp/zk.tgz &&
  tar -xzf /tmp/zk.tgz -C /opt && ln -sfn /opt/apache-zookeeper-3.9.3-bin /opt/zookeeper &&
  mkdir -p /var/lib/zookeeper &&
  printf '%s\n' 'tickTime=2000' 'initLimit=10' 'syncLimit=5' 'dataDir=/var/lib/zookeeper' 'clientPort=2181' 'admin.enableServer=false' > /opt/zookeeper/conf/zoo.cfg &&
  printf '%s\n' '[Unit]' 'Description=Apache ZooKeeper (Storm)' 'After=network.target' '' '[Service]' 'Type=simple' 'WorkingDirectory=/opt/zookeeper' 'ExecStart=/opt/zookeeper/bin/zkServer.sh start-foreground' 'Restart=on-failure' '' '[Install]' 'WantedBy=multi-user.target' > /etc/systemd/system/zookeeper-storm.service &&
  systemctl stop zookeeper 2>/dev/null; systemctl disable zookeeper 2>/dev/null;
  # If an old 3.4 JVM is still bound to 2181 after stopping the sysv unit, kill it:  fuser -k 2181/tcp
  systemctl daemon-reload && systemctl enable --now zookeeper-storm
"
```

Check it's up (ZK 3.5+ restricts four-letter words by default; `srvr` works):

```bash
lxc exec zookeeper -- bash -c "echo srvr | nc localhost 2181 | head -1"
# should mention Zookeeper version: 3.9.x
```

---

## 5. Get the Zookeeper container IP

```bash
ZK_IP=$(lxc list zookeeper -c 4 --format csv | awk '{print $1}')
echo $ZK_IP
```

---

## 6. Configure Storm

Create `/opt/storm/conf/storm.yaml` on **both** `nimbus` and `supervisor`:

```bash
for NODE in nimbus supervisor; do
lxc exec $NODE -- bash -c "cat > /opt/storm/conf/storm.yaml << 'EOF'
storm.zookeeper.servers:
  - \"$ZK_IP\"

nimbus.seeds:
  - \"$(lxc list nimbus -c 4 --format csv | awk '{print $1}')\"

storm.local.dir: \"/var/storm\"
ui.port: 8080
supervisor.slots.ports:
  - 6700
  - 6701
  - 6702
  - 6703
EOF
mkdir -p /var/storm
"
done
```

---

## 7. Start Storm daemons

```bash
# on nimbus: start nimbus + UI
lxc exec nimbus -- bash -c "nohup /opt/storm/bin/storm nimbus > /var/log/nimbus.log 2>&1 &"
lxc exec nimbus -- bash -c "nohup /opt/storm/bin/storm ui > /var/log/ui.log 2>&1 &"

# on supervisor: start supervisor
lxc exec supervisor -- bash -c "nohup /opt/storm/bin/storm supervisor > /var/log/supervisor.log 2>&1 &"
```

Storm UI is now accessible from your host at:
```
http://<nimbus-ip>:8080
```

---

## 8. The Java WordCount topology

Here's a self-contained Maven project. Three files:

**`pom.xml`** (see the `storm-tutorial` repo for the full file — use Java 17, `storm-core` **2.8.7** with `provided` scope, `maven-shade-plugin`, and `maven-compiler-plugin` with `<release>17</release>`.)

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>storm-wordcount</artifactId>
  <version>1.0</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.storm</groupId>
      <artifactId>storm-core</artifactId>
      <version>2.8.7</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.13.0</version>
        <configuration>
          <release>${maven.compiler.release}</release>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <artifactSet>
                <excludes>
                  <exclude>org.apache.storm:storm-core</exclude>
                </excludes>
              </artifactSet>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

**`src/main/java/com/example/WordCountTopology.java`**
```java
package com.example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;

public class WordCountTopology {

    // --- Spout: emits random sentences ---
    public static class SentenceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private final String[] sentences = {
            "the quick brown fox jumps over the lazy dog",
            "apache storm is a distributed stream processing system",
            "lxd containers make great lightweight virtual machines",
            "java makes distributed systems a joy to build"
        };
        private final Random random = new Random();

        @Override
        public void open(Map<String, Object> conf, TopologyContext ctx, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(500); // emit every 500ms
            String sentence = sentences[random.nextInt(sentences.length)];
            collector.emit(new Values(sentence));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    // --- Bolt 1: split sentence into words ---
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            for (String word : sentence.split("\\s+")) {
                collector.emit(tuple, new Values(word.toLowerCase()));
            }
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    // --- Bolt 2: count words ---
    public static class CountBolt extends BaseRichBolt {
        private final Map<String, Long> counts = new HashMap<>();
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext ctx, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            counts.merge(word, 1L, Long::sum);
            System.out.printf("[COUNT] %s -> %d%n", word, counts.get(word));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    }

    // --- Topology wiring ---
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentence-spout", new SentenceSpout(), 1);

        builder.setBolt("split-bolt", new SplitBolt(), 2)
               .shuffleGrouping("sentence-spout");

        builder.setBolt("count-bolt", new CountBolt(), 2)
               .fieldsGrouping("split-bolt", new Fields("word")); // same word → same bolt instance

        Config config = new Config();
        config.setNumWorkers(2);
        config.setMessageTimeoutSecs(30);

        // Remote submit to the LXD nimbus
        StormSubmitter.submitTopologyWithProgressBar("word-count", config, builder.createTopology());
    }
}
```

---

## 9. Build and submit

```bash
mvn clean package -q

# submit from the host (requires `storm` on your PATH)
storm jar target/storm-wordcount-1.0.jar \
  com.example.WordCountTopology \
  -c nimbus.seeds="[\"$(lxc list nimbus -c 4 --format csv | awk '{print $1}')\"]"
```

If you do **not** have the Storm CLI on the host, push the JAR and submit from `nimbus` (reads `nimbus.seeds` from `/opt/storm/conf/storm.yaml`):

```bash
lxc file push target/storm-wordcount-1.0.jar nimbus/tmp/storm-wordcount-1.0.jar
lxc exec nimbus -- /opt/storm/bin/storm jar /tmp/storm-wordcount-1.0.jar com.example.WordCountTopology
```

---

## 10. Observe the topology

From your host, open the Storm UI in a browser:
```
http://<nimbus-ip>:8080
```

Or watch logs in the supervisor container:
```bash
lxc exec supervisor -- tail -f /var/storm/workers-artifacts/word-count/*/worker.log
```

To kill the topology:
```bash
storm kill word-count \
  -c nimbus.seeds="[\"<nimbus-ip>\"]"
```

From **inside** the `nimbus` container (uses `/opt/storm/conf/storm.yaml`, no `-c` needed):

```bash
lxc exec nimbus -- /opt/storm/bin/storm kill word-count
```

---

## 11. Reset: clean state or remove everything

Pick one of the following depending on how far you want to roll back.

### 11a. Stop Storm and ZooKeeper (keep containers and installs)

```bash
lxc exec nimbus -- bash -c 'pkill -f org.apache.storm.daemon.nimbus.Nimbus 2>/dev/null || true; pkill -f Dlogfile.name=ui.log 2>/dev/null || true'
lxc exec supervisor -- bash -c 'pkill -f org.apache.storm.daemon.supervisor.Supervisor 2>/dev/null || true'
lxc exec zookeeper -- systemctl stop zookeeper-storm 2>/dev/null || true
```

### 11b. Wipe Storm and ZooKeeper **data** (keep OS, packages, configs)

Stops daemons, deletes local Storm working dirs and the ZooKeeper data dir, then restarts ZooKeeper empty. You must re-run **§6** if you change IPs, and restart daemons (**§7**). Topologies and cluster metadata in ZK are gone.

```bash
lxc exec nimbus -- bash -c 'pkill -f org.apache.storm.daemon.nimbus.Nimbus 2>/dev/null || true; pkill -f Dlogfile.name=ui.log 2>/dev/null || true'
lxc exec supervisor -- bash -c 'pkill -f org.apache.storm.daemon.supervisor.Supervisor 2>/dev/null || true'
lxc exec zookeeper -- systemctl stop zookeeper-storm 2>/dev/null || true

lxc exec nimbus -- bash -c 'rm -rf /var/storm/*'
lxc exec supervisor -- bash -c 'rm -rf /var/storm/*'
lxc exec zookeeper -- bash -c 'rm -rf /var/lib/zookeeper/*'

lxc exec zookeeper -- systemctl start zookeeper-storm
```

If port **2181** is still held by an old process after a failed upgrade: `lxc exec zookeeper -- fuser -k 2181/tcp` (only when no ZooKeeper instance should be running).

### 11c. Remove Storm and ZooKeeper installs (keep containers)

Handy if you want to retry **§3** and **§4** without recreating instances:

```bash
lxc exec zookeeper -- bash -c 'systemctl disable --now zookeeper-storm 2>/dev/null || true; rm -f /etc/systemd/system/zookeeper-storm.service; systemctl daemon-reload'
lxc exec zookeeper -- bash -c 'rm -rf /opt/zookeeper /opt/apache-zookeeper-* /var/lib/zookeeper /tmp/zk.tgz'

for NODE in nimbus supervisor zookeeper; do
  lxc exec "$NODE" -- bash -c 'rm -rf /opt/storm /opt/apache-storm-* /tmp/storm.tgz'
done
```

Then reinstall Java/Storm (**§3**), ZooKeeper (**§4**), `storm.yaml` (**§6**), and daemons (**§7**).

### 11d. Full teardown (delete the three lab containers)

This removes all data and software in those instances. Other LXD instances on your machine are unchanged.

```bash
lxc exec nimbus -- bash -c 'pkill -f org.apache.storm 2>/dev/null || true' 2>/dev/null || true
lxc exec supervisor -- bash -c 'pkill -f org.apache.storm 2>/dev/null || true' 2>/dev/null || true
lxc exec zookeeper -- systemctl stop zookeeper-storm 2>/dev/null || true

lxc stop nimbus supervisor zookeeper 2>/dev/null || true
lxc delete -f nimbus supervisor zookeeper
```

After that, start again from **§2** (launch containers). To remove the host copy of tarballs: `rm -f /tmp/apache-storm-2.8.7.tar.gz /tmp/zk.tgz` (paths may differ if you used **Option B** elsewhere).

---

## Quick reference: useful LXC commands

```bash
lxc list                        # show all containers + IPs
lxc exec nimbus -- bash         # shell into nimbus
lxc stop nimbus supervisor zookeeper   # stop all
lxc start nimbus supervisor zookeeper  # start all
lxc snapshot nimbus clean-state        # snapshot before experiments
```

---

The key insight in the distributed flow: the **Spout** runs on the supervisor's worker slots, emitting tuples → the **SplitBolt** fans them out across 2 parallel executors → **CountBolt** uses `fieldsGrouping` so the same word always routes to the same bolt instance (critical for correct counts in a distributed setting). Zookeeper is what lets Nimbus and the Supervisor maintain cluster state without a single point of failure.
