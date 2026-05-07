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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WordCountTopology {

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
            Utils.sleep(500);
            String sentence = sentences[random.nextInt(sentences.length)];
            collector.emit(new Values(sentence));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

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

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentence-spout", new SentenceSpout(), 1);

        builder.setBolt("split-bolt", new SplitBolt(), 2)
               .shuffleGrouping("sentence-spout");

        builder.setBolt("count-bolt", new CountBolt(), 2)
               .fieldsGrouping("split-bolt", new Fields("word"));

        Config config = new Config();
        config.setNumWorkers(2);
        config.setMessageTimeoutSecs(30);

        StormSubmitter.submitTopologyWithProgressBar("word-count", config, builder.createTopology());
    }
}
