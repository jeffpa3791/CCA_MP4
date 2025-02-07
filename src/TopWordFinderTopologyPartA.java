
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * This topology counts the words from sentences emmited from a random sentence spout.
 */
public class TopWordFinderTopologyPartA {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    Config config = new Config();
    config.setDebug(true);


    /*
    ----------------------TODO-----------------------
    Task: wire up the topology

    NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
    you use the following names for each component:

    RandomSentanceSpout -> "spout"
    SplitSentenceBolt -> "split"
    WordCountBolt -> "count"


    ------------------------------------------------- */
    


    
    /* implementation -- copied from tutorial , did not require any changes */
    builder.setSpout("spout", new RandomSentenceSpout(), 5);
    builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));

    




    config.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());

    //wait for 60 seconds and then kill the topology
    Thread.sleep(60 * 1000);

    cluster.shutdown();
  }
}
