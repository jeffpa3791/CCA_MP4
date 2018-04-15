import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;
  private int topNthreshold = 0;

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words
    ------------------------------------------------- */
    String word = tuple.getString(0);
    Integer count = tuple.getInteger(1);
    
    // keep track of the top-N-threshold = the lowest count for the top N words on the Hashmap
    // if a word comes in with a count >= threshold, it might be qualified for inclusion in the top, so take action:  
    //   put() will insert or update the entry
    //   if we now have more than N items, trim it to the top N and update the threshold
    
    if (count >= topNthreshold) {
      currentTopWords.put(word,count);
      if (currentTopWords.size() > N) {
        // put the counts on a list
        List<Integer> countList = new ArrayList<Integer>();
        countList.addAll(currentTopWords.values());
        //sort the list
        Collections.sort(countList);
        //new threshold is the Nth entry
        topNthreshold = countList.get(N);
        //any item on currentTopWords with count < threhsold should be removed
        //   in case of ties, this may retain more than N elements. 
        for (String keyword : currentTopWords.keySet()) {
          if (currentTopWords.get(keyword) < topNthreshold) {
            System.err.println("DEBUG: threshold is " + topNthreshold + 
               ", removing word " + keyword + " with count " + currentTopWords.get(keyword));
            currentTopWords.remove(keyword);
            System.err.println("DEBUG: size is now " + currentTopWords.size());
          } // end of if (currentTopWords.get(word) < topNthreshold)
        } // end of for (String word : currentTopWords.keySet())
      }  // end of if (currentTopWords.size() > N) 
    }  // end of if (count >= topNthreshold) 

    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
     declarer.declare(new Fields("top-N"));
  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}
