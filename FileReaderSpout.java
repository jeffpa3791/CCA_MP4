
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

//change backtype to org.apache
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  /* added variables */
  private FileReader _fileReader;
  private BufferedReader _bufferedReader;
  private boolean _fileComplete = false;


  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader
    ------------------------------------------------- */
    /* implementation:
       accept file name as args[0] and open it
    */
    String file_name;
    file_name = args[0];

		try {
			this._fileReader = new FileReader(conf.get(file_name).toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+file_name+"]");
		}
		
    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop
    ------------------------------------------------- */
    // check if file is complete, sleep for 1 second and then return 
    if (this._fileComplete) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
    }
    
    // Open the reader
		this._bufferedReader = new BufferedReader(this._fileReader);
		try{
			//Read all lines
			while((str = _bufferedReader.readLine()) != null){
				this._collector.emit(new Values(str));
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			this._fileComplete = true;
		}


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file
    ------------------------------------------------- */
    this._bufferedReader.close();
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
