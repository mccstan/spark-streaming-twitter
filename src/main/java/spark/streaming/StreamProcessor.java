package spark.streaming;

/**
 * Created by mccstan on 02/05/17.
 */

import com.google.gson.Gson;
import org.apache.spark.streaming.dstream.DStream;
import java.util.Date;


public class StreamProcessor {
    public void process(){
        Gson gson = new Gson();
        gson.toJson(new Long(10));
    }

}
