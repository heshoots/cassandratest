import com.datastax.driver.core.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.json.simple.parser.ParseException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.List;

public class GettingStarted {
  public static void main(String[] args) {
    Cluster cluster;
    Session session;

    cluster = Cluster.builder().addContactPoint("cassandra").build();
    session = cluster.connect();

    session.execute("DROP KEYSPACE IF EXISTS dev");

    try {
      JSONParser parser = new JSONParser();
      JSONObject config = (JSONObject) parser.parse(new FileReader("./config.json"));
      session.execute((String) config.get("replication"));
      session.execute("USE dev");

      int numberofrows = Integer.parseInt((String) config.get("rows"));
      long start = System.nanoTime();
      new DevicePerRow(session, new ValueGenerator(numberofrows), true);
      long end = System.nanoTime();
      long timetaken = (end-start)/1000000000;
      System.out.println(timetaken + " Seconds taken");
      System.out.println(numberofrows/timetaken + " Updates per second");
      cluster.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
