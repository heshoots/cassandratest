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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GettingStarted {

  public static void main(String[] args) {
    Cluster cluster;
    Session session;


    int NUM_ROWS;
    int NUM_THREADS;
    int NUM_STATIONS;
    String hostname;
    String replication;
    try {
      JSONParser parser = new JSONParser();
      JSONObject config = (JSONObject) parser.parse(new FileReader("./config.json"));
      NUM_ROWS = Integer.parseInt((String) config.get("rows"));
      NUM_THREADS = Integer.parseInt((String) config.get("threads"));
      NUM_STATIONS = Integer.parseInt((String) config.get("stations"));
      hostname = (String) config.get("hostname");
      replication = (String) config.get("replication");
    } catch (Exception e) {
      NUM_ROWS = 10000;
      NUM_THREADS = 4;
      NUM_STATIONS = 4;
      hostname = "cassandra";
      replication = "CREATE KEYSPACE dev with REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}";
      e.printStackTrace();
    }
    System.out.println("Num threads: " + NUM_THREADS);
    System.out.println("Rows per station: " + NUM_ROWS);
    System.out.println("Num stations: " + NUM_STATIONS);
    System.out.println("hostname : " + hostname);

    cluster = Cluster.builder().addContactPoint(hostname).build();
    session = cluster.connect();

    session.execute("DROP KEYSPACE IF EXISTS dev");
    session.execute(replication);
    session.execute("USE dev");
    session.execute("CREATE TABLE measures (station_id text, time timestamp, value decimal, PRIMARY KEY (station_id, time))");

    long start = System.nanoTime();
    ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
    DevicePerRow writer;
    Thread thread;
    String station = "station";
    for (int i = 0; i < NUM_STATIONS ; i++) {
      writer = new DevicePerRow(session, new ValueGenerator(NUM_ROWS), false, station + i);
      thread = new Thread(writer);
      pool.execute(thread);
    }
    pool.shutdown();
    try {
      pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }

    long end = System.nanoTime();
    long timetaken = (end-start)/1000000000;
    System.out.println(timetaken + " Seconds taken");
    System.out.println((NUM_ROWS * NUM_STATIONS)/timetaken + " Updates per second");
    cluster.close();
  }
}
