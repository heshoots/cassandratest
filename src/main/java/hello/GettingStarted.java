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
import java.util.ArrayList;
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
    List<String> hostnames = new ArrayList<String>();
    String replication;
    try {
      JSONParser parser = new JSONParser();
      JSONObject config = (JSONObject) parser.parse(new FileReader("./config.json"));
      NUM_ROWS = Integer.parseInt((String) config.get("rows"));
      NUM_THREADS = Integer.parseInt((String) config.get("threads"));
      NUM_STATIONS = Integer.parseInt((String) config.get("stations"));
      JSONArray arr = (JSONArray) config.get("hostnames");
      for (int i = 0; i < arr.size(); i++) {
        hostnames.add((String) arr.get(i));
      }
      replication = (String) config.get("replication");
    } catch (Exception e) {
      NUM_ROWS = 10000;
      NUM_THREADS = 4;
      NUM_STATIONS = 4;
      hostnames.add("cassandra");
      replication = "CREATE KEYSPACE dev with REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}";
      e.printStackTrace();
    }
    System.out.println("Num threads: " + NUM_THREADS);
    System.out.println("Rows per station: " + NUM_ROWS);
    System.out.println("Num stations: " + NUM_STATIONS);

    Cluster.Builder builder = Cluster.builder();
    for (int i = 0; i < hostnames.size(); i++) {
      String hostname = hostnames.get(i);
      System.out.println("hostname : " + hostname);
      builder.addContactPoint(hostname);
    }
    cluster = builder.build();
    session = cluster.connect();

    session.execute("DROP KEYSPACE IF EXISTS dev");
    session.execute(replication);
    session.execute("USE dev");
    session.execute("CREATE TABLE measures (station_id text, time timestamp, value decimal, PRIMARY KEY (station_id, time))");
    session.execute("CREATE TABLE station_day (station_id text, date text, time timestamp, value decimal, PRIMARY KEY ((date, station_id), time))");
    session.execute("CREATE TABLE days (station_id text, date text, time timestamp, value decimal, PRIMARY KEY ((date), station_id, time))");

    long start = System.nanoTime();
    ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
    Batcher writer;
    Thread thread;
    String station = "station";
    PreparedStatement measures = session.prepare("INSERT INTO measures (station_id, time, value) values (?, ?, ?)");
    PreparedStatement stationDate = session.prepare("INSERT INTO station_day (station_id, date, time, value) values (?, ?, ?, ?)");
    PreparedStatement days = session.prepare("INSERT INTO days (station_id, date, time, value) values (?, ?, ?, ?)");
    for (int i = 0; i < NUM_STATIONS ; i++) {
      writer = new Batcher(session, new ValueGenerator(NUM_ROWS, station + i, measures));
      thread = new Thread(writer);
      pool.execute(thread);

      writer = new Batcher(session, new StationDateGenerator(NUM_ROWS, station + i, stationDate));
      thread = new Thread(writer);
      pool.execute(thread);

      writer = new Batcher(session, new StationDateGenerator(NUM_ROWS, station + i, days));
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
