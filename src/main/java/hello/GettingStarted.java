import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;

public class GettingStarted {

  public static String formatTime(String datetime) {
    String[] dateTimeSplit = datetime.split(" ");
    String time = dateTimeSplit[1];
    String[] dateSplit = dateTimeSplit[0].split("/");
    String date = dateSplit[2] + "-" + dateSplit[1] + "-" + dateSplit[0];
    return date + " " + time + "+0000";
  }

  public static void main(String[] args) {
    Cluster cluster;
    Session session;

    String csv = "../hour.csv";
    BufferedReader br = null;
    String line = "";

    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    session = cluster.connect("dev");

    session.execute("CREATE TABLE measures (station_id text, time timestamp, value decimal, PRIMARY KEY (station_id, time))");

    try {
      br = new BufferedReader(new FileReader(csv));
      while ((line = br.readLine()) != null) {
        String[] values = line.split(",");
        String date = values[0];
        String value = values[1];
        System.out.println("" + date + " " + value);
        session.execute("INSERT INTO measures (station_id, time, value) VALUES ('SS90F011', '" + formatTime(date) + "', " + value + ")");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    // Connect to the cluster and keyspace "demo"

    /* // Use select to get the user we just entered
    ResultSet results = session.execute("SELECT * FROM measures");
    for (Row row : results) {
      System.out.format("%s %f\n", row.getTimestamp(1), row.getDecimal("value"));
    }
    // Clean up the connection by closing it */

    cluster.close();
  }
}
