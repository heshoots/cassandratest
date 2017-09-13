import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;

public class GettingStarted {
  public static void main(String[] args) {
    Cluster cluster;
    Session session;

    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    session = cluster.connect("dev");
    session.execute("DROP TABLE if exists measures");

    int numberofrows = 1000;
    long start = System.nanoTime();
    new DevicePerRow(session, new ValueGenerator(numberofrows), true);
    long end = System.nanoTime();
    long timetaken = (end-start)/1000000000;
    System.out.println(timetaken + " Seconds taken");
    System.out.println(numberofrows/timetaken + " Updates per second");
    cluster.close();
  }
}
