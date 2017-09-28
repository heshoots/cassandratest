import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.util.Random;
import java.util.Date;
import java.text.DateFormat;
import java.math.BigDecimal;


public class DevicePerRow implements Runnable{
  int numperbatch = 100;
  Session session;
  QueryObjectGenerator val;
  Boolean log;
  String station;

  public static String formatTime(String datetime) {
    String[] dateTimeSplit = datetime.split(" ");
    String time = dateTimeSplit[1];
    String[] dateSplit = dateTimeSplit[0].split("/");
    String date = dateSplit[2] + "-" + dateSplit[1] + "-" + dateSplit[0];
    return date + " " + time + "+0000";
  }

  DevicePerRow(Session session, QueryObjectGenerator val, Boolean log, String station) {
    this.session = session;
    this.val = val;
    this.log = log;
    this.station = station;
  }

  @Override
  public void run() {
    Random rand = new Random();
    String line = "";
    int numlines = 0;
    PreparedStatement prepared = session.prepare("INSERT INTO measures (station_id, time, value) values (?, ?, ?)");
    try {
      while (!val.done()) {
        BatchStatement batch = new BatchStatement();
        numlines += numperbatch;
        for (int i = 0; i < numperbatch; i++) {
          Object[] testval = this.val.getRow();
          if (log) {
            System.out.println(testval[0] + " " + testval[1] + " " + this.station);
          }
          batch.add(prepared.bind(this.station, (Date) testval[0], new BigDecimal(new Double(testval[1].toString()))));
        }
        this.session.execute(batch);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(numlines + " records written");
  }
}
