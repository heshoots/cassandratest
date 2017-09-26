import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.util.Random;


public class DevicePerRow implements Runnable{
  int numperbatch = 1000;
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

  public static String getStation(int rand) {
    String out;
    switch(rand) {
      case 0: out = "SS90F011";
              break;
      case 1: out = "TESTSTAT";
              break;
      case 2: out = "lookatest";
              break;
      case 4: out = "itsatest";
              break;
      default: out = "blah";
               break;
    }
    return out;
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
    try {
      while (!val.done()) {
        BatchStatement batch = new BatchStatement();
        numlines += numperbatch;
        for (int i = 0; i < numperbatch; i++) {
          String[] testval = this.val.getRow();
          if (log) {
            System.out.println(testval[0] + " " + testval[1] + " " + this.station);
          }
          batch.add(new SimpleStatement("INSERT INTO measures (station_id, time, value) VALUES ('" + this.station + "', '" + testval[0] + "', " + testval[1] + "); "));
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
