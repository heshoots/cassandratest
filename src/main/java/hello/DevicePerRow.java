import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.util.Random;

public class DevicePerRow {
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

  DevicePerRow(Session session, QueryObjectGenerator val, Boolean log) {
    Random rand = new Random();
    String line = "";
    int numlines = 0;
    session.execute("CREATE TABLE measures (station_id text, time timestamp, value decimal, PRIMARY KEY (station_id, time))");
    try {
      while (!val.done()) {
        numlines++;
        String[] testval = val.getRow();
        String station = getStation((int) (rand.nextFloat() * 4));
        if (log) {
          System.out.println(testval[0] + " " + testval[1] + " " + station);
        }
        session.execute("INSERT INTO measures (station_id, time, value) VALUES ('" + station + "', '" + testval[0] + "', " + testval[1] + ")");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(numlines + " records written");
  }
}
