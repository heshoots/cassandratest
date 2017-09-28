import com.datastax.driver.core.*;
import java.io.BufferedReader;

public class RowPerStationDay {
  public static String formatTime(String datetime) {
    String[] dateTimeSplit = datetime.split(" ");
    String time = dateTimeSplit[1];
    String[] dateSplit = dateTimeSplit[0].split("/");
    String date = dateSplit[2] + "-" + dateSplit[1] + "-" + dateSplit[0];
    return date + " " + time + "+0000";
  }

  RowPerStationDay(Session session, QueryObjectGenerator val, Boolean log) {
    int numlines = 0;
    session.execute("CREATE TABLE measures (station_id text, date text, time timestamp, value decimal, PRIMARY KEY ((station_id, date), time))");
    try {
      while (!val.done()) {
        numlines++;
        Object[] testval = val.getRow();
        if (log) {
          System.out.println(testval[0] + " " + testval[1]);
        }
       // session.execute("INSERT INTO measures (station_id, date, time, value) VALUES ('SS90F011', '" + testval[0].split(" ")[0] + "' , '" + testval[0] + "', " + testval[1] + ")");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(numlines + " records written");
  }
}
