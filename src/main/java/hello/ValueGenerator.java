import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.util.Random;
import java.lang.Math;
import com.datastax.driver.core.*;
import java.util.Date;
import java.text.DateFormat;
import java.math.BigDecimal;

class ValueGenerator implements QueryObjectGenerator {
  private Boolean sequential;
  private DateTime start;
  private DateTime end;
  private DateTime current;
  private Random rand;
  private int output;
  private String station;
  private PreparedStatement statement;

  ValueGenerator(int numberofrows, String station, PreparedStatement statement) {
    this.sequential = true;
    this.station = station;
    this.start = new DateTime(1960, 2, 16, 9, 0, 0, DateTimeZone.UTC);
    this.current = new DateTime(1960, 2, 16, 9, 0, 0, DateTimeZone.UTC);
    this.end = this.start.plusHours(numberofrows);
    this.rand = new Random();
    this.output = 0;
    this.statement = statement;
  }

  public Boolean done() {
    return this.current.compareTo(this.end) >= 0;
  }

  public Object[] getRow() {
    Object[] out = new Object[2];
    out[0] = this.current.toDate();
    this.current = this.current.plusHours(1);
    out[1] = rand.nextFloat() * 200.0;
    return out;
  }

  public BoundStatement getStatement() {
    Date date = this.current.toDate();
    this.current = this.current.plusHours(1);
    Float value = rand.nextFloat() * 200;
    return statement.bind(this.station, date, new BigDecimal(value));
  }

  String[] getRandomRow() {
    String[] out = new String[2];
    out[0] = new DateTime(Math.abs(rand.nextLong())).toString("yyyy-MM-dd HH:mm:ssZ");
    out[1] = String.format("%.2f", rand.nextFloat() * 200.0);
    return out;
  }
}
