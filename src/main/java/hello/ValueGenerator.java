import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.util.Random;
import java.lang.Math;

class ValueGenerator implements QueryObjectGenerator {
  private Boolean sequential;
  private DateTime start;
  private DateTime end;
  private DateTime current;
  private Random rand;
  private int output;

  ValueGenerator(int numberofrows) {
    this.sequential = true;
    this.start = new DateTime(1960, 2, 16, 9, 0, 0, DateTimeZone.UTC);
    this.current = new DateTime(1960, 2, 16, 9, 0, 0, DateTimeZone.UTC);
    this.end = this.start.plusHours(numberofrows);
    this.rand = new Random();
    this.output = 0;
  }

  public Boolean done() {
    return this.current.compareTo(this.end) >= 0;
  }

  public String[] getRow() {
    String[] out = new String[2];
    out[0] = this.current.toString("yyyy-MM-dd HH:mm:ssZ");
    this.current = this.current.plusHours(1);
    out[1] = String.format("%.2f", rand.nextFloat() * 200.0);
    return out;
  }

  String[] getRandomRow() {
    String[] out = new String[2];
    out[0] = new DateTime(Math.abs(rand.nextLong())).toString("yyyy-MM-dd HH:mm:ssZ");
    out[1] = String.format("%.2f", rand.nextFloat() * 200.0);
    return out;
  }
}
