import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.util.Random;
import java.lang.Math;

class RandomDateGenerator implements QueryObjectGenerator {
  private Boolean sequential;
  private DateTime start;
  private DateTime end;
  private DateTime current;
  private Random rand;
  private int output;
  private int limit;

  RandomDateGenerator(int limit) {
    this.output = 0;
    this.limit = limit;
    this.rand = new Random();
  }

  public Boolean done() {
    return output >= limit;
  }

  public String[] getRow() {
    String[] out = new String[2];
    out[0] = new DateTime(Math.abs(rand.nextLong())).toString("yyyy-MM-dd HH:mm:ssZ");
    out[1] = String.format("%.2f", rand.nextFloat() * 200.0);
    output++;
    return out;
  }
}
