import com.datastax.driver.core.*;
public interface QueryObjectGenerator {
  Boolean done();
  Object[] getRow();
  BoundStatement getStatement();
}
