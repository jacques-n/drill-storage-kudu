package org.apache.drill.hbase;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestKuduPlugin extends BaseTestQuery {

  @Test
  public void testBasicQuery() throws Exception {
    test("select * from sys.options;");
  }
}
