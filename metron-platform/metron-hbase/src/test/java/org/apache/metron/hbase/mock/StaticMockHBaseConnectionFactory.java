package org.apache.metron.hbase.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.hbase.client.HBaseConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StaticMockHBaseConnectionFactory extends HBaseConnectionFactory {

  /**
   * A set of {@link Table}s that will be returned by all {@link Connection}
   * objects created by this factory.
   */
  private static Map<TableName, Table> tables = new HashMap<>();

  /**
   * The {@link Connection} returned by this factory will return the given table by
   * name when {@link Connection#getTable(TableName)} is called.
   *
   * @param tableName The name of the table.
   * @param table The table.
   * @return
   */
  public static void withTable(String tableName, Table table) {
    tables.put(TableName.valueOf(tableName), table);
  }

  /**
   * The {@link Connection} returned by this factory will return a table by
   * name when {@link Connection#getTable(TableName)} is called.
   *
   * @param tableName The name of the table.
   * @return
   */
  public static void withTable(String tableName) {
    withTable(tableName, mock(Table.class));
  }

  public static Table getTable(String tableName) {
    return tables.get(TableName.valueOf(tableName));
  }

  @Override
  public Connection createConnection(Configuration configuration) throws IOException {
    Connection connection = mock(Connection.class);

    // the connection must return each of the given tables by name
    for(Map.Entry<TableName, Table> entry: tables.entrySet()) {
      TableName tableName = entry.getKey();
      Table table = entry.getValue();
      when(connection.getTable(eq(tableName))).thenReturn(table);
    }

    return connection;
  }
}
