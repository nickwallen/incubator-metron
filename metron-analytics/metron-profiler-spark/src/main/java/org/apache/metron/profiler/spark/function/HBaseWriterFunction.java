package org.apache.metron.profiler.spark.function;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.metron.common.utils.ReflectionUtils;
import org.apache.metron.hbase.HTableProvider;
import org.apache.metron.hbase.TableProvider;
import org.apache.metron.hbase.client.HBaseClient;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.profiler.hbase.RowKeyBuilder;
import org.apache.metron.profiler.hbase.SaltyRowKeyBuilder;
import org.apache.metron.profiler.hbase.ValueOnlyColumnBuilder;
import org.apache.metron.profiler.spark.ProfileMeasurementAdapter;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_COLUMN_FAMILY;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_SALT_DIVISOR;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_TABLE_NAME;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_WRITE_DURABILITY;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;

/**
 * Writes the profile measurements to HBase in Spark.
 */
public class HBaseWriterFunction implements MapPartitionsFunction<ProfileMeasurementAdapter, Integer> {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private TableProvider tableProvider;

  /**
   * The name of the HBase table to write to.
   */
  private String tableName;

  /**
   * The durability guarantee when writing to HBase.
   */
  private Durability durability;

  /**
   * Builds the HBase row key.
   */
  private RowKeyBuilder rowKeyBuilder;

  /**
   * Assembles the columns for HBase.
   */
  private ColumnBuilder columnBuilder;

  public HBaseWriterFunction(Properties properties) {
    tableName = HBASE_TABLE_NAME.get(properties, String.class);
    durability = HBASE_WRITE_DURABILITY.get(properties, Durability.class);
    rowKeyBuilder = new SaltyRowKeyBuilder(
            HBASE_SALT_DIVISOR.get(properties, Integer.class),
            PERIOD_DURATION.get(properties, Integer.class),
            TimeUnit.valueOf(PERIOD_DURATION_UNITS.get(properties, String.class)));
    columnBuilder = new ValueOnlyColumnBuilder(
            HBASE_COLUMN_FAMILY.get(properties, String.class));
    tableProvider = new HTableProvider();
  }

  /**
   * Writes a set of measurements to HBase.
   *
   * @param iterator The measurements to write.
   * @return The number of measurements written to HBase.
   */
  @Override
  public Iterator<Integer> call(Iterator<ProfileMeasurementAdapter> iterator) throws Exception {
    LOG.debug("About to write profile measurement(s) to HBase");

    // open an HBase connection
    Configuration config = HBaseConfiguration.create();
    try(HBaseClient client = new HBaseClient(tableProvider, config, tableName)) {

      while(iterator.hasNext()) {
        ProfileMeasurement m = iterator.next().toProfileMeasurement();
        client.addMutation(rowKeyBuilder.rowKey(m), columnBuilder.columns(m), durability);
      }

      int count = client.mutate();
      LOG.debug("{} profile measurement(s) written to HBase", count);
      return IteratorUtils.singletonIterator(count);

    } catch(IOException e) {
      LOG.error("Unable to open connection to HBase", e);
      throw new RuntimeException(e);
    }
  }

  public HBaseWriterFunction withTableProvider(TableProvider tableProvider) {
    this.tableProvider = tableProvider;
    return this;
  }
}
