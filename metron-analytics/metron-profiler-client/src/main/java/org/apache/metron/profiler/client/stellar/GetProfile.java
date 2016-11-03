/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.metron.profiler.client.stellar;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.metron.common.dsl.Context;
import org.apache.metron.common.dsl.ParseException;
import org.apache.metron.common.dsl.Stellar;
import org.apache.metron.common.dsl.StellarFunction;
import org.apache.metron.common.utils.ConversionUtils;
import org.apache.metron.hbase.HTableProvider;
import org.apache.metron.hbase.TableProvider;
import org.apache.metron.profiler.client.HBaseProfilerClient;
import org.apache.metron.profiler.client.ProfilerClient;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.profiler.hbase.RowKeyBuilder;
import org.apache.metron.profiler.hbase.SaltyRowKeyBuilder;
import org.apache.metron.profiler.hbase.ValueOnlyColumnBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.metron.common.dsl.Context.Capabilities.GLOBAL_CONFIG;

/**
 * Stellar functions that can retrieve data contained within a Profile.
 *
 *  PROFILE_GET and PROFILE_GET_CONF
 *
 * Retrieve all values for 'entity1' from 'profile1' over the past 4 hours.
 *
 *   <code>PROFILE_GET('profile1', 'entity1', 4, 'HOURS')</code>
 *
 * Retrieve all values for 'entity1' from 'profile1' over the past 2 days.
 *
 *   <code>PROFILE_GET('profile1', 'entity1', 2, 'DAYS')</code>
 *
 * Retrieve all values for 'entity1' from 'profile1' that occurred on 'weekdays' over the past month.
 *
 *   <code>PROFILE_GET('profile1', 'entity1', 1, 'MONTHS', 'weekdays')</code>
 *
 * Retrieve all values for 'entity1' from 'profile1' over the past 2 days,
 * overriding the usual global client configuration parameters for window duration.
 *
 *   <code>PROFILE_GET_CONF('profile1', 'entity1', 2, 'DAYS', {'profiler.client.period.duration' : 2, 'profiler.client.period.duration.units' : 'MINUTES'})</code>
 *
 * Retrieve all values for 'entity1' from 'profile1' that occurred on 'weekdays' over the past month,
 * overriding the usual global client configuration parameters for window duration.
 *
 *   <code>PROFILE_GET_CONF('profile1', 'entity1', 1, 'MONTHS', {'profiler.client.period.duration' : 2, 'profiler.client.period.duration.units' : 'MINUTES'}, 'weekdays')</code>
 *
 */
@Stellar(
        namespace="PROFILE",
        name="GET",
        description="Retrieves a series of values from a stored profile.",
        params={
          "profile - The name of the profile.",
          "entity - The name of the entity.",
          "durationAgo - How long ago should values be retrieved from?",
          "units - The units of 'durationAgo'.",
          "groups - Optional - The groups used to sort the profile."
        },
        returns="The profile measurements."
)
public class GetProfile implements StellarFunction {

  /**
   * A global property that defines the name of the HBase table used to store profile data.
   */
  public static final String PROFILER_HBASE_TABLE = "profiler.client.hbase.table";

  /**
   * A global property that defines the name of the column family used to store profile data.
   */
  public static final String PROFILER_COLUMN_FAMILY = "profiler.client.hbase.column.family";

  /**
   * A global property that defines the name of the HBaseTableProvider implementation class.
   */
  public static final String PROFILER_HBASE_TABLE_PROVIDER = "hbase.provider.impl";

  /**
   * A global property that defines the duration of each profile period.  This value
   * should be defined along with 'profiler.client.period.duration.units'.
   */
  public static final String PROFILER_PERIOD = "profiler.client.period.duration";

  /**
   * A global property that defines the units of the profile period duration.  This value
   * should be defined along with 'profiler.client.period.duration'.
   */
  public static final String PROFILER_PERIOD_UNITS = "profiler.client.period.duration.units";

  /**
   * A global property that defines the salt divisor used to store profile data.
   */
  public static final String PROFILER_SALT_DIVISOR = "profiler.client.salt.divisor";

  /**
   * The default Profile period duration should none be defined in the global properties.
   */
  public static final String PROFILER_PERIOD_DEFAULT = "15";

  /**
   * The default units of the Profile period should none be defined in the global properties.
   */
  public static final String PROFILER_PERIOD_UNITS_DEFAULT = "MINUTES";

  /**
   * The default salt divisor should none be defined in the global properties.
   */
  public static final String PROFILER_SALT_DIVISOR_DEFAULT = "1000";

  private static final Logger LOG = LoggerFactory.getLogger(GetProfile.class);


  // Global configuration from client context, used to help create profiler client.
  private Map<String, Object> global;

  // Cached client that can retrieve profile values.
  private ProfilerClient client;

  // Cached value of config override map used to construct the previously cached client.
  private Map cachedConfigOverrideMap;

  /**
   * Initialization.
   */
  @Override
  public void initialize(Context context) {
    // ensure the required capabilities are defined
    Context.Capabilities[] required = { GLOBAL_CONFIG };
    validateCapabilities(context, required);
    global = (Map<String, Object>) context.getCapability(GLOBAL_CONFIG).get();
  }

  /**
   * Is the function initialized?
   */
  @Override
  public boolean isInitialized() {
    return global != null;
  }

  /**
   * Apply the function.
   * @param args The function arguments.
   * @param context
   */
  @Override
  public Object apply(List<Object> args, Context context) throws ParseException {

    return applyWithConf(args, null, context);
  }

  /**
   * Apply the function with override_config (see PROFILE_GET_CONF).
   * @param args The function arguments other than the config override map.
   * @param configOverrideMap - the config override map - may be null.
   * @param context
   */
  protected Object applyWithConf(List<Object> args, Map configOverrideMap, Context context) throws ParseException {

    String profile = getArg(0, String.class, args);
    String entity = getArg(1, String.class, args);
    long durationAgo = getArg(2, Long.class, args);
    String unitsName = getArg(3, String.class, args);
    TimeUnit units = TimeUnit.valueOf(unitsName);
    List<Object> groups = getGroupsArg(4, args);

    //create new profiler client if needed
    if (client == null || !cachedConfigOverrideMap.equals(configOverrideMap)) {
      Map<String, Object> configWithOverrides = processOverrides(global, configOverrideMap);
      RowKeyBuilder rowKeyBuilder = getRowKeyBuilder(configWithOverrides);
      ColumnBuilder columnBuilder = getColumnBuilder(configWithOverrides);
      HTableInterface table = getTable(configWithOverrides);
      client = new HBaseProfilerClient(table, rowKeyBuilder, columnBuilder);
      cachedConfigOverrideMap = configOverrideMap;
    }

    return client.fetch(Object.class, profile, entity, groups, durationAgo, units);
  }

  /**
   * Combine the configuration parameter override list with the config from global context,
   * and return the result.
   *
   * Only the six recognized profiler client config parameters may be overridden,
   * all other key-value pairs in the Map will be ignored.
   *
   * Type violations cause a Stellar ParseException.
   *
   * @param global - a context global config map.
   * @param configOverrideMap - Map as described above.
   * @return config map with overrides applied.
   * @throws ParseException - if any override values are of wrong type.
   */
  private Map<String, Object> processOverrides(
              Map<String, Object> global
              , Map configOverrideMap
  ) throws ParseException {

    if (configOverrideMap == null || configOverrideMap.isEmpty())
      return global;

    Map<String, Object> result = new HashMap<String, Object>(global);
    Object v;
    try {
      for (Object key : configOverrideMap.keySet()) {
        if (key.getClass() != String.class) {
          // Probably unintended user error, so throw an exception rather than ignore
          throw new ParseException("Non-string key in config_override map is not allowed: " + key.toString());
        }
        switch ((String) key) {
          case PROFILER_HBASE_TABLE:
          case PROFILER_COLUMN_FAMILY:
          case PROFILER_HBASE_TABLE_PROVIDER:
          case PROFILER_PERIOD_UNITS:
            v = configOverrideMap.get(key);
            if (v != null) result.put((String) key, (String) v);
            break;
          case PROFILER_PERIOD:
          case PROFILER_SALT_DIVISOR:
            v = configOverrideMap.get(key);
            if (v != null) {
              // be tolerant if the user put a number instead of a string
              // but validate that it is an integer value
              if (v instanceof String) {
                long vlong = Long.parseLong((String) v);
                result.put((String) key, String.valueOf(vlong));
              }
              else {
                result.put((String) key, String.valueOf(((Number) v).longValue()));
              }
            }
            break;
          default:
            LOG.warn("Ignoring unallowed key {} in config_override map.", key);
            break;
        }
      }
    } catch (ClassCastException | NumberFormatException cce) {
      throw new ParseException("Type violation in config_override map values: ", cce);
    }
    return result;
  }

  /**
   * Get the groups defined by the user.
   *
   * The user can specify 0 or more groups.  All arguments from the specified position
   * on are assumed to be groups.  If there is no argument in the specified position,
   * then it is assumed the user did not specify any groups.
   *
   * @param startIndex The starting index of groups within the function argument list.
   * @param args The function arguments.
   * @return The groups.
   */
  private List<Object> getGroupsArg(int startIndex, List<Object> args) {
    List<Object> groups = new ArrayList<>();

    for(int i=startIndex; i<args.size(); i++) {
      String group = getArg(i, String.class, args);
      groups.add(group);
    }

    return groups;
  }

  /**
   * Ensure that the required capabilities are defined.
   * @param context The context to validate.
   * @param required The required capabilities.
   * @throws IllegalStateException if all of the required capabilities are not present in the Context.
   */
  private void validateCapabilities(Context context, Context.Capabilities[] required) throws IllegalStateException {

    // collect the name of each missing capability
    String missing = Stream
            .of(required)
            .filter(c -> !context.getCapability(c).isPresent())
            .map(c -> c.toString())
            .collect(Collectors.joining(", "));

    if(StringUtils.isNotBlank(missing) || context == null) {
      throw new IllegalStateException("missing required context: " + missing);
    }
  }

  /**
   * Get an argument from a list of arguments.
   * @param index The index within the list of arguments.
   * @param clazz The type expected.
   * @param args All of the arguments.
   * @param <T> The type of the argument expected.
   */
  protected <T> T getArg(int index, Class<T> clazz, List<Object> args) {
    if(index >= args.size()) {
      throw new IllegalArgumentException(format("expected at least %d argument(s), found %d", index+1, args.size()));
    }

    return ConversionUtils.convert(args.get(index), clazz);
  }

  /**
   * Creates the ColumnBuilder to use in accessing the profile data.
   * @param global The global configuration.
   */
  private ColumnBuilder getColumnBuilder(Map<String, Object> global) {
    // the builder is not currently configurable - but should be made so
    ColumnBuilder columnBuilder;

    if(global.containsKey(PROFILER_COLUMN_FAMILY)) {
      String columnFamily = (String) global.get(PROFILER_COLUMN_FAMILY);
      columnBuilder = new ValueOnlyColumnBuilder(columnFamily);

    } else {
      columnBuilder = new ValueOnlyColumnBuilder();
    }

    return columnBuilder;
  }

  /**
   * Creates the ColumnBuilder to use in accessing the profile data.
   * @param global The global configuration.
   */
  private RowKeyBuilder getRowKeyBuilder(Map<String, Object> global) {

    // how long is the profile period?
    String configuredDuration = (String) global.getOrDefault(PROFILER_PERIOD, PROFILER_PERIOD_DEFAULT);
    long duration = Long.parseLong(configuredDuration);
    LOG.debug("profiler client: {}={}", PROFILER_PERIOD, duration);

    // which units are used to define the profile period?
    String configuredUnits = (String) global.getOrDefault(PROFILER_PERIOD_UNITS, PROFILER_PERIOD_UNITS_DEFAULT);
    TimeUnit units = TimeUnit.valueOf(configuredUnits);
    LOG.debug("profiler client: {}={}", PROFILER_PERIOD_UNITS, units);

    // what is the salt divisor?
    String configuredSaltDivisor = (String) global.getOrDefault(PROFILER_SALT_DIVISOR, PROFILER_SALT_DIVISOR_DEFAULT);
    int saltDivisor = Integer.parseInt(configuredSaltDivisor);
    LOG.debug("profiler client: {}={}", PROFILER_SALT_DIVISOR, saltDivisor);

    return new SaltyRowKeyBuilder(saltDivisor, duration, units);
  }

  /**
   * Create an HBase table used when accessing HBase.
   * @param global The global configuration.
   * @return
   */
  private HTableInterface getTable(Map<String, Object> global) {

    String tableName = (String) global.getOrDefault(PROFILER_HBASE_TABLE, "profiler");
    TableProvider provider = getTableProvider(global);

    try {
      return provider.getTable(HBaseConfiguration.create(), tableName);

    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Unable to access table: %s", tableName));
    }
  }

  /**
   * Create the TableProvider to use when accessing HBase.
   * @param global The global configuration.
   */
  private TableProvider getTableProvider(Map<String, Object> global) {
    String clazzName = (String) global.getOrDefault(PROFILER_HBASE_TABLE_PROVIDER, HTableProvider.class.getName());

    TableProvider provider;
    try {
      Class<? extends TableProvider> clazz = (Class<? extends TableProvider>) Class.forName(clazzName);
      provider = clazz.newInstance();

    } catch (Exception e) {
      provider = new HTableProvider();
    }

    return provider;
  }
}
