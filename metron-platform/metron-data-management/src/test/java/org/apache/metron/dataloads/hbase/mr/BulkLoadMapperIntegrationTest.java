/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.dataloads.hbase.mr;

import com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.metron.dataloads.bulk.ThreatIntelBulkLoader;
import org.apache.metron.enrichment.converter.EnrichmentConverter;
import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;
import org.apache.metron.enrichment.lookup.LookupKV;
import org.apache.metron.test.utils.UnitTestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class BulkLoadMapperIntegrationTest {
  /** The test util. */
  private HBaseTestingUtility testUtil;

  /** The test table. */
  private HTable testTable;
  private String tableName = "malicious_domains";
  private String cf = "cf";
  private String csvFile="input.csv";
  private String extractorJson = "extractor.json";
  private String enrichmentJson = "enrichment_config.json";
  private String asOf = "04/15/2016";
  private String asOfFormat = "georgia";
  private String convertClass = "threadIntel.class";
  private Configuration config = null;


  @Before
  public void setup() throws Exception {
    UnitTestHelper.setJavaLoggingLevel(Level.SEVERE);
    Map.Entry<HBaseTestingUtility, Configuration> kv = HBaseUtil.INSTANCE.create(true);
    config = kv.getValue();
    testUtil = kv.getKey();
    testTable = testUtil.createTable(Bytes.toBytes(tableName), Bytes.toBytes(cf));
  }

  @After
  public void teardown() throws Exception {
    HBaseUtil.INSTANCE.teardown(testUtil);
  }
 /**
         {
            "config" : {
                        "columns" : {
                                "host" : 0
                                ,"meta" : 2
                                    }
                       ,"indicator_column" : "host"
                       ,"separator" : ","
                       ,"type" : "threat"
                       }
            ,"extractor" : "CSV"
         }
         */
  @Multiline
  private static String extractorConfig;

  @Test
  public void testCommandLine() throws Exception {
    UnitTestHelper.setJavaLoggingLevel(GuiceComponentProviderFactory.class, Level.WARNING);
    Configuration conf = HBaseConfiguration.create();

    String[] argv = {"-f cf", "-t malicious_domains", "-e extractor.json", "-n enrichment_config.json", "-a 04/15/2016", "-i input.csv", "-z georgia", "-c threadIntel.class"};
    String[] otherArgs = new GenericOptionsParser(conf, argv).getRemainingArgs();

    CommandLine cli = ThreatIntelBulkLoader.BulkLoadOptions.parse(new PosixParser(), otherArgs);
    Assert.assertEquals(extractorJson,ThreatIntelBulkLoader.BulkLoadOptions.EXTRACTOR_CONFIG.get(cli).trim());
    Assert.assertEquals(cf, ThreatIntelBulkLoader.BulkLoadOptions.COLUMN_FAMILY.get(cli).trim());
    Assert.assertEquals(tableName,ThreatIntelBulkLoader.BulkLoadOptions.TABLE.get(cli).trim());
    Assert.assertEquals(enrichmentJson,ThreatIntelBulkLoader.BulkLoadOptions.ENRICHMENT_CONFIG.get(cli).trim());
    Assert.assertEquals(csvFile,ThreatIntelBulkLoader.BulkLoadOptions.INPUT_DATA.get(cli).trim());
    Assert.assertEquals(asOf, ThreatIntelBulkLoader.BulkLoadOptions.AS_OF_TIME.get(cli).trim());
    Assert.assertEquals(asOfFormat, ThreatIntelBulkLoader.BulkLoadOptions.AS_OF_TIME_FORMAT.get(cli).trim());
    Assert.assertEquals(convertClass, ThreatIntelBulkLoader.BulkLoadOptions.CONVERTER.get(cli).trim());
  }

  @Test
  public void test() throws IOException, ClassNotFoundException, InterruptedException {

    Assert.assertNotNull(testTable);
    FileSystem fs = FileSystem.get(config);
    String contents = "google.com,1,foo";
    EnrichmentConverter converter = new EnrichmentConverter();
    HBaseUtil.INSTANCE.writeFile(contents, new Path("input.csv"), fs);
    Job job = ThreatIntelBulkLoader.createJob(config, "input.csv", tableName, cf, extractorConfig, 0L, new EnrichmentConverter());
    Assert.assertTrue(job.waitForCompletion(true));
    ResultScanner scanner = testTable.getScanner(Bytes.toBytes(cf));
    List<LookupKV<EnrichmentKey, EnrichmentValue>> results = new ArrayList<>();
    for(Result r : scanner) {
      results.add(converter.fromResult(r, cf));
    }
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(results.get(0).getKey().indicator, "google.com");
    Assert.assertEquals(results.get(0).getKey().type, "threat");
    Assert.assertEquals(results.get(0).getValue().getMetadata().size(), 2);
    Assert.assertEquals(results.get(0).getValue().getMetadata().get("meta"), "foo");
    Assert.assertEquals(results.get(0).getValue().getMetadata().get("host"), "google.com");
  }
}
