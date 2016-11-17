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

package org.apache.metron.common.stellar;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.metron.common.dsl.Context;
import org.apache.metron.common.dsl.ParseException;
import org.apache.metron.common.dsl.StellarFunctions;
import org.apache.metron.common.math.stats.OnlineStatisticsProviderTest;
import org.apache.metron.common.math.stats.StatisticsProvider;
import org.apache.metron.common.utils.ConversionUtils;
import org.apache.metron.common.utils.SerDeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static java.lang.String.format;

/**
 * Tests the statistical summary functions of Stellar.
 */
@RunWith(Parameterized.class)
public class StellarStatisticsFunctionsTest {

  private List<Double> values;
  private Map<String, Object> variables;
  private DescriptiveStatistics stats;
  private SummaryStatistics summaryStats;
  private int windowSize;

  public StellarStatisticsFunctionsTest(int windowSize) {
    this.windowSize = windowSize;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // each test will be run against these values for windowSize
    return Arrays.asList(new Object[][] {{ 0 }, { 100 }});
  }

  private static void tolerantAssertEquals( Function<StatisticsProvider, Number> func
                                          , StatisticsProvider left
                                          , StatisticsProvider right
                                          )

  {
    tolerantAssertEquals(func, left, right, null);
  }

  private static void tolerantAssertEquals( Function<StatisticsProvider, Number> func
                                          , StatisticsProvider left
                                          , StatisticsProvider right
                                          , Double epsilon
                                          )
  {
    try {
      Number leftVal = func.apply(left);
      Number rightVal = func.apply(left);
      if(epsilon != null) {
        Assert.assertEquals((double)leftVal, (double)rightVal, epsilon);
      }
      else {
        Assert.assertEquals(leftVal, rightVal);
      }
    }
    catch(UnsupportedOperationException uoe) {
      //ignore
    }

  }

  /**
   * Runs a Stellar expression.
   * @param expr The expression to run.
   * @param variables The variables available to the expression.
   */
  private static Object run(String expr, Map<String, Object> variables) {
    StellarProcessor processor = new StellarProcessor();
    Object ret = processor.parse(expr, x-> variables.get(x), StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
    byte[] raw = SerDeUtils.toBytes(ret);
    Object actual = SerDeUtils.fromBytes(raw, Object.class);
    if(ret instanceof StatisticsProvider) {
      StatisticsProvider left = (StatisticsProvider)ret;
      StatisticsProvider right = (StatisticsProvider)actual;
      //N
      tolerantAssertEquals(prov -> prov.getCount(), left, right);
      //sum
      tolerantAssertEquals(prov -> prov.getSum(), left, right, 1e-3);
      //sum of squares
      tolerantAssertEquals(prov -> prov.getSumSquares(), left, right, 1e-3);
      //sum of squares
      tolerantAssertEquals(prov -> prov.getSumLogs(), left, right, 1e-3);
      //Mean
      tolerantAssertEquals(prov -> prov.getMean(), left, right, 1e-3);
      //Quadratic Mean
      tolerantAssertEquals(prov -> prov.getQuadraticMean(), left, right, 1e-3);
      //SD
      tolerantAssertEquals(prov -> prov.getStandardDeviation(), left, right, 1e-3);
      //Variance
      tolerantAssertEquals(prov -> prov.getVariance(), left, right, 1e-3);
      //Min
      tolerantAssertEquals(prov -> prov.getMin(), left, right, 1e-3);
      //Max
      tolerantAssertEquals(prov -> prov.getMax(), left, right, 1e-3);
      //Kurtosis
      tolerantAssertEquals(prov -> prov.getKurtosis(), left, right, 1e-3);
      //Skewness
      tolerantAssertEquals(prov -> prov.getSkewness(), left, right, 1e-3);
      for (double d = 10.0; d < 100.0; d += 10) {
        final double pctile = d;
        //This is a sketch, so we're a bit more forgiving here in our choice of \epsilon.
        tolerantAssertEquals(prov -> prov.getPercentile(pctile), left, right, 1e-2);

      }
    }
    return ret;
  }

  @Before
  public void setup() {
    variables = new HashMap<>();

    // test input data
    values = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0);

    // the DescriptiveStatistics is used for validation
    stats = new DescriptiveStatistics(1000);
    values.stream().forEach(val -> stats.addValue(val));

    // the StatisticalSummary is used for validation
    summaryStats = new SummaryStatistics();
    values.stream().forEach(val -> summaryStats.addValue(val));
  }

  private void statsInit(int windowSize) {

    // initialize
    Object result = run("STATS_INIT(" + windowSize + ")", variables);
    assertNotNull(result);
    variables.put("stats", result);

    // add some values
    values.stream().forEach(val -> run(format("STATS_ADD (stats, %f)", val), variables));
  }

  @Test(expected=ParseException.class)
  public void testOverflow() throws Exception {
   run(format("STATS_ADD(STATS_INIT(), %f)", (Double.MAX_VALUE + 1)), new HashMap<>());
  }

  @Test
  public void ensureDeterminism() throws Exception {
    for(int i = 0;i < 20;++i) {
      testMergeProviders();
    }
  }

  @Test
  public void testMergeProviders() throws Exception {
    List<StatisticsProvider> providers = new ArrayList<>();
    /*
    Create 10 providers, each with a sample drawn from a gaussian distribution.
    Update the reference stats from commons math to ensure we are
     */
    GaussianRandomGenerator gaussian = new GaussianRandomGenerator(new MersenneTwister(1L));
    SummaryStatistics sStatistics = new SummaryStatistics();
    DescriptiveStatistics dStatistics = new DescriptiveStatistics();

    for(int i = 0;i < 10;++i) {
      List<Double> sample = new ArrayList<>();
      for(int j = 0;j < 100;++j) {
        double s = gaussian.nextNormalizedDouble();
        sample.add(s);
        sStatistics.addValue(s);
        dStatistics.addValue(s);
      }

      String expression = "STATS_ADD(STATS_INIT(), " + Joiner.on(",").join(sample) + ")";
      StatisticsProvider provider = (StatisticsProvider)run(expression, new HashMap<>());
      providers.add(provider);
    }

    /*
    Merge the providers and validate
     */
    Map<String, Object> providerVariables = new HashMap<>();
    for(int i = 0;i < providers.size();++i) {
      providerVariables.put("provider_" + i, providers.get(i));
    }

    String expression = "STATS_MERGE([" + Joiner.on(",").join(providerVariables.keySet()) + "])";
    StatisticsProvider mergedProvider = (StatisticsProvider)run(expression, providerVariables);
    OnlineStatisticsProviderTest.validateStatisticsProvider(mergedProvider, sStatistics , dStatistics);
  }

  @Test
  public void testAddManyIntegers() throws Exception {
    statsInit(windowSize);
    double countAtStart = (double) run("STATS_COUNT(stats)", variables);

    run("STATS_ADD(stats, 10, 20, 30, 40, 50)", variables);

    Object actual = run("STATS_COUNT(stats)", variables);
    assertEquals(countAtStart + 5, actual);
  }

  @Test
  public void testAddManyFloats() throws Exception {
    statsInit(windowSize);
    double countAtStart = (double) run("STATS_COUNT(stats)", variables);

    run("STATS_ADD(stats, 10.0, 20.0, 30.0, 40.0, 50.0)", variables);

    Object actual = run("STATS_COUNT(stats)", variables);
    assertEquals(countAtStart + 5, actual);
  }

  @Test
  public void testCount() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_COUNT(stats)", variables);
    assertEquals((long)stats.getN(), (long)ConversionUtils.convert(actual,Long.class));
  }

  @Test
  public void testMean() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_MEAN(stats)", variables);
    assertEquals(stats.getMean(), (Double) actual, 0.1);
  }

  @Test
  public void testGeometricMean() throws Exception {
    if(windowSize > 0) {
      statsInit(windowSize);
      Object actual = run("STATS_GEOMETRIC_MEAN(stats)", variables);
      assertEquals(stats.getGeometricMean(), (Double) actual, 0.1);
    }
  }

  @Test
  public void testMax() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_MAX(stats)", variables);
    assertEquals(stats.getMax(), (Double) actual, 0.1);
  }

  @Test
  public void testMin() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_MIN(stats)", variables);
    assertEquals(stats.getMin(), (Double) actual, 0.1);
  }

  @Test
  public void testSum() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_SUM(stats)", variables);
    assertEquals(stats.getSum(), (Double) actual, 0.1);
  }

  @Test
  public void testStandardDeviation() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_SD(stats)", variables);
    assertEquals(stats.getStandardDeviation(), (Double) actual, 0.1);
  }

  @Test
  public void testVariance() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_VARIANCE(stats)", variables);
    assertEquals(stats.getVariance(), (Double) actual, 0.1);
  }

  @Test
  public void testPopulationVariance() throws Exception {
    if(windowSize > 0) {
      statsInit(windowSize);
      Object actual = run("STATS_POPULATION_VARIANCE(stats)", variables);
      assertEquals(stats.getPopulationVariance(), (Double) actual, 0.1);
    }
  }

  @Test
  public void testQuadraticMean() throws Exception {
    if(windowSize > 0) {
      statsInit(windowSize);
      Object actual = run("STATS_QUADRATIC_MEAN(stats)", variables);
      assertEquals(stats.getQuadraticMean(), (Double) actual, 0.1);
    }
  }

  @Test
  public void testSumLogsNoWindow() throws Exception {
    statsInit(0);
    Object actual = run("STATS_SUM_LOGS(stats)", variables);
    assertEquals(summaryStats.getSumOfLogs(), (Double) actual, 0.1);
  }

  @Test(expected = ParseException.class)
  public void testSumLogsWithWindow() throws Exception {
    statsInit(100);
    run("STATS_SUM_LOGS(stats)", variables);
  }

  @Test
  public void testSumSquares() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_SUM_SQUARES(stats)", variables);
    assertEquals(stats.getSumsq(), (Double) actual, 0.1);
  }

  @Test
  public void testKurtosis() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_KURTOSIS(stats)", variables);
    assertEquals(stats.getKurtosis(), (Double) actual, 0.1);
  }

  @Test
  public void testSkewness() throws Exception {
    statsInit(windowSize);
    Object actual = run("STATS_SKEWNESS(stats)", variables);
    assertEquals(stats.getSkewness(), (Double) actual, 0.1);
  }

  @Test
  public void testPercentileNoWindow() throws Exception {
    statsInit(0);
    final double percentile = 0.9;
    Object actual = run(format("STATS_PERCENTILE(stats, %f)", percentile), variables);
    assertEquals(stats.getPercentile(percentile), (Double) actual, 1);
  }

  @Test
  public void testPercentileWithWindow() throws Exception {
    statsInit(100);
    final double percentile = 0.9;
    Object actual = run(format("STATS_PERCENTILE(stats, %f)", percentile), variables);
    assertEquals(stats.getPercentile(percentile), (Double) actual, 0.1);
  }

  @Test
  public void testWithNull() throws Exception {
    {
      Object actual = run("STATS_MEAN(null)", variables);
      assertTrue(((Double) actual).isNaN());
    }
    {
      double actual = (double) run("STATS_COUNT(null)", variables);
      assertTrue(((Double)actual).isNaN());
    }
    {
      Object actual = run("STATS_VARIANCE(null)", variables);
      assertTrue(((Double) actual).isNaN());
    }
  }
}