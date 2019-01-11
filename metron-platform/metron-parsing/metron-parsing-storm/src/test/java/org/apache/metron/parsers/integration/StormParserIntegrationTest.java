package org.apache.metron.parsers.integration;/*
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

import java.util.ArrayList;
import java.util.List;
import org.apache.metron.parsers.integration.validation.ParserDriver;
import org.apache.metron.parsers.integration.validation.SampleDataValidation;
import org.apache.metron.parsers.integration.validation.StormParserDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class StormParserIntegrationTest extends ParserIntegrationTest {

  @Parameters(name = "{index}: sensorType={0}")
  public static Iterable<String> data() {
    return sensorTypes;
  }

  @Parameter
  public String sensorType;

  @Test
  public void test() throws Exception {
    ParserDriver driver = new StormParserDriver(sensorType, readSensorConfig(sensorType),
        readGlobalConfig());
    runTest(driver);
  }

  @Override
  List<ParserValidation> getValidations() {
    return new ArrayList<ParserValidation>() {{
      add(new SampleDataValidation());
    }};
  }
}
