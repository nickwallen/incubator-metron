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
package org.apache.metron.management;

import org.apache.metron.management.EnrichmentFunctions.Enricher;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.common.shell.VariableResult;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.MapVariableResolver;
import org.apache.metron.stellar.dsl.functions.resolver.FunctionResolver;
import org.apache.metron.stellar.dsl.functions.resolver.SimpleFunctionResolver;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
public class EnrichmentFunctionsTest {

    private Map<String, Object> variables;
    private Context context = null;
    private FunctionResolver functionResolver;

    @Test
    public void testInitialize() {
        Enricher enricher = execute("ENRICHER_INIT()", Enricher.class);
        assertNotNull(enricher);
        assertNotNull(enricher.getEnrichmentConfig());
    }

    @Test
    public void testEnrich() {

    }

    @Before
    public void setup() {
        functionResolver = new SimpleFunctionResolver()
                .withClass(EnrichmentFunctions.EnricherInit.class)
                .withClass(EnrichmentFunctions.EnricherEnrich.class);
        variables = new HashMap<>();
        context = new Context.Builder().build();
    }

    private <T> T assign(String variable, String expression, Class<T> clazz) {
        T result= execute(expression, clazz);
        variables.put(variable, result);
        return result;
    }

    private <T> T execute(String expression, Class<T> clazz) {
        return execute(expression, Collections.emptyMap(), clazz);
    }

    private <T> T execute(String expression, Map<String, Object> variables, Class<T> clazz) {
        StellarProcessor processor = new StellarProcessor();
        Object result = processor.parse(expression, new MapVariableResolver(variables), functionResolver, context);
        return clazz.cast(result);
    }

}
