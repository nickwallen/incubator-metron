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
package org.apache.metron.enrichment.parallel;

import org.apache.metron.stellar.common.utils.ConversionUtils;
import org.apache.metron.stellar.dsl.BaseStellarFunction;
import org.apache.metron.stellar.dsl.Stellar;

import java.util.List;

@Stellar(
        namespace = "",
        name = "SLEEP",
        description = "Suspends execution for an interval of time. Only useful for testing.",
        params = {
                "milliseconds - The number of milliseconds to sleep for."
        }
)
public class SleepFunction extends BaseStellarFunction {

    @Override
    public Object apply(List<Object> args) {
        if(args.size() != 1) {
            throw new IllegalArgumentException("Expected only one argument");
        }
        long millis = ConversionUtils.convert(args.get(0), Long.class);
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        return millis;
    }
}
