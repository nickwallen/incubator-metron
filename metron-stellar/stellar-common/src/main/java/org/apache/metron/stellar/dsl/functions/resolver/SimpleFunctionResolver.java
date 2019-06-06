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

package org.apache.metron.stellar.dsl.functions.resolver;

import com.google.common.base.Suppliers;
import org.apache.metron.stellar.dsl.StellarFunction;
import org.apache.metron.stellar.dsl.StellarFunctionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A simple Stellar function resolver that resolves functions from specific
 * classes rather than by searching the classpath.
 *
 *     FunctionResolver functionResolver = new SimpleFunctionResolver()
 *       .withClass(OneStellarFunction.class)
 *       .withClass(AnotherStellarFunction.class)
 *       .withClass(YetAnotherFunction.class)
 */
public class SimpleFunctionResolver extends BaseFunctionResolver {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The classes that will be further interrogated for Stellar functions.
   */
  Set<Class<? extends StellarFunction>> classesToResolve = new HashSet<>();

  @Override
  public Set<Class<? extends StellarFunction>> resolvables() {
    return classesToResolve;
  }

  /**
   * Attempts to resolve any functions defined within a specific class.
   *
   * @param clazz The class which may contain a Stellar function.
   */
  public SimpleFunctionResolver withClass(Class<? extends StellarFunction> clazz) {
    this.classesToResolve.add(clazz);
    return this;
  }

  /**
   * Attempts to resolve a function defined within the provided {@link StellarFunction}
   * instance.
   *
   * @param function The Stellar function to resolve.
   */
  public SimpleFunctionResolver withInstance(StellarFunction function) {
    // perform function resolution on the instance that was passed in
    StellarFunctionInfo functionInfo = resolveFunction(function.getClass());
    functionInfo.setFunction(function);

    // add the function to the set of resolvable functions
    Map<String, StellarFunctionInfo> currentFunctions = this.functions.get();
    currentFunctions.put(functionInfo.getName(), functionInfo);

    this.functions = Suppliers.memoize(() -> currentFunctions);
    return this;
  }
}
