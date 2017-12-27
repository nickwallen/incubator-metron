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

package org.apache.metron.stellar.common.executor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.commons.lang.ClassUtils;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.common.shell.StellarExecutionListeners;
import org.apache.metron.stellar.common.shell.StellarExecutionNotifier;
import org.apache.metron.stellar.common.shell.VariableResult;
import org.apache.metron.stellar.common.shell.specials.AssignmentCommand;
import org.apache.metron.stellar.common.shell.specials.Comment;
import org.apache.metron.stellar.common.shell.specials.DocCommand;
import org.apache.metron.stellar.common.shell.specials.MagicDefineGlobal;
import org.apache.metron.stellar.common.shell.specials.MagicListFunctions;
import org.apache.metron.stellar.common.shell.specials.MagicListGlobals;
import org.apache.metron.stellar.common.shell.specials.MagicListVariables;
import org.apache.metron.stellar.common.shell.specials.MagicUndefineGlobal;
import org.apache.metron.stellar.common.shell.specials.QuitCommand;
import org.apache.metron.stellar.common.shell.specials.SpecialCommand;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctionInfo;
import org.apache.metron.stellar.dsl.functions.resolver.FunctionResolver;
import org.apache.metron.stellar.dsl.MapVariableResolver;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.stellar.dsl.VariableResolver;
import org.apache.metron.stellar.common.utils.ConversionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The default implementation of a StellarStatefulExecutor.
 */
public class DefaultStellarStatefulExecutor implements StellarStatefulExecutor, StellarExecutionNotifier, Serializable {

  /**
   * The current state of the Stellar execution environment.
   */
//  private Map<String, Object> state;

  /**
   * The variables known by Stellar.
   */
  private Map<String, VariableResult> variables;

  /**
   * Provides additional context for initializing certain Stellar functions.  For
   * example, references to Zookeeper or HBase.
   */
  private Context context;

  /**
   * Responsible for function resolution.
   */
  private FunctionResolver functionResolver;

  /**
   * A registry of all special commands; like %magic, ?doc, and quit.
   *
   * Maps the special command (like '%globals') to the command implementing it.
   */
  private List<SpecialCommand> commandRegistry;

  /**
   * Listeners that are notified when a function is defined.
   */
  private List<StellarExecutionListeners.FunctionDefinedListener> functionListeners;

  /**
   * Listeners that are notified when a variable is defined.
   */
  private List<StellarExecutionListeners.VariableDefinedListener> variableListeners;

  /**
   * Listeners that are notified when a special command is defined.
   */
  private List<StellarExecutionListeners.SpecialDefinedListener> specialListeners;

  public DefaultStellarStatefulExecutor() {
    this(StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT(), Collections.emptyList());
  }

  public DefaultStellarStatefulExecutor(FunctionResolver functionResolver, Context context, List<SpecialCommand> specials) {
    clearState();
    this.context = context;
    this.functionResolver = functionResolver;
    this.functionListeners = new ArrayList<>();
    this.variableListeners = new ArrayList<>();
    this.specialListeners = new ArrayList<>();
    this.commandRegistry = specials;
  }

  /**
   * @param initialState Initial state loaded into the execution environment.
   */
  public DefaultStellarStatefulExecutor(Map<String, Object> initialState) {
    this();
    this.variables = new HashMap<>();

    // define the initial state
    initialState.forEach((varName, varValue) ->
      this.variables.put(varName, VariableResult.withValue(varValue))
    );
  }

  @Override
  public void init() {
    StellarFunctions.initialize(this.context);

    // notify listeners about the new specials
    for(SpecialCommand command : commandRegistry) {
      notifySpecialListeners(command);
    }

    // TODO are all the functions defined at this point?

    // notify the listeners about the new functions that have been defined
    for(StellarFunctionInfo fn : functionResolver.getFunctionInfo()) {
      notifyFunctionListeners(fn);
    }
  }

  @Override
  public Map<String, VariableResult> getState() {
    return UnmodifiableMap.decorate(variables);
  }

  public Map<String, Object> getVariables() {
    return Maps.transformValues(variables, (v) -> v.getResult());
  }

  /**
   * Execute an expression and assign the result to a variable.  The variable is maintained
   * in the context of this executor and is available to all subsequent expressions.
   *
   * @param variable       The name of the variable to assign to.
   * @param expression     The expression to execute.
   * @param transientState Additional state available to the expression.  This most often represents
   *                       the values available to the expression from an individual message. The state
   *                       maps a variable name to a variable's value.
   */
  @Override
  public void assign(String variable, String expression, Map<String, Object> transientState) {
    Object result = execute(expression, transientState);
    if(result == null || variable == null) {
      return;
    }
    state.put(variable, result);
  }

  @Override
  public void assign(String variable, Object value) {

    // assign a value to the variable
    VariableResult varResult = VariableResult.withValue(value);
    this.variables.put(variable, varResult);

    // notify listeners about the new variable
    notifyVariableListeners(variable, varResult);

  }

  /**
   * Execute a Stellar expression and return the result.  The internal state of the executor
   * is not modified.
   *
   * @param expression The expression to execute.
   * @param state      Additional state available to the expression.  This most often represents
   *                   the values available to the expression from an individual message. The state
   *                   maps a variable name to a variable's value.
   * @param clazz      The expected type of the expression's result.
   * @param <T>        The expected type of the expression's result.
   */
  @Override
  public <T> StellarResult<T> execute(String expression, Map<String, Object> state, Class<T> clazz) {
    Object resultObject = execute(expression, state);

    // perform type conversion, if necessary
    T result = ConversionUtils.convert(resultObject, clazz);
    if (result == null) {
      throw new IllegalArgumentException(String.format("Unexpected type: expected=%s, actual=%s, expression=%s",
              clazz.getSimpleName(), ClassUtils.getShortClassName(resultObject,"null"), expression));
    }

    return StellarResult.success(result);
  }

  @Override
  public void assign(String variableName, Object value, Optional<String> expression) {


    VariableResult varResult = VariableResult.withExpression(value, expression);
    this.variables.put(variableName, varResult);

    // notify listeners about the new variable
    notifyVariableListeners(variableName, varResult);
  }

  @Override
  public void clearState() {
    this.variables = new HashMap<>();
  }

  @Override
  public void setContext(Context context) {
    this.context = context;
  }

  public void setFunctionResolver(FunctionResolver functionResolver) {
    this.functionResolver = functionResolver;
  }

  /**
   * Execute a Stellar expression.
   *
   * @param expression     The expression to execute.
   * @param transientState Additional state available to the expression.  This most often represents
   *                       the values available to the expression from an individual message. The state
   *                       maps a variable name to a variable's value.
   */
  private Object execute(String expression, Map<String, Object> transientState) {
    VariableResolver variableResolver = new MapVariableResolver(getVariables(), transientState);
    StellarProcessor processor = new StellarProcessor();
    return processor.parse(expression, variableResolver, functionResolver, context);
  }

  /**
   * Add a listener that will be notified when a function is defined.
   * @param listener The listener to notify.
   */
  @Override
  public void addFunctionListener(StellarExecutionListeners.FunctionDefinedListener listener) {
    this.functionListeners.add(listener);
  }

  /**
   * Notify function listeners that a function has been defined.
   * @param functionInfo The function that was defined.
   */
  private void notifyFunctionListeners(StellarFunctionInfo functionInfo) {
    for(StellarExecutionListeners.FunctionDefinedListener listener : functionListeners) {
      listener.whenFunctionDefined(functionInfo);
    }
  }

  /**
   * Add a listener that will be notified when a variable is defined.
   * @param listener The listener to notify.
   */
  @Override
  public void addVariableListener(StellarExecutionListeners.VariableDefinedListener listener) {
    this.variableListeners.add(listener);
  }

  /**
   * Notify variable listeners that a variable has been (re)defined.
   * @param variableName The variable name.
   * @param result The variable result.
   */
  private void notifyVariableListeners(String variableName, VariableResult result) {
    for(StellarExecutionListeners.VariableDefinedListener listener : variableListeners) {
      listener.whenVariableDefined(variableName, result);
    }
  }

  /**
   * Add a listener that will be notified when a magic command is defined.
   * @param listener The listener to notify.
   */
  @Override
  public void addSpecialListener(StellarExecutionListeners.SpecialDefinedListener listener) {
    this.specialListeners.add(listener);
  }

  /**
   * Notify listeners that a magic command has been defined.
   * @param specialCommand The magic command.
   */
  private void notifySpecialListeners(SpecialCommand specialCommand) {
    for(StellarExecutionListeners.SpecialDefinedListener listener : specialListeners) {
      listener.whenSpecialDefined(specialCommand);
    }
  }

}
