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
package org.apache.metron.stellar.common.shell;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the DefaultStellarShellExecutor class.
 */
public class DefaultStellarShellExecutorTest {

  DefaultStellarShellExecutor executor;
  boolean notified;

  @Before
  public void setup() throws Exception {
    Properties props = new Properties();
    executor = new DefaultStellarShellExecutor(props, Optional.empty());
    executor.init();
  }

  @Test
  public void testAssignment() {

    // x = 2 + 2
    {
      StellarShellResult result = executor.execute("x := 2 + 2");
      assertTrue(result.isSuccess());
      assertTrue(result.getValue().isPresent());
      assertEquals(4, result.getValue().get());
      assertEquals(4, executor.getVariables().get("x"));
    }

    // y = x + 2
    {
      StellarShellResult result = executor.execute("y := x + 2");
      assertTrue(result.isSuccess());
      assertTrue(result.getValue().isPresent());
      assertEquals(6, result.getValue().get());
      assertEquals(6, executor.getVariables().get("y"));
    }

    // z = x + y
    {
      StellarShellResult result = executor.execute("z := x + y");
      assertTrue(result.isSuccess());
      assertTrue(result.getValue().isPresent());
      assertEquals(10, result.getValue().get());
      assertEquals(10, executor.getVariables().get("z"));
    }
  }

  @Test
  public void testAssignmentWithOddWhitespace() {
    StellarShellResult result = executor.execute("   x   :=    2 +      2      ");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertEquals(4, result.getValue().get());
    assertEquals(4, executor.getVariables().get("x"));
  }

  @Test
  public void testBadAssignment() {
    StellarShellResult result = executor.execute("x := 2 + ");
    assertTrue(result.isError());
    assertTrue(result.getException().isPresent());
  }

  @Test
  public void testExpression() {
    StellarShellResult result = executor.execute("2 + 2");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertEquals(4, result.getValue().get());
  }

  @Test
  public void testExpressionWithOddWhitespace() {
    StellarShellResult result = executor.execute("    2    +    2");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertEquals(4, result.getValue().get());
  }

  @Test
  public void testBadExpression() {
    StellarShellResult result = executor.execute("2 + ");
    assertTrue(result.isError());
    assertTrue(result.getException().isPresent());
  }

  @Test
  public void testMagicCommand() {
    // create a var that we can see with the magic command
    executor.execute("x := 2 + 2");

    // just testing that we can execute the magic, not the actual result
    StellarShellResult result = executor.execute("%vars");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertNotNull(result.getValue().get());
  }

  @Test
  public void testDefineGlobal() {

    // create a global var
    executor.execute("%define x := 2");

    assertFalse(executor.getVariables().containsKey("x"));

    // the global should have been defined
    assertEquals(2, executor.getGlobalConfig().get("x"));
  }

  @Test
  public void testBadMagicCommand() {
    StellarShellResult result = executor.execute("%invalid");
    assertTrue(result.isError());
    assertTrue(result.getException().isPresent());
  }

  @Test
  public void testDocCommand() {
    // just testing that we can execute the doc, not the actual result
    StellarShellResult result = executor.execute("?TO_STRING");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertNotNull(result.getValue().get());
  }

  @Test
  public void testBadDocCommand() {
    StellarShellResult result = executor.execute("?INVALID");
    assertTrue(result.isError());
    assertTrue(result.getException().isPresent());
  }

  @Test
  public void testQuit() {
    StellarShellResult result = executor.execute("quit");
    assertTrue(result.isTerminate());
  }

  @Test
  public void testAssign() {
    {
      // this is how variables get loaded into the REPL directly from a file
      executor.assign("x", 10, Optional.empty());
    }
    {
      StellarShellResult result = executor.execute("x + 2");
      assertTrue(result.isSuccess());
      assertTrue(result.getValue().isPresent());
      assertEquals(12, result.getValue().get());
    }
  }

  @Test
  public void testNotifyVariableListeners() {
    notified = false;
    executor.addVariableListener((var, value) -> {
      assertEquals("x", var);
      assertEquals(4, value.getResult());
      notified = true;
    });

    executor.execute("x := 2 + 2");
    assertTrue(notified);
  }

  @Test
  public void testNotifySpecialListeners() throws Exception {

    // setup an executor
    notified = false;
    Properties props = new Properties();
    DefaultStellarShellExecutor executor = new DefaultStellarShellExecutor(props, Optional.empty());

    // setup listener
    notified = false;
    executor.addSpecialListener((magic) -> {
      assertNotNull(magic);
      assertNotNull(magic.getCommand());
      notified = true;
    });

    // initialize... magics should be setup during initialization
    executor.init();
    assertTrue(notified);
  }

  @Test
  public void testNotifyFunctionListeners() throws Exception {
    // setup an executor
    notified = false;
    Properties props = new Properties();
    DefaultStellarShellExecutor executor = new DefaultStellarShellExecutor(props, Optional.empty());

    // setup listener
    notified = false;
    executor.addFunctionListener((fn) -> {
      assertNotNull(fn);
      assertNotNull(fn.getName());
      assertNotNull(fn.getFunction());
      notified = true;
    });

    // initialize... magics should be setup during initialization
    executor.init();
    assertTrue(notified);
  }

  @Test
  public void testEmptyInput() {
    StellarShellResult result = executor.execute("");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertEquals("", result.getValue().get());
  }

  @Test
  public void testComment() {
    StellarShellResult result = executor.execute("# this is a comment");
    assertTrue(result.isSuccess());
    assertTrue(result.getValue().isPresent());
    assertEquals("", result.getValue().get());
  }
}