package org.apache.metron.stellar.dsl.functions;

import org.apache.metron.stellar.common.utils.StellarProcessorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ListFunctionsTest {

  @Test
  public void testCreateList() {
    List<Integer> expected = new ArrayList<>();
    expected.add(1);
    expected.add(2);
    expected.add(3);
    assertEquals(expected, exec("[1, 2, 3]"));

    assertEquals(Collections.emptyList(), exec("[]"));
  }

  @Test
  public void testListEquality() {
    assertTrue(bool("[1] == [1]"));
    assertTrue(bool("[1, 2] == [1, 2]"));
    assertTrue(bool("[1, 2, 3] == [1, 2, 3]"));

    assertFalse(bool("[1] == [1, 2]"));
    assertFalse(bool("[1, 2] == [1, 2, 3]"));
    assertFalse(bool("[3, 2, 1] == [1, 2, 3]"));

    assertFalse(bool("[1] == []"));
    assertFalse(bool("[1, 2] == []"));
    assertFalse(bool("[3, 2, 1] == []"));

    assertTrue(bool("['foo'] == ['foo']"));
    assertTrue(bool("['foo', 'bar'] == ['foo', 'bar']"));
    assertTrue(bool("['foo', 'bar', 'baz'] == ['foo', 'bar', 'baz']"));

    assertFalse(bool("['foo'] == ['baz', 'foo']"));
    assertFalse(bool("['foo', 'bar'] == ['baz', 'foo', 'bar']"));
    assertFalse(bool("['foo', 'bar', 'baz'] == ['baz', 'foo', 'bar']"));

    assertTrue(bool("[] == []"));
  }

  @Test
  public void testListInequality() {
    assertTrue(bool("[1] != [1, 2]"));
    assertTrue(bool("[1, 2] != [1, 2, 3]"));
    assertTrue(bool("[3, 2, 1] != [1, 2, 3]"));

    assertFalse(bool("[1] != [1]"));
    assertFalse(bool("[1, 2] != [1, 2]"));
    assertFalse(bool("[1, 2, 3] != [1, 2, 3]"));

    assertTrue(bool("['foo'] != ['baz', 'foo']"));
    assertTrue(bool("['foo', 'bar'] != ['baz', 'foo', 'bar']"));
    assertTrue(bool("['foo', 'bar', 'baz'] != ['baz', 'foo', 'bar']"));

    assertFalse(bool("['foo'] != ['foo']"));
    assertFalse(bool("['foo', 'bar'] != ['foo', 'bar']"));
    assertFalse(bool("['foo', 'bar', 'baz'] != ['foo', 'bar', 'baz']"));
  }

  @Test
  public void testListInclusion() {
    assertTrue(bool("1 in [1, 2, 3]"));
    assertTrue(bool("1 in [1, 2]"));
    assertTrue(bool("1 in [1]"));

    assertFalse(bool("1 in [2, 3]"));
    assertFalse(bool("1 in [3]"));
    assertFalse(bool("1 in []"));

    assertTrue(bool("'foo' in ['foo', 'bar', 'baz']"));
    assertTrue(bool("'foo' in ['foo', 'bar']"));
    assertTrue(bool("'foo' in ['foo']"));
  }

  @Test
  public void testListNonInclusion() {
    assertFalse(bool("1 not in [1, 2, 3]"));
    assertFalse(bool("1 not in [1, 2]"));
    assertFalse(bool("1 not in [1]"));

    assertTrue(bool("1 not in [2, 3]"));
    assertTrue(bool("1 not in [3]"));
    assertTrue(bool("1 not in []"));

    assertFalse(bool("'foo' not in ['foo', 'bar', 'baz']"));
    assertFalse(bool("'foo' not in ['foo', 'bar']"));
    assertFalse(bool("'foo' not in ['foo']"));
  }

  @Test
  public void testListLookup() {
    List<Integer> listOfThree = new ArrayList<>();
    listOfThree.add(1);
    listOfThree.add(2);
    listOfThree.add(3);
    Map<String, Object> variables = Collections.singletonMap("three", listOfThree);

    assertTrue(bool("three[0] == 1", variables));
    assertTrue(bool("three[1] == 2", variables));
    assertTrue(bool("three[2] == 3", variables));
  }

  @Test
  public void testListLookupOvershoot() {
    List<Integer> listOfThree = new ArrayList<>();
    listOfThree.add(1);
    listOfThree.add(2);
    listOfThree.add(3);
    Map<String, Object> variables = Collections.singletonMap("three", listOfThree);

    exec("three[4]", variables);
    fail("expected exception");
  }

  @Test
  public void testListLookupUndershoot() {
    List<Integer> listOfThree = new ArrayList<>();
    listOfThree.add(1);
    listOfThree.add(2);
    listOfThree.add(3);
    Map<String, Object> variables = Collections.singletonMap("three", listOfThree);

    exec("three[-1]", variables);
    fail("expected exception");
  }

  private Object exec(String expression) {
    return exec(expression, Collections.emptyMap());
  }

  private Object exec(String expression, Map<String, Object> variables) {
    return StellarProcessorUtils.run(expression, variables);
  }

  private boolean bool(String expression) {
    return (boolean) exec(expression);
  }

  private boolean bool(String expression, Map<String, Object> variables) {
    return (boolean) StellarProcessorUtils.run(expression, variables);
  }
}
