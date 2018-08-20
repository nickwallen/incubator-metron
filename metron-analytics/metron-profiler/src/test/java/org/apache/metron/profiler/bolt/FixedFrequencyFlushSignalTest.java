/*
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

package org.apache.metron.profiler.bolt;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@code FixedFrequencyFlushSignal} class.
 */
public class FixedFrequencyFlushSignalTest {

  @Test
  public void testSignalFlush() {

    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(1000);

    // not time to flush yet
    assertFalse(signal.isTimeToFlush());

    // advance time
    signal.update(5000);

    // not time to flush yet
    assertFalse(signal.isTimeToFlush());

    // advance time
    signal.update(7000);

    // time to flush
    assertTrue(signal.isTimeToFlush());
  }

  @Test
  public void testOutOfOrderTimestamps() {
    int flushFreq = 1000;
    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(flushFreq);

    // advance time, out-of-order
    signal.update(5000);
    signal.update(1000);
    signal.update(7000);
    signal.update(3000);

    // need to flush @ min + flushFreq = 1000 + 1000 = 6000. if anything > 6000, then it should signal a flush
    assertTrue(signal.isTimeToFlush());
  }

  @Test
  public void testOutOfOrderTimestampsNoFlush() {
    int flushFreq = 7000;
    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(flushFreq);

    // advance time, out-of-order
    signal.update(5000);
    signal.update(1000);
    signal.update(7000);
    signal.update(3000);

    // need to flush @ min + flushFreq = 1000 + 7000 = 8000. if anything > 8000, then it should signal a flush
    assertFalse(signal.isTimeToFlush());
  }

  @Test
  public void testTimestampsDescending() {
    int flushFreq = 3000;
    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(flushFreq);

    // advance time, out-of-order
    signal.update(4000);
    signal.update(3000);
    signal.update(2000);
    signal.update(1000);

    // need to flush @ min + flushFreq = 1000 + 3000 = 4000. if anything > 4000, then it should signal a flush
    assertTrue(signal.isTimeToFlush());
  }

  @Test
  public void testTimestampsDescendingNoFlush() {
    int flushFreq = 4000;
    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(flushFreq);

    // advance time, out-of-order
    signal.update(4000);
    signal.update(3000);
    signal.update(2000);
    signal.update(1000);

    // need to flush @ min + flushFreq = 1000 + 4000 = 5000. if anything > 5000, then it should signal a flush
    assertFalse(signal.isTimeToFlush());
  }

  @Test
  public void testTimestampsAscending() {
    int flushFreq = 3000;
    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(flushFreq);

    // advance time, out-of-order
    signal.update(1000);
    signal.update(2000);
    signal.update(3000);
    signal.update(4000);

    // need to flush @ min + flushFreq = 1000 + 3000 = 4000. if anything >= 4000, then it should signal a flush
    assertTrue(signal.isTimeToFlush());
  }

  @Test
  public void testTimestampsAscendingNoFlush() {
    int flushFreq = 4000;
    FixedFrequencyFlushSignal signal = new FixedFrequencyFlushSignal(flushFreq);

    // advance time, out-of-order
    signal.update(1000);
    signal.update(2000);
    signal.update(3000);
    signal.update(4000);

    // need to flush @ min + flushFreq = 1000 + 4000 = 5000. if anything >= 5000, then it should signal a flush
    assertFalse(signal.isTimeToFlush());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeFrequency() {
    new FixedFrequencyFlushSignal(-1000);
  }
}
