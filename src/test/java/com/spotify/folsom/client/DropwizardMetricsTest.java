/*
 * Copyright (c) 2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.folsom.client;

import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.spotify.folsom.AsciiMemcacheClient;
import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.FakeRawMemcacheClient;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.transcoder.StringTranscoder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DropwizardMetricsTest {

  private DropwizardMetrics metrics;
  private AsciiMemcacheClient<String> client;
  private FakeRawMemcacheClient fakeRawMemcacheClient;

  @Before
  public void setUp() throws Exception {
    metrics = new DropwizardMetrics(new MetricRegistry());
    fakeRawMemcacheClient = new FakeRawMemcacheClient(metrics);
    client = new DefaultAsciiMemcacheClient<>(
        fakeRawMemcacheClient,
        metrics,
        StringTranscoder.UTF8_INSTANCE,
        Charsets.UTF_8);
    ConnectFuture.connectFuture(client).get();
  }

  @Test
  public void testGetMiss() throws Exception {
    assertEquals(0, metrics.getGets().getCount());
    assertEquals(0, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getGetFailures().getCount());
    assertEquals(0, metrics.getGetSuccesses().getCount());

    assertNull(client.get("key-miss").get());

    awaitCount(1, metrics.getGets());
    assertEquals(0, metrics.getGetHits().getCount());
    assertEquals(1, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getGetFailures().getCount());
    assertEquals(1, metrics.getGetSuccesses().getCount());
  }

  @Test
  public void testGetHit() throws Exception {
    assertEquals(0, metrics.getGets().getCount());
    assertEquals(0, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getGetFailures().getCount());
    assertEquals(0, metrics.getGetSuccesses().getCount());

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).get());
    assertEquals("value", client.get("key").get());

    awaitCount(1, metrics.getGets());
    assertEquals(1, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getGetFailures().getCount());
    assertEquals(1, metrics.getGetSuccesses().getCount());
  }

  @Test
  public void testMultiget() throws Exception {
    assertEquals(0, metrics.getMultigets().getCount());
    assertEquals(0, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getMultigetFailures().getCount());
    assertEquals(0, metrics.getMultigetSuccesses().getCount());

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).get());
    assertEquals(Arrays.asList("value", null), client.get(Arrays.asList("key", "key-miss")).get());

    awaitCount(1, metrics.getMultigets());
    assertEquals(1, metrics.getGetHits().getCount());
    assertEquals(1, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getMultigetFailures().getCount());
    assertEquals(1, metrics.getMultigetSuccesses().getCount());
  }

  @Test
  public void testSet() throws Exception {
    assertEquals(0, metrics.getSets().getCount());
    assertEquals(0, metrics.getSetFailures().getCount());
    assertEquals(0, metrics.getSetSuccesses().getCount());

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).get());

    awaitCount(1, metrics.getSets());
    assertEquals(0, metrics.getSetFailures().getCount());
    assertEquals(1, metrics.getSetSuccesses().getCount());
  }

  @Test
  public void testIncrDecr() throws Exception {
    assertEquals(MemcacheStatus.OK, client.set("key", "0", 0).get());

    assertEquals(0, metrics.getIncrDecrs().getCount());
    assertEquals(0, metrics.getIncrDecrFailures().getCount());
    assertEquals(0, metrics.getIncrDecrSuccesses().getCount());

    assertEquals(Long.valueOf(1L), client.incr("key", 1).get());

    awaitCount(1, metrics.getIncrDecrs());
    assertEquals(0, metrics.getIncrDecrFailures().getCount());
    assertEquals(1, metrics.getIncrDecrSuccesses().getCount());
  }

  @Test
  public void testTouch() throws Exception {
    assertEquals(MemcacheStatus.OK, client.set("key", "0", 0).get());

    assertEquals(0, metrics.getTouches().getCount());
    assertEquals(0, metrics.getTouchFailures().getCount());
    assertEquals(0, metrics.getTouchSuccesses().getCount());

    assertEquals(MemcacheStatus.OK, client.touch("key", 1).get());

    awaitCount(1, metrics.getTouches());
    assertEquals(0, metrics.getTouchFailures().getCount());
    assertEquals(1, metrics.getTouchSuccesses().getCount());
  }

  @Test
  public void testDelete() throws Exception {
    assertEquals(0, metrics.getDeletes().getCount());
    assertEquals(0, metrics.getDeleteFailures().getCount());
    assertEquals(0, metrics.getDeleteSuccesses().getCount());

    assertEquals(MemcacheStatus.OK, client.delete("key").get());

    awaitCount(1, metrics.getDeletes());
    assertEquals(0, metrics.getDeleteFailures().getCount());
    assertEquals(1, metrics.getDeleteSuccesses().getCount());
  }

  /** Test wiring up of OutstandingRequestGauge to the Yammer-metrics gauge. */
  @Test
  public void testOutstandingRequests() throws Exception {
    // baseline
    assertEquals(0, metrics.getOutstandingRequestsGauge().getValue().intValue());

    fakeRawMemcacheClient.setOutstandingRequests(5);
    assertEquals(5, metrics.getOutstandingRequestsGauge().getValue().intValue());

    fakeRawMemcacheClient.setOutstandingRequests(0);
    assertEquals(0, metrics.getOutstandingRequestsGauge().getValue().intValue());
  }

  private void awaitCount(int expectedValue, Metered metered) throws InterruptedException {
    final int timeout = 10;
    final long t1 = System.currentTimeMillis();
    while (expectedValue != metered.getCount()) {
      if (System.currentTimeMillis() - t1 > timeout) {
        assertEquals(expectedValue, metered.getCount());
        return;
      }
      Thread.sleep(0, 100);
    }
  }
}
