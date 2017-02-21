/*
 * Copyright (c) 2014-2015 Spotify AB
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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;

import java.util.List;



public class DropwizardMetrics implements Metrics {

  public static final String GROUP = "com.spotify.folsom";

  private final Timer gets;
  private final Meter getHits;
  private final Meter getMisses;

  private final Meter getSuccesses;
  private final Meter getFailures;

  private final Timer multigets;
  private final Meter multigetSuccesses;
  private final Meter multigetFailures;

  private final Timer sets;
  private final Meter setSuccesses;
  private final Meter setFailures;

  private final Timer deletes;
  private final Meter deleteSuccesses;
  private final Meter deleteFailures;

  private final Timer incrDecrs;
  private final Meter incrDecrSuccesses;
  private final Meter incrDecrFailures;

  private final Timer touches;
  private final Meter touchSuccesses;
  private final Meter touchFailures;

  private volatile OutstandingRequestsGauge internalOutstandingReqGauge;
  private final Gauge<Integer> outstandingRequestsGauge;

  public DropwizardMetrics(final MetricRegistry registry) {
    this.gets = registry.timer(name("get", "requests"));
    this.getSuccesses = registry.meter(name("get", "successes"));
    this.getHits = registry.meter(name("get", "hits"));
    this.getMisses = registry.meter(name("get", "misses"));
    this.getFailures = registry.meter(name("get", "failures"));

    this.multigets = registry.timer(name("multiget", "requests"));
    this.multigetSuccesses = registry.meter(name("multiget", "successes"));
    this.multigetFailures = registry.meter(name("multiget", "failures"));

    this.sets = registry.timer(name("set", "requests"));
    this.setSuccesses = registry.meter(name("set", "successes"));
    this.setFailures = registry.meter(name("set", "failures"));

    this.deletes = registry.timer(name("delete", "requests"));
    this.deleteSuccesses = registry.meter(name("delete", "successes"));
    this.deleteFailures = registry.meter(name("delete", "failures"));

    this.incrDecrs = registry.timer(name("incrdecr", "requests"));
    this.incrDecrSuccesses = registry.meter(name("incrdecr", "successes"));
    this.incrDecrFailures = registry.meter(name("incrdecr", "failures"));

    this.touches = registry.timer(name("touch", "requests"));
    this.touchSuccesses = registry.meter(name("touch", "successes"));
    this.touchFailures = registry.meter(name("touch", "failures"));

    final String gaugeName = name("outstandingRequests", "count");
    this.outstandingRequestsGauge = registry.register(gaugeName, new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return internalOutstandingReqGauge != null ?
                internalOutstandingReqGauge.getOutstandingRequests() : 0;
      }
    });
  }

  private String name(final String type, final String name) {
    return MetricRegistry.name(GROUP, type, name);
  }

  @Override
  public void measureGetFuture(ListenableFuture<GetResult<byte[]>> future) {
    final Timer.Context ctx = gets.time();

    final FutureCallback<GetResult<byte[]>> metricsCallback =
            new FutureCallback<GetResult<byte[]>>() {
      @Override
      public void onSuccess(final GetResult<byte[]> result) {
        getSuccesses.mark();
        if (result != null) {
          getHits.mark();
        } else {
          getMisses.mark();
        }
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        getFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureMultigetFuture(ListenableFuture<List<GetResult<byte[]>>> future) {
    final Timer.Context ctx = multigets.time();

    final FutureCallback<List<GetResult<byte[]>>> metricsCallback =
            new FutureCallback<List<GetResult<byte[]>>>() {
      @Override
      public void onSuccess(final List<GetResult<byte[]>> result) {
        multigetSuccesses.mark();
        int hits = 0;
        int total = result.size();
        for (int i = 0; i < total; i++) {
          if (result.get(i) != null) {
            hits++;
          }
        }
        getHits.mark(hits);
        getMisses.mark(total - hits);
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        multigetFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureDeleteFuture(ListenableFuture<MemcacheStatus> future) {
    final Timer.Context ctx = deletes.time();

    final FutureCallback<MemcacheStatus> metricsCallback = new FutureCallback<MemcacheStatus>() {
      @Override
      public void onSuccess(final MemcacheStatus result) {
        deleteSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        deleteFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureSetFuture(ListenableFuture<MemcacheStatus> future) {
    final Timer.Context ctx = sets.time();

    final FutureCallback<MemcacheStatus> metricsCallback = new FutureCallback<MemcacheStatus>() {
      @Override
      public void onSuccess(final MemcacheStatus result) {
        setSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        setFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureIncrDecrFuture(ListenableFuture<Long> future) {
    final Timer.Context ctx = incrDecrs.time();

    final FutureCallback<Long> metricsCallback = new FutureCallback<Long>() {
      @Override
      public void onSuccess(final Long result) {
        incrDecrSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        incrDecrFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureTouchFuture(ListenableFuture<MemcacheStatus> future) {
    final Timer.Context ctx = touches.time();

    final FutureCallback<MemcacheStatus> metricsCallback = new FutureCallback<MemcacheStatus>() {
      @Override
      public void onSuccess(final MemcacheStatus result) {
        touchSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        touchFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void registerOutstandingRequestsGauge(OutstandingRequestsGauge gauge) {
    this.internalOutstandingReqGauge = gauge;
  }

  public Timer getGets() {
    return gets;
  }

  public Meter getGetHits() {
    return getHits;
  }

  public Meter getGetMisses() {
    return getMisses;
  }

  public Meter getGetSuccesses() {
    return getSuccesses;
  }

  public Meter getGetFailures() {
    return getFailures;
  }

  public Timer getMultigets() {
    return multigets;
  }

  public Meter getMultigetSuccesses() {
    return multigetSuccesses;
  }

  public Meter getMultigetFailures() {
    return multigetFailures;
  }

  public Timer getSets() {
    return sets;
  }

  public Meter getSetSuccesses() {
    return setSuccesses;
  }

  public Meter getSetFailures() {
    return setFailures;
  }

  public Timer getDeletes() {
    return deletes;
  }

  public Meter getDeleteSuccesses() {
    return deleteSuccesses;
  }

  public Meter getDeleteFailures() {
    return deleteFailures;
  }

  public Timer getIncrDecrs() {
    return incrDecrs;
  }

  public Meter getIncrDecrSuccesses() {
    return incrDecrSuccesses;
  }

  public Meter getIncrDecrFailures() {
    return incrDecrFailures;
  }

  public Timer getTouches() {
    return touches;
  }

  public Meter getTouchSuccesses() {
    return touchSuccesses;
  }

  public Meter getTouchFailures() {
    return touchFailures;
  }

  public Gauge<Integer> getOutstandingRequestsGauge() {
    return outstandingRequestsGauge;
  }
}
