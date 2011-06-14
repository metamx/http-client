/*
 * Copyright 2011 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.metamx.http.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
*/
class LatchedFuture<T> implements Future<T>
{
  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile T obj = null;

  @Override
  public boolean cancel(boolean b)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCancelled()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDone()
  {
    return latch.getCount() == 0;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException
  {
    latch.await();
    return obj;
  }

  @Override
  public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
  {
    latch.await(l, timeUnit);
    return obj;
  }

  public void set(T obj)
  {
    synchronized (latch) {
      if (isDone()) {
        throw new IllegalStateException(String.format("set() was called multiple times, can only be called once."));
      }

      this.obj = obj;
      latch.countDown();
    }
  }
}
