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

package com.metamx.http.client.pool;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class ResourcePool<K, V> implements Closeable
{
  private static final Logger log = Logger.getLogger(ResourcePool.class);
  private final ConcurrentMap<K, ImmediateCreationResourceHolder<K, V>> pool;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ResourcePool(
      final ResourceFactory<K, V> factory,
      final ResourcePoolConfig config
  )
  {
    this.pool = new MapMaker().makeComputingMap(
        new Function<K, ImmediateCreationResourceHolder<K, V>>()
        {
          @Override
          public ImmediateCreationResourceHolder<K, V> apply(K input)
          {
            return new ImmediateCreationResourceHolder<K, V>(
                config.getMaxPerKey(),
                input,
                factory
            );
          }
        }
    );
  }

  public ResourceContainer<V> take(final K key)
  {
    if (closed.get()) {
      log.error(String.format("take(%s) called even though I'm closed.", key));
      return null;
    }

    final ImmediateCreationResourceHolder<K, V> holder = pool.get(key);
    final V value = holder.get();

    return new ResourceContainer<V>()
    {
      private final AtomicBoolean returned = new AtomicBoolean(false);

      @Override
      public V get()
      {
        Preconditions.checkState(!returned.get(), "Resource for key[%s] has been returned, cannot get().", key);
        return value;
      }

      @Override
      public void returnResource()
      {
        if (returned.getAndSet(true)) {
          log.warn(String.format("Resource at key[%s] was returned multiple times?", key));
        } else {
          holder.giveBack(value);
        }
      }

      @Override
      protected void finalize() throws Throwable
      {
        if (!returned.get()) {
          log.warn(
              String.format(
                  "Resource[%s] at key[%s] was not returned before Container was finalized, potential resource leak.",
                  value,
                  key
              )
          );
          returnResource();
        }
        super.finalize();
      }
    };
  }

  public void close()
  {
    closed.set(true);
    for (K k : pool.keySet()) {
      pool.remove(k).close();
    }
  }

  private static class ImmediateCreationResourceHolder<K, V>
  {
    private final int maxSize;
    private final K key;
    private final ResourceFactory<K, V> factory;

    private final LinkedList<V> objectList;

    private boolean closed = false;

    private ImmediateCreationResourceHolder(
        int maxSize,
        K key,
        ResourceFactory<K, V> factory
    )
    {
      this.maxSize = maxSize;
      this.key = key;
      this.factory = factory;

      this.objectList = new LinkedList<V>();
      for (int i = 0; i < maxSize; ++i) {
        objectList.addLast(factory.generate(key));
      }
    }

    V get()
    {
      final V retVal;
      synchronized (this) {
        if (closed) {
          log.info(String.format("get() called even though I'm closed. key[%s]", key));
          return null;
        }

        while (objectList.size() == 0) {
          try {
            this.wait();
          }
          catch (InterruptedException e) {
            Thread.interrupted();
            return null;
          }
        }

        retVal = objectList.removeFirst();
      }

      if (!factory.isGood(retVal)) {
        // If current object is no good, close it and make a new one.
        factory.close(retVal);
        return factory.generate(key);
      }

      return retVal;
    }

    void giveBack(V object)
    {
      synchronized (this) {
        if (closed) {
          log.info(String.format("giveBack called after being closed. key[%s]", key));
          factory.close(object);
          return;
        }

        if (objectList.size() >= maxSize) {
          if (objectList.contains(object)) {
            log.warn(
                String.format(
                    "Returning object[%s] at key[%s] that has already been returned!? Skipping",
                    object,
                    key
                ),
                new Exception("Exception for stacktrace")
            );
          } else {
            log.warn(
                String.format(
                    "Returning object[%s] at key[%s] even though we already have all that we can hold[%s]!? Skipping",
                    object,
                    key,
                    objectList
                ),
                new Exception("Exception for stacktrace")
            );
          }
          return;
        }

        objectList.addLast(object);
        this.notifyAll();
      }
    }

    void close()
    {
      synchronized (this) {
        closed = true;
        while (!objectList.isEmpty()) {
          factory.close(objectList.removeFirst());
        }
      }
    }
  }
}
