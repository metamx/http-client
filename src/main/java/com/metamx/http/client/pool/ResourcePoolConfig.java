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

/**
 */
public class ResourcePoolConfig
{
  private final int maxPerKey;
  private final boolean cleanIdle;

  public ResourcePoolConfig(
      int maxPerKey,
      boolean cleanIdle
  )
  {
    if (cleanIdle) {
      throw new IllegalStateException(
          "Cleaning up idle connections is a bad idea.  "
          + "If your services can't handle the max number then lower the max number."
      );
    }

    this.maxPerKey = maxPerKey;
    this.cleanIdle = cleanIdle;
  }

  public int getMaxPerKey()
  {
    return maxPerKey;
  }

  public boolean isCleanIdle()
  {
    return cleanIdle;
  }
}
