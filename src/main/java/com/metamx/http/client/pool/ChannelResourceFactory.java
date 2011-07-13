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

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Future;

/**
*/
public class ChannelResourceFactory implements ResourceFactory<String, ChannelFuture>
{
  private final ClientBootstrap bootstrap;

  public ChannelResourceFactory(ClientBootstrap bootstrap) {
    this.bootstrap = bootstrap;
  }

  @Override
  public ChannelFuture generate(String hostname)
  {
    URL url = null;
    try {
      url = new URL(hostname);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    return bootstrap.connect(
        new InetSocketAddress(
            url.getHost(), url.getPort() == -1 ? url.getDefaultPort() : url.getPort()
        )
    );
  }

  @Override
  public boolean isGood(ChannelFuture resource)
  {
    Channel channel = resource.awaitUninterruptibly().getChannel();

    return channel.isConnected() && channel.isOpen();
  }

  @Override
  public void close(ChannelFuture resource)
  {
    resource.awaitUninterruptibly().getChannel().close();
  }
}
