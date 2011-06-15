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

/**
*/
public class ChannelResourceFactory implements ResourceFactory<String, Channel>
{
  private final ClientBootstrap bootstrap;

  public ChannelResourceFactory(ClientBootstrap bootstrap) {
    this.bootstrap = bootstrap;
  }

  @Override
  public Channel generate(String hostname)
  {
    URL url = null;
    try {
      url = new URL(hostname);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    ChannelFuture channelFuture = bootstrap.connect(
        new InetSocketAddress(
            url.getHost(), url.getPort() == -1 ? url.getDefaultPort() : url.getPort()
        )
    );

    Channel channel = channelFuture.awaitUninterruptibly().getChannel();

    return channel;
  }

  @Override
  public boolean isGood(Channel resource)
  {
    return resource.isConnected() && resource.isOpen();
  }

  @Override
  public void close(Channel resource)
  {
    resource.close();
  }
}
