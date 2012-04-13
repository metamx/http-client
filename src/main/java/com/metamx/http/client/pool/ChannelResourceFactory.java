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

import com.google.common.base.Throwables;
import com.metamx.http.client.netty.HandshakeRememberingSslHandler;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;

/**
*/
public class ChannelResourceFactory implements ResourceFactory<String, ChannelFuture>
{
  private static final Logger log = Logger.getLogger(ChannelResourceFactory.class);
  private final ClientBootstrap bootstrap;
  private final SSLContext sslContext;

  public ChannelResourceFactory(ClientBootstrap bootstrap)
  {
    this(bootstrap, null);
  }

  public ChannelResourceFactory(
      ClientBootstrap bootstrap,
      SSLContext sslContext
  )
  {
    this.bootstrap = bootstrap;
    this.sslContext = sslContext;
  }

  @Override
  public ChannelFuture generate(String hostname)
  {
    log.info(String.format("Generating: %s", hostname));
    URL url = null;
    try {
      url = new URL(hostname);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    final ChannelFuture retVal = bootstrap.connect(
        new InetSocketAddress(
            url.getHost(), url.getPort() == -1 ? url.getDefaultPort() : url.getPort()
        )
    );

    if ("https".equals(url.getProtocol())) {
      if (sslContext == null) {
        throw new IllegalStateException("No sslContext set, cannot do https");
      }

      final SSLEngine sslEngine = sslContext.createSSLEngine();
      sslEngine.setUseClientMode(true);
      final HandshakeRememberingSslHandler sslHandler = new HandshakeRememberingSslHandler(sslEngine);

      final ChannelPipeline pipeline = retVal.getChannel().getPipeline();
      pipeline.addFirst("ssl", sslHandler);

      retVal.addListener(
          new ChannelFutureListener()
          {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
              sslHandler.getHandshakeFutureOrHandshake();
            }
          }
      );
    }

    return retVal;
  }

  @Override
  public boolean isGood(ChannelFuture resource)
  {
    Channel channel = resource.awaitUninterruptibly().getChannel();

    boolean isConnected = channel.isConnected();
    boolean isOpen = channel.isOpen();

    boolean isHandshook = true;
    HandshakeRememberingSslHandler sslHandler = channel.getPipeline().get(HandshakeRememberingSslHandler.class);
    if (sslHandler != null) {
      try {
        sslHandler.getHandshakeFutureOrHandshake().await();
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    if (log.isTraceEnabled()) {
      log.trace(String.format("isGood = isConnected[%s] && isOpen[%s]", isConnected, isOpen));
    }

    return isConnected && isOpen && isHandshook;
  }

  @Override
  public void close(ChannelFuture resource)
  {
    log.trace("Closing");
    resource.awaitUninterruptibly().getChannel().close();
  }
}
