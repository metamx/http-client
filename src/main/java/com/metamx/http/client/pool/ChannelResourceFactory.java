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

import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 */
public class ChannelResourceFactory implements ResourceFactory<String, ChannelFuture>
{
  private static final Logger log = new Logger(ChannelResourceFactory.class);

  private static final long DEFAULT_SSL_HANDSHAKE_TIMEOUT = 10000L; /* 10 seconds */

  private final Bootstrap bootstrap;
  private final SSLContext sslContext;
  private final long sslHandshakeTimeout;

  public ChannelResourceFactory(
      Bootstrap bootstrap,
      SSLContext sslContext,
      long sslHandshakeTimeout
  )
  {
    this.bootstrap = Preconditions.checkNotNull(bootstrap, "bootstrap");
    this.sslContext = sslContext;
    this.sslHandshakeTimeout = sslHandshakeTimeout >= 0 ? sslHandshakeTimeout : DEFAULT_SSL_HANDSHAKE_TIMEOUT;
  }

  @Override
  public ChannelFuture generate(final String hostname)
  {
    log.info("Generating: %s", hostname);
    URL url = null;
    try {
      url = new URL(hostname);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    final String host = url.getHost();
    final int port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
    final ChannelFuture retVal;
    final ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));

    if ("https".equals(url.getProtocol())) {
      if (sslContext == null) {
        throw new IllegalStateException("No sslContext set, cannot do https");
      }

      final SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
      final SSLParameters sslParameters = new SSLParameters();
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
      sslEngine.setSSLParameters(sslParameters);
      sslEngine.setUseClientMode(true);
      final SslHandler sslHandler = new SslHandler(sslEngine);
      sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeout);

      final ChannelPipeline pipeline = connectFuture.channel().pipeline();
      pipeline.addFirst("ssl", sslHandler);

      final ChannelPromise handshakeFuture = connectFuture.channel().newPromise();
      connectFuture.addListener(
          new ChannelFutureListener()
          {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception
            {
              if (f.isSuccess()) {
                sslHandler.handshakeFuture().addListener(
                    new GenericFutureListener<Future<Channel>>()
                    {
                      @Override
                      public void operationComplete(Future<Channel> f2) throws Exception
                      {
                        if (f2.isSuccess()) {
                          handshakeFuture.setSuccess();
                        } else {
                          handshakeFuture.setFailure(
                              new ChannelException(
                                  String.format("Failed to handshake with host[%s]", hostname),
                                  f2.cause()
                              )
                          );
                        }
                      }
                    }
                );
              } else {
                handshakeFuture.setFailure(
                    new ChannelException(
                        String.format("Failed to connect to host[%s]", hostname),
                        f.cause()
                    )
                );
              }
            }
          }
      );

      retVal = handshakeFuture;
    } else {
      retVal = connectFuture;
    }

    return retVal;
  }

  @Override
  public boolean isGood(ChannelFuture resource)
  {
    Channel channel = resource.awaitUninterruptibly().channel();

    boolean isSuccess = resource.isSuccess();
    boolean isOpen = channel.isOpen();

    if (log.isTraceEnabled()) {
      log.trace("isGood = isSucess[%s] && isOpen[%s]", isSuccess, isOpen);
    }

    return isSuccess && isOpen;
  }

  @Override
  public void close(ChannelFuture resource)
  {
    log.trace("Closing");
    resource.awaitUninterruptibly().channel().close();
  }
}
