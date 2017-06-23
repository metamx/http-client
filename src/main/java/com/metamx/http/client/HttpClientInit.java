/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.netty.HttpClientPipelineFactory;
import com.metamx.http.client.pool.ChannelResourceFactory;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.pool.ResourcePoolConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import org.joda.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executors;

/**
 */
public class HttpClientInit
{
  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    try {
      return lifecycle.addMaybeStartManagedInstance(
          new NettyHttpClient(
              new ResourcePool<>(
                  new ChannelResourceFactory(
                      createBootstrap(lifecycle, config.getBossPoolSize(), config.getWorkerPoolSize()),
                      config.getSslContext(),
                      config.getSslHandshakeTimeout() == null ? -1 : config.getSslHandshakeTimeout().getMillis()
                  ),
                  new ResourcePoolConfig(config.getNumConnections())
              ),
              config.getReadTimeout(),
              config.getCompressionCodec()
          )
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Deprecated
  public static HttpClient createClient(ResourcePoolConfig config, final SSLContext sslContext, Lifecycle lifecycle)
  {
    return createClient(
        new HttpClientConfig(config.getMaxPerKey(), sslContext, Duration.ZERO),
        lifecycle
    );
  }

  @Deprecated // use createClient directly
  public static Bootstrap createBootstrap(Lifecycle lifecycle)
  {
    final HttpClientConfig defaultConfig = HttpClientConfig.builder().build();
    return createBootstrap(lifecycle, defaultConfig.getBossPoolSize(), defaultConfig.getWorkerPoolSize());
  }

  public static SSLContext sslContextWithTrustedKeyStore(final String keyStorePath, final String keyStorePassword)
  {
    FileInputStream in = null;
    try {
      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

      in = new FileInputStream(keyStorePath);
      ks.load(in, keyStorePassword.toCharArray());
      in.close();

      final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      final SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);

      return sslContext;
    }
    catch (CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException | IOException e) {
      throw Throwables.propagate(e);
    }
    finally {
      CloseQuietly.close(in);
    }
  }

  private static Bootstrap createBootstrap(Lifecycle lifecycle, int bossPoolSize, int workerPoolSize)
  {
    final NioEventLoopGroup group = new NioEventLoopGroup(
        bossPoolSize,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HttpClient-Netty-Client-%s")
                .build()
        )
    );

    final Bootstrap bootstrap = new Bootstrap()
        .group(group)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new HttpClientPipelineFactory());

    InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE);

    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
            }

            @Override
            public void stop()
            {
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return bootstrap;
  }
}
