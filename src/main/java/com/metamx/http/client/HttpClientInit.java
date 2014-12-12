package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.netty.HttpClientPipelineFactory;
import com.metamx.http.client.pool.ChannelResourceFactory;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.pool.ResourcePoolConfig;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.Timer;
import org.joda.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 */
public class HttpClientInit
{
  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    try {
      // We need to use the full constructor in order to set a ThreadNameDeterminer. The other parameters are taken
      // from the defaults in HashedWheelTimer's other constructors.
      final HashedWheelTimer timer = new HashedWheelTimer(
          new ThreadFactoryBuilder().setDaemon(true)
                                    .setNameFormat("HttpClient-Timer-%s")
                                    .build(),
          ThreadNameDeterminer.CURRENT,
          100,
          TimeUnit.MILLISECONDS,
          512
      );
      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              timer.start();
            }

            @Override
            public void stop()
            {
              timer.stop();
            }
          }
      );
      return lifecycle.addMaybeStartManagedInstance(
          new HttpClient(
              new ResourcePool<String, ChannelFuture>(
                  new ChannelResourceFactory(createBootstrap(lifecycle, timer), config.getSslContext(), timer, -1),
                  new ResourcePoolConfig(config.getNumConnections())
              )
          ).withTimer(timer).withReadTimeout(config.getReadTimeout())
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

  public static ClientBootstrap createBootstrap(Lifecycle lifecycle, Timer timer)
  {
    // Default from NioClientSocketChannelFactory.DEFAULT_BOSS_COUNT, which is private:
    final int bossCount = 1;

    // Default from SelectorUtil.DEFAULT_IO_THREADS, which is private:
    final int workerCount = Runtime.getRuntime().availableProcessors() * 2;

    final NioClientBossPool bossPool = new NioClientBossPool(
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HttpClient-Netty-Boss-%s")
                .build()
        ),
        bossCount,
        timer,
        ThreadNameDeterminer.CURRENT
    );

    final NioWorkerPool workerPool = new NioWorkerPool(
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HttpClient-Netty-Worker-%s")
                .build()
        ),
        workerCount,
        ThreadNameDeterminer.CURRENT
    );

    final ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossPool, workerPool));

    bootstrap.setOption("keepAlive", true);
    bootstrap.setPipelineFactory(new HttpClientPipelineFactory());

    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

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
              bootstrap.releaseExternalResources();
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return bootstrap;
  }

  @Deprecated
  public static ClientBootstrap createBootstrap(Lifecycle lifecycle)
  {
    return createBootstrap(lifecycle, new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).build()));
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
    catch (CertificateException e) {
      throw Throwables.propagate(e);
    }
    catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }
    catch (KeyStoreException e) {
      throw Throwables.propagate(e);
    }
    catch (KeyManagementException e) {
      throw Throwables.propagate(e);
    }
    catch (FileNotFoundException e) {
      throw Throwables.propagate(e);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    finally {
      Closeables.closeQuietly(in);
    }
  }
}
