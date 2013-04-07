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
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
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

/**
 */
public class HttpClientInit
{
  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    return lifecycle.addManagedInstance(
        new HttpClient(
            new ResourcePool<String, ChannelFuture>(
                new ChannelResourceFactory(createBootstrap(lifecycle), config.getSslContext()),
                new ResourcePoolConfig(config.getNumConnections())
            )
        ).withReadTimeout(config.getReadTimeout())
    );
  }

  @Deprecated
  public static HttpClient createClient(ResourcePoolConfig config, final SSLContext sslContext, Lifecycle lifecycle)
  {
    return createClient(
        new HttpClientConfig(config.getMaxPerKey(), sslContext, Duration.ZERO),
        lifecycle
    );
  }

  public static ClientBootstrap createBootstrap(Lifecycle lifecycle)
  {
    final ClientBootstrap bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("Netty-Boss-%s")
                    .build()
            ),
            Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("Netty-Worker-%s")
                    .build()
            )
        )
    );
    bootstrap.setPipelineFactory(new HttpClientPipelineFactory());

    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    lifecycle.addHandler(
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

    return bootstrap;
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
