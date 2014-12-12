package com.metamx.http.client;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JankyServersTest
{
  @Test
  public void testReadTimeoutsWithSilentServer() throws Throwable
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  InputStream in = clientSocket.getInputStream()
              ) {
                while (in.read() != -1) ;
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withReadTimeout(new Duration(100)).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> future = client
          .get(new URL(String.format("http://localhost:%d/", serverSocket.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8));

      Throwable e = null;
      try {
        future.get();
      } catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("ReadTimeoutException thrown by 'get'", e instanceof ReadTimeoutException);
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testSslWithConnectionClosingServer() throws Throwable
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  InputStream in = clientSocket.getInputStream()
              ) {
                in.read();
                clientSocket.close();
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      ChannelException e = null;
      try {
        client
            .get(new URL(String.format("https://localhost:%d/", serverSocket.getLocalPort())))
            .go(new StatusResponseHandler(Charsets.UTF_8));
      } catch (ChannelException e1) {
        e = e1;
      }

      Assert.assertTrue("ChannelException thrown by 'go'", e instanceof ChannelException);
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testSslWithEchoServer() throws Throwable
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  OutputStream out = clientSocket.getOutputStream();
                  InputStream in = clientSocket.getInputStream()
              ) {
                int b;
                while ((b = in.read()) != -1) {
                  out.write(b);
                }
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      ChannelException e = null;
      try {
        client
            .get(new URL(String.format("https://localhost:%d/", serverSocket.getLocalPort())))
            .go(new StatusResponseHandler(Charsets.UTF_8));
      } catch (ChannelException e1) {
        e = e1;
      }

      Assert.assertNotNull("ChannelException thrown by 'go'", e);
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  @Ignore
  public void testHttpBin() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      {
        final HttpResponseStatus status = client
            .get(new URL("https://httpbin.org/get"))
            .go(new StatusResponseHandler(Charsets.UTF_8))
            .get().getStatus();

        Assert.assertEquals(200, status.getCode());
      }

      {
        final HttpResponseStatus status = client
            .post(new URL("https://httpbin.org/post"))
            .setContent(new byte[]{'a', 'b', 'c', 1, 2, 3})
            .go(new StatusResponseHandler(Charsets.UTF_8))
            .get().getStatus();

        Assert.assertEquals(200, status.getCode());
      }
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testSelfSigned() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();

    Server server = new Server();

    ServerConnector connector = new ServerConnector(server);
    connector.setPort(9999);

    HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());

    final String keyStorePath = JankyServersTest.class.getClassLoader()
                                                      .getResource("keystore.jks")
                                                      .getFile();

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword("abc123");
    sslContextFactory.setKeyManagerPassword("abc123");

    ServerConnector sslConnector = new ServerConnector(
        server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https)
    );
    sslConnector.setPort(9998);

    server.setConnectors(new Connector[]{connector, sslConnector});

    server.start();

    try {
      {
        final SSLContext defaultSsl = SSLContext.getDefault();

        final HttpClientConfig config = HttpClientConfig.builder().withSslContext(defaultSsl).build();
        final HttpClient client = HttpClientInit.createClient(config, lifecycle);

        Exception e = null;
        try {
          client
              .get(new URL("https://localhost:9998/"))
              .go(new StatusResponseHandler(Charsets.UTF_8))
              .get().getStatus();
        }
        catch (ChannelException e1) {
          e = e1;
        }
        Assert.assertNotNull("ChannelException thrown by 'go'", e);
        Assert.assertTrue("Root cause is SSLHandshakeException", e.getCause().getCause() instanceof SSLHandshakeException);
      }

      {
        final SSLContext selfSignedSsl = HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, "abc123");

        final HttpClientConfig config = HttpClientConfig.builder().withSslContext(selfSignedSsl).build();
        final HttpClient client = HttpClientInit.createClient(config, lifecycle);


        final HttpResponseStatus status = client
            .get(new URL("https://localhost:9998/"))
            .go(new StatusResponseHandler(Charsets.UTF_8))
            .get().getStatus();

        Assert.assertEquals(404, status.getCode());
      }
    }
    finally {
      lifecycle.stop();
    }
  }
}
