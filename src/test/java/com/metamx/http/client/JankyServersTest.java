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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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

/**
 * Tests with a bunch of goofy not-actually-http servers.
 */
public class JankyServersTest
{
  static ExecutorService exec;
  static ServerSocket silentServerSocket;
  static ServerSocket echoServerSocket;
  static ServerSocket closingServerSocket;

  @BeforeClass
  public static void setUp() throws Exception
  {
    exec = Executors.newCachedThreadPool();

    silentServerSocket = new ServerSocket(0);
    echoServerSocket = new ServerSocket(0);
    closingServerSocket = new ServerSocket(0);

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = silentServerSocket.accept();
                  InputStream in = clientSocket.getInputStream()
              ) {
                while (in.read() != -1) {
                }
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = closingServerSocket.accept();
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

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = echoServerSocket.accept();
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
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    exec.shutdownNow();
    silentServerSocket.close();
    echoServerSocket.close();
    closingServerSocket.close();
  }

  @Test
  public void testHttpSilentServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withReadTimeout(new Duration(100)).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> future = client
          .get(new URL(String.format("http://localhost:%d/", silentServerSocket.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8));

      Throwable e = null;
      try {
        future.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("ReadTimeoutException thrown by 'get'", e instanceof ReadTimeoutException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpsSilentServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder()
                                                      .withSslContext(SSLContext.getDefault())
                                                      .withSslHandshakeTimeout(new Duration(100))
                                                      .build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      Throwable e = null;
      try {
        client
            .get(new URL(String.format("https://localhost:%d/", silentServerSocket.getLocalPort())))
            .go(new StatusResponseHandler(Charsets.UTF_8));
      }
      catch (ChannelException e1) {
        e = e1;
      }

      Assert.assertTrue("ChannelException thrown by 'go'", e instanceof ChannelException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpConnectionClosingServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> response = client
          .get(new URL(String.format("http://localhost:%d/", closingServerSocket.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8));

      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("ChannelException thrown by 'get'", e instanceof ChannelException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpsConnectionClosingServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      ChannelException e = null;
      try {
        client
            .get(new URL(String.format("https://localhost:%d/", closingServerSocket.getLocalPort())))
            .go(new StatusResponseHandler(Charsets.UTF_8));
      }
      catch (ChannelException e1) {
        e = e1;
      }

      Assert.assertTrue("ChannelException thrown by 'go'", e instanceof ChannelException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpEchoServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> response = client
          .get(new URL(String.format("http://localhost:%d/", echoServerSocket.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8));

      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("IllegalArgumentException thrown by 'get'", e instanceof IllegalArgumentException);
      Assert.assertTrue("Expected error message", e.getMessage().matches(".*invalid version format:.*"));
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpsEchoServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      ChannelException e = null;
      try {
        client
            .get(new URL(String.format("https://localhost:%d/", echoServerSocket.getLocalPort())))
            .go(new StatusResponseHandler(Charsets.UTF_8));
      }
      catch (ChannelException e1) {
        e = e1;
      }

      Assert.assertNotNull("ChannelException thrown by 'go'", e);
    }
    finally {
      lifecycle.stop();
    }
  }
}
