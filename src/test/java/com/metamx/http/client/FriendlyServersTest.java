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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests with servers that are at least moderately well-behaving.
 */
public class FriendlyServersTest
{
  @Test
  public void testFriendlyHttpServer() throws Exception
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
                  BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                  OutputStream out = clientSocket.getOutputStream()
              ) {
                while (!in.readLine().equals("")) {
                  ;
                }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(Charsets.UTF_8));
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
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final StatusResponseHolder response = client
          .get(new URL(String.format("http://localhost:%d/", serverSocket.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8))
          .get();

      Assert.assertEquals(200, response.getStatus().getCode());
      Assert.assertEquals("hello!", response.getContent());
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testFriendlySelfSignedHttpsServer() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();
    final String keyStorePath = JankyServersTest.class.getClassLoader().getResource("keystore.jks").getFile();
    Server server = new Server();

    HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword("abc123");
    sslContextFactory.setKeyManagerPassword("abc123");

    ServerConnector sslConnector = new ServerConnector(
        server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https)
    );

    sslConnector.setPort(0);
    server.setConnectors(new Connector[]{sslConnector});
    server.start();

    try {
      final SSLContext mySsl = HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, "abc123");
      final HttpClientConfig trustingConfig = HttpClientConfig.builder().withSslContext(mySsl).build();
      final HttpClient trustingClient = HttpClientInit.createClient(trustingConfig, lifecycle);

      final HttpClientConfig skepticalConfig = HttpClientConfig.builder()
                                                               .withSslContext(SSLContext.getDefault())
                                                               .build();
      final HttpClient skepticalClient = HttpClientInit.createClient(skepticalConfig, lifecycle);

      // Correct name ("localhost")
      final HttpResponseStatus status = trustingClient
          .get(new URL(String.format("https://localhost:%d/", sslConnector.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8))
          .get().getStatus();
      Assert.assertEquals(404, status.getCode());

      // Incorrect name ("127.0.0.1")
      final ListenableFuture<StatusResponseHolder> response1 = trustingClient
          .get(new URL(String.format("https://127.0.0.1:%d/", sslConnector.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8));

      Throwable ea = null;
      try {
        response1.get();
      }
      catch (ExecutionException e) {
        ea = e.getCause();
      }

      Assert.assertTrue("ChannelException thrown by 'get'", ea instanceof ChannelException);
      Assert.assertTrue("Expected error message", ea.getCause().getMessage().matches(".*Failed to handshake.*"));

      // Untrusting client
      final ListenableFuture<StatusResponseHolder> response2 = skepticalClient
          .get(new URL(String.format("https://localhost:%d/", sslConnector.getLocalPort())))
          .go(new StatusResponseHandler(Charsets.UTF_8));

      Throwable eb = null;
      try {
        response1.get();
      }
      catch (ExecutionException e) {
        eb = e.getCause();
      }
      Assert.assertNotNull("ChannelException thrown by 'get'", eb);
      Assert.assertTrue(
          "Root cause is SSLHandshakeException",
          eb.getCause().getCause() instanceof SSLHandshakeException
      );
    }
    finally {
      lifecycle.stop();
      server.stop();
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
}
