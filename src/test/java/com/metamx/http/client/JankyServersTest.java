package com.metamx.http.client;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
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

      Assert.assertTrue("ChannelException thrown by 'go'", e instanceof ChannelException);
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }
}
