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

package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.IAE;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.http.client.auth.Credentials;
import com.metamx.http.client.netty.HandshakeRememberingSslHandler;
import com.metamx.http.client.pool.ResourceContainer;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.pool.ResourcePoolConfig;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.joda.time.Duration;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public class HttpClient
{
  private static final Logger log = Logger.getLogger(HttpClient.class);

  private static final String READ_TIMEOUT_HANDLER_NAME = "read-timeout";
  private static final String LAST_HANDLER_NAME = "last-handler";

  private final Timer timer;
  private final ResourcePool<String, ChannelFuture> pool;
  private final boolean enforceSSL;
  private final Credentials credentials;
  private final Duration readTimeout;

  public HttpClient(
      ResourcePool<String, ChannelFuture> pool
  )
  {
    this(pool, false, null, null);
  }

  private HttpClient(
      ResourcePool<String, ChannelFuture> pool,
      boolean enforceSSL,
      Credentials credentials,
      Duration readTimeout
  )
  {
    this.pool = pool;
    this.enforceSSL = enforceSSL;
    this.credentials = credentials;
    this.readTimeout = readTimeout;

    if (readTimeout != null && readTimeout.getMillis() > 0) {
      this.timer = new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).build());
    } else {
      this.timer = null;
    }
  }

  @LifecycleStart
  public void start()
  {
  }

  @LifecycleStop
  public void stop()
  {
    if (hasTimeout()) {
      timer.stop();
    }

    pool.close();
  }

  public HttpClient secureClient()
  {
    return new HttpClient(pool, true, credentials, readTimeout);
  }

  public HttpClient withCredentials(Credentials credentials)
  {
    return new HttpClient(pool, enforceSSL, credentials, readTimeout);
  }

  public HttpClient withReadTimeout(Duration readTimeout)
  {
    return new HttpClient(pool, enforceSSL, credentials, readTimeout);
  }

  public <Intermediate, Final> ListenableFuture<Final> get(
      URL url,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    return get(url).go(httpResponseHandler);
  }

  public <Intermediate, Final> ListenableFuture<Final> get(
      URL url,
      ImmutableMultimap<String, Object> headers,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    RequestBuilder builder = get(url);

    for (Map.Entry<String, Collection<Object>> entry : headers.asMap().entrySet()) {
      builder.setHeaderValues(entry.getKey(), entry.getValue());
    }

    return builder.go(httpResponseHandler);
  }

  public <Intermediate, Final> ListenableFuture<Final> post(
      URL url,
      ChannelBuffer content,
      ImmutableMultimap<String, Object> headers,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    RequestBuilder builder = post(url);

    builder.setContent(content);

    for (Map.Entry<String, Collection<Object>> entry : headers.asMap().entrySet()) {
      builder.setHeaderValues(entry.getKey(), entry.getValue());
    }

    return builder.go(httpResponseHandler);
  }

  public RequestBuilder get(URL url)
  {
    return makeBuilder(HttpMethod.GET, url);
  }

  public RequestBuilder post(URL url)
  {
    return makeBuilder(HttpMethod.POST, url);
  }

  public RequestBuilder put(URL url)
  {
    return makeBuilder(HttpMethod.PUT, url);
  }

  private RequestBuilder makeBuilder(final HttpMethod method, URL url)
  {
    if (enforceSSL && !"https".equals(url.getProtocol())) {
      throw new IllegalArgumentException(String.format("Requests must be over https, got[%s].", url));
    }

    final RequestBuilder builder = new RequestBuilder(this, method, url);

    if (credentials != null) {
      credentials.addCredentials(builder);
    }

    return builder;
  }

  public <Intermediate, Final> ListenableFuture<Final> go(
      Request<Intermediate, Final> request
  )
  {
    final HttpMethod method = request.getMethod();
    final URL url = request.getUrl();
    final Multimap<String, Object> headers = request.getHeaders();
    final HttpResponseHandler<Intermediate, Final> httpResponseHandler = request.getHandler();

    final String requestDesc = String.format("%s %s", method, url);
    if (log.isDebugEnabled()) {
      log.debug(String.format("[%s] starting", requestDesc));
    }
    final String hostKey = getPoolKey(url);
    final ResourceContainer<ChannelFuture> channelResourceContainer = pool.take(hostKey);
    final Channel channel = channelResourceContainer.get().awaitUninterruptibly().getChannel();

    HandshakeRememberingSslHandler sslHandler = channel.getPipeline().get(HandshakeRememberingSslHandler.class);
    if (sslHandler != null) {
      try {
        sslHandler.getHandshakeFutureOrHandshake().await();
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, url.getFile());

    if (!headers.containsKey(HttpHeaders.Names.HOST)) {
      httpRequest.headers().add(HttpHeaders.Names.HOST, getHost(url));
    }

    httpRequest.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    for (Map.Entry<String, Collection<Object>> entry : headers.asMap().entrySet()) {
      String key = entry.getKey();

      for (Object obj : entry.getValue()) {
        httpRequest.headers().add(key, obj);
      }
    }

    if (request.hasContent()) {
      httpRequest.setContent(request.getContent());
    }

    final SettableFuture<Final> retVal = SettableFuture.create();

    if (hasTimeout()) {
      channel.getPipeline().addLast(
          READ_TIMEOUT_HANDLER_NAME,
          new ReadTimeoutHandler(timer, readTimeout.getMillis(), TimeUnit.MILLISECONDS)
      );
    }

    channel.getPipeline().addLast(
        LAST_HANDLER_NAME,
        new SimpleChannelUpstreamHandler()
        {
          private volatile ClientResponse<Intermediate> response = null;

          @Override
          public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
          {
            if (log.isDebugEnabled()) {
              log.debug(String.format("[%s] messageReceived: %s", requestDesc, e.getMessage()));
            }
            try {
              Object msg = e.getMessage();

              if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;
                if (log.isDebugEnabled()) {
                  log.debug(String.format("[%s] Got response: %s", requestDesc, httpResponse.getStatus()));
                }

                response = httpResponseHandler.handleResponse(httpResponse);
                if (response.isFinished()) {
                  retVal.set((Final) response.getObj());
                }

                if (!httpResponse.isChunked()) {
                  finishRequest();
                }
              } else if (msg instanceof HttpChunk) {
                HttpChunk httpChunk = (HttpChunk) msg;
                if (log.isDebugEnabled()) {
                  log.debug(
                      String.format(
                          "[%s] Got chunk: %sB, last=%s",
                          requestDesc,
                          httpChunk.getContent().readableBytes(),
                          httpChunk.isLast()
                      )
                  );
                }

                if (httpChunk.isLast()) {
                  finishRequest();
                } else {
                  response = httpResponseHandler.handleChunk(response, httpChunk);
                  if (response.isFinished() && !retVal.isDone()) {
                    retVal.set((Final) response.getObj());
                  }
                }
              } else {
                throw new IllegalStateException(String.format("Unknown message type[%s]", msg.getClass()));
              }
            }
            catch (Exception ex) {
              log.warn(
                  String.format("[%s] Exception thrown while processing message, closing channel.", requestDesc), ex
              );

              if (!retVal.isDone()) {
                retVal.set(null);
              }
              channel.close();
              channelResourceContainer.returnResource();

              throw ex;
            }
          }

          private void finishRequest()
          {
            ClientResponse<Final> finalResponse = httpResponseHandler.done(response);
            if (!finalResponse.isFinished()) {
              throw new IllegalStateException(
                  String.format(
                      "[%s] Didn't get a completed ClientResponse Object from [%s]",
                      requestDesc,
                      httpResponseHandler.getClass()
                  )
              );
            }
            if (!retVal.isDone()) {
              retVal.set(finalResponse.getObj());
            }
            removeHandlers();
            channelResourceContainer.returnResource();
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception
          {
            if (log.isDebugEnabled()) {
              log.debug(String.format("[%s] Caught exception", requestDesc), event.getCause());
            }

            retVal.setException(event.getCause());
            removeHandlers();
            try {
              channel.close();
            }
            catch (Exception e) {
              // ignore
            }
            finally {
              channelResourceContainer.returnResource();
            }

            context.sendUpstream(event);
          }

          @Override
          public void channelDisconnected(ChannelHandlerContext context, ChannelStateEvent event) throws Exception
          {
            if (log.isDebugEnabled()) {
              log.debug(String.format("[%s] Channel disconnected", requestDesc));
            }

            channel.close();
            channelResourceContainer.returnResource();
            if (!retVal.isDone()) {
              log.warn(String.format("[%s] Channel disconnected before response complete", requestDesc));
              retVal.setException(new ChannelException("Channel disconnected"));
            }
            context.sendUpstream(event);
          }

          private void removeHandlers()
          {
            if (hasTimeout()) {
              channel.getPipeline().remove(READ_TIMEOUT_HANDLER_NAME);
            }
            channel.getPipeline().remove(LAST_HANDLER_NAME);
          }
        }
    );

    channel.write(httpRequest);

    return retVal;
  }

  private boolean hasTimeout()
  {
    return timer != null;
  }

  private String getHost(URL url) {
    int port = url.getPort();

    if (port == -1) {
      final String protocol = url.getProtocol();

      if ("http".equalsIgnoreCase(protocol)) {
        port = 80;
      }
      else if ("https".equalsIgnoreCase(protocol)) {
        port = 443;
      }
      else {
        throw new IAE("Cannot figure out default port for protocol[%s], please set Host header.", protocol);
      }
    }

    return String.format("%s:%s", url.getHost(), port);
  }

  private String getPoolKey(URL url)
  {
    return String.format(
        "%s://%s:%s", url.getProtocol(), url.getHost(), url.getPort() == -1 ? url.getDefaultPort() : url.getPort()
    );
  }

  /**
   * Use HttpClientInit instead *
   */
  @Deprecated
  public static HttpClient create(ResourcePoolConfig config, Lifecycle lifecycle)
  {
    return HttpClientInit.createClient(
        HttpClientConfig.builder().withNumConnections(config.getMaxPerKey()).build(),
        lifecycle
    );
  }

  /**
   * Use HttpClientInit instead *
   */
  @Deprecated
  public static ClientBootstrap createBootstrap(Lifecycle lifecycle)
  {
    return HttpClientInit.createBootstrap(lifecycle);
  }
}
