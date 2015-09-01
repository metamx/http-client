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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.IAE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.pool.ResourceContainer;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
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
import org.jboss.netty.util.Timer;
import org.joda.time.Duration;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public class NettyHttpClient implements HttpClient
{
  private static final Logger log = new Logger(NettyHttpClient.class);

  private static final String READ_TIMEOUT_HANDLER_NAME = "read-timeout";
  private static final String LAST_HANDLER_NAME = "last-handler";

  private final Timer timer;
  private final ResourcePool<String, ChannelFuture> pool;
  private final Duration readTimeout;

  public NettyHttpClient(
      ResourcePool<String, ChannelFuture> pool
  )
  {
    this(pool, null, null);
  }

  private NettyHttpClient(
      ResourcePool<String, ChannelFuture> pool,
      Duration readTimeout,
      Timer timer
  )
  {
    this.pool = Preconditions.checkNotNull(pool, "pool");
    this.readTimeout = readTimeout;
    this.timer = timer;

    if (hasTimeout()) {
      Preconditions.checkNotNull(timer, "timer");
    }
  }

  @LifecycleStart
  public void start()
  {
  }

  @LifecycleStop
  public void stop()
  {
    pool.close();
  }

  public HttpClient withReadTimeout(Duration readTimeout)
  {
    return new NettyHttpClient(pool, readTimeout, timer);
  }

  public NettyHttpClient withTimer(Timer timer)
  {
    return new NettyHttpClient(pool, readTimeout, timer);
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler
  )
  {
    final HttpMethod method = request.getMethod();
    final URL url = request.getUrl();
    final Multimap<String, String> headers = request.getHeaders();

    final String requestDesc = String.format("%s %s", method, url);
    if (log.isDebugEnabled()) {
      log.debug("[%s] starting", requestDesc);
    }

    // Block while acquiring a channel from the pool, then complete the request asynchronously.
    final Channel channel;
    final String hostKey = getPoolKey(url);
    final ResourceContainer<ChannelFuture> channelResourceContainer = pool.take(hostKey);
    final ChannelFuture channelFuture = channelResourceContainer.get().awaitUninterruptibly();
    if (!channelFuture.isSuccess()) {
      channelResourceContainer.returnResource(); // Some other poor sap will have to deal with it...
      return Futures.immediateFailedFuture(
          new ChannelException(
              "Faulty channel in resource pool",
              channelFuture.getCause()
          )
      );
    } else {
      channel = channelFuture.getChannel();
    }

    final String urlFile = Strings.nullToEmpty(url.getFile());
    final HttpRequest httpRequest = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1,
        method,
        urlFile.isEmpty() ? "/" : urlFile
    );

    if (!headers.containsKey(HttpHeaders.Names.HOST)) {
      httpRequest.headers().add(HttpHeaders.Names.HOST, getHost(url));
    }

    httpRequest.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    for (Map.Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
      String key = entry.getKey();

      for (String obj : entry.getValue()) {
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
              log.debug("[%s] messageReceived: %s", requestDesc, e.getMessage());
            }
            try {
              Object msg = e.getMessage();

              if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;
                if (log.isDebugEnabled()) {
                  log.debug("[%s] Got response: %s", requestDesc, httpResponse.getStatus());
                }

                response = handler.handleResponse(httpResponse);
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
                      "[%s] Got chunk: %sB, last=%s",
                      requestDesc,
                      httpChunk.getContent().readableBytes(),
                      httpChunk.isLast()
                  );
                }

                if (httpChunk.isLast()) {
                  finishRequest();
                } else {
                  response = handler.handleChunk(response, httpChunk);
                  if (response.isFinished() && !retVal.isDone()) {
                    retVal.set((Final) response.getObj());
                  }
                }
              } else {
                throw new IllegalStateException(String.format("Unknown message type[%s]", msg.getClass()));
              }
            }
            catch (Exception ex) {
              log.warn(ex, "[%s] Exception thrown while processing message, closing channel.", requestDesc);

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
            ClientResponse<Final> finalResponse = handler.done(response);
            if (!finalResponse.isFinished()) {
              throw new IllegalStateException(
                  String.format(
                      "[%s] Didn't get a completed ClientResponse Object from [%s]",
                      requestDesc,
                      handler.getClass()
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
              final Throwable cause = event.getCause();
              if (cause == null) {
                log.debug("[%s] Caught exception", requestDesc);
              } else {
                log.debug(cause, "[%s] Caught exception", requestDesc);
              }
            }

            retVal.setException(event.getCause());
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
              handler.exceptionCaught(response, event.getCause());
            }
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
              log.debug("[%s] Channel disconnected", requestDesc);
            }
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
              handler.exceptionCaught(response, new ChannelException("Channel disconnected"));
            }
            channel.close();
            channelResourceContainer.returnResource();
            if (!retVal.isDone()) {
              log.warn("[%s] Channel disconnected before response complete", requestDesc);
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

    channel.write(httpRequest).addListener(
        new ChannelFutureListener()
        {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception
          {
            if (!future.isSuccess()) {
              channel.close();
              channelResourceContainer.returnResource();
              if (!retVal.isDone()) {
                retVal.setException(
                    new ChannelException(
                        String.format("[%s] Failed to write request to channel", requestDesc),
                        future.getCause()
                    )
                );
              }
            }
          }
        }
    );

    return retVal;
  }

  private boolean hasTimeout()
  {
    return readTimeout != null && readTimeout.getMillis() > 0;
  }

  private String getHost(URL url)
  {
    int port = url.getPort();

    if (port == -1) {
      final String protocol = url.getProtocol();

      if ("http".equalsIgnoreCase(protocol)) {
        port = 80;
      } else if ("https".equalsIgnoreCase(protocol)) {
        port = 443;
      } else {
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
}
