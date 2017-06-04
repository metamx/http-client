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
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AsciiString;
import org.joda.time.Duration;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public class NettyHttpClient extends AbstractHttpClient
{
  private static final Logger log = new Logger(NettyHttpClient.class);

  private static final String READ_TIMEOUT_HANDLER_NAME = "read-timeout";
  private static final String LAST_HANDLER_NAME = "last-handler";

  private final ResourcePool<String, ChannelFuture> pool;
  private final HttpClientConfig.CompressionCodec compressionCodec;
  private final Duration defaultReadTimeout;

  public NettyHttpClient(ResourcePool<String, ChannelFuture> pool)
  {
    this(pool, null, HttpClientConfig.DEFAULT_COMPRESSION_CODEC);
  }

  NettyHttpClient(
      ResourcePool<String, ChannelFuture> pool,
      Duration defaultReadTimeout,
      HttpClientConfig.CompressionCodec compressionCodec
  )
  {
    this.pool = Preconditions.checkNotNull(pool, "pool");
    this.defaultReadTimeout = defaultReadTimeout;
    this.compressionCodec = Preconditions.checkNotNull(compressionCodec);
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
    return new NettyHttpClient(pool, readTimeout, compressionCodec);
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler,
      final Duration requestReadTimeout
  )
  {
    final HttpMethod method = request.getMethod();
    final URL url = request.getUrl();
    final Multimap<AsciiString, String> headers = request.getHeaders();

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
              channelFuture.cause()
          )
      );
    } else {
      channel = channelFuture.channel();
    }

    final String urlFile = Strings.nullToEmpty(url.getFile());
    String uri = urlFile.isEmpty() ? "/" : urlFile;
    final DefaultFullHttpRequest httpRequest =
        request.hasContent() ?
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, request.getContent()) :
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri);

    if (!headers.containsKey(HttpHeaderNames.HOST)) {
      httpRequest.headers().add(HttpHeaderNames.HOST, getHost(url));
    }

    // If Accept-Encoding is set in the Request, use that. Otherwise use the default from "compressionCodec".
    if (!headers.containsKey(HttpHeaderNames.ACCEPT_ENCODING)) {
      httpRequest.headers().set(HttpHeaderNames.ACCEPT_ENCODING, compressionCodec.getEncodingString());
    }

    for (Map.Entry<AsciiString, Collection<String>> entry : headers.asMap().entrySet()) {
      AsciiString key = entry.getKey();

      for (String obj : entry.getValue()) {
        httpRequest.headers().add(key, obj);
      }
    }

    final long readTimeout = getReadTimeout(requestReadTimeout);
    final SettableFuture<Final> retVal = SettableFuture.create();

    if (readTimeout > 0) {
      channel.pipeline().addLast(READ_TIMEOUT_HANDLER_NAME, new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS));
    }
    channel.pipeline().addLast(
        LAST_HANDLER_NAME,
        new SimpleChannelInboundHandler()
        {
          private volatile ClientResponse<Intermediate> response = null;

          @Override
          protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception
          {
            if (log.isDebugEnabled()) {
              log.debug("[%s] messageReceived: %s", requestDesc, msg);
            }
            try {
              if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;
                if (log.isDebugEnabled()) {
                  log.debug("[%s] Got response: %s", requestDesc, httpResponse.status());
                }

                response = handler.handleResponse(httpResponse);
                if (response.isFinished()) {
                  retVal.set((Final) response.getObj());
                }
              }
              if (msg instanceof HttpContent) {
                HttpContent httpChunk = (HttpContent) msg;
                boolean isLast = httpChunk instanceof LastHttpContent;
                if (log.isDebugEnabled()) {
                  log.debug(
                      "[%s] Got chunk: %sB, last=%s",
                      requestDesc,
                      httpChunk.content().readableBytes(),
                      isLast
                  );
                }
                response = handler.handleChunk(response, httpChunk);
                if (response.isFinished() && !retVal.isDone()) {
                  retVal.set((Final) response.getObj());
                }
                if (isLast) {
                  finishRequest();
                  ctx.close();
                }
              }
              if (!(msg instanceof HttpContent) && !(msg instanceof HttpResponse)) {
                throw new IllegalStateException(String.format("Unknown message type[%s]", msg.getClass()));
              }

            }
            catch (Exception ex) {
              log.warn(ex, "[%s] Exception thrown while processing message, closing channel.", requestDesc);
              if (!retVal.isDone()) {
                retVal.setException(ex);
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
          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
          {
            if (log.isDebugEnabled()) {
              if (cause == null) {
                log.debug("[%s] Caught exception", requestDesc);
              } else {
                log.debug(cause, "[%s] Caught exception", requestDesc);
              }
            }
            retVal.setException(cause);
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
              handler.exceptionCaught(response, cause);
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
            super.exceptionCaught(ctx, cause);
          }


          @Override
          public void channelInactive(ChannelHandlerContext ctx) throws Exception
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
            super.channelInactive(ctx);
          }

          private void removeHandlers()
          {
            if (readTimeout > 0) {
              channel.pipeline().remove(READ_TIMEOUT_HANDLER_NAME);
            }
            channel.pipeline().remove(LAST_HANDLER_NAME);
          }
        }
    );

    channel.writeAndFlush(httpRequest).addListener(
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
                        future.cause()
                    )
                );
              }
            }
          }
        }
    );

    return retVal;
  }

  private long getReadTimeout(Duration requestReadTimeout)
  {
    final long timeout;
    if (requestReadTimeout != null) {
      timeout = requestReadTimeout.getMillis();
    } else if (defaultReadTimeout != null) {
      timeout = defaultReadTimeout.getMillis();
    } else {
      timeout = 0;
    }
    return timeout;
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
