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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.metamx.http.client.pool.ResourceContainer;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class HttpClient
{
  private static final Logger log = Logger.getLogger(HttpClient.class);

  private final ResourcePool<String, Channel> pool;

  public HttpClient(
      ResourcePool<String, Channel> pool
  )
  {
    this.pool = pool;
  }

  public void stop()
  {
    pool.close();
  }

  public <Intermediate, Final> Future<Final> get(
      URL url,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    return get(url, ImmutableMultimap.<String, Object>of(), httpResponseHandler);
  }

  public <Intermediate, Final> Future<Final> get(
      URL url,
      ImmutableMultimap<String, Object> headers,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    return go(HttpMethod.GET, url, headers, null, httpResponseHandler);
  }

  public <Intermediate, Final> Future<Final> post(
      URL url,
      ChannelBuffer content,
      ImmutableMultimap<String, Object> headers,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    return go(
        HttpMethod.POST,
        url,
        ImmutableMultimap.<String, Object>builder()
                         .put(HttpHeaders.Names.CONTENT_LENGTH, content.writerIndex())
                         .putAll(headers)
                         .build(),
        content,
        httpResponseHandler
    );
  }


  public <Intermediate, Final> Future<Final> go(
      HttpMethod method,
      URL url,
      Multimap<String, Object> headers,
      ChannelBuffer content,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    final String hostKey = getPoolKey(url);
    final ResourceContainer<Channel> channelResourceContainer = pool.take(hostKey);
    final Channel channel = channelResourceContainer.get();

    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, url.toString());

    if (!headers.containsKey(HttpHeaders.Names.HOST)) {
      request.addHeader(HttpHeaders.Names.HOST, String.format("%s:%s", url.getHost(), url.getPort()));
    }

    for (Map.Entry<String, Collection<Object>> entry : headers.asMap().entrySet()) {
      String key = entry.getKey();

      for (Object obj : entry.getValue()) {
        request.addHeader(key, obj);
      }
    }

    if (content != null) {
      request.setContent(content);
    }

    final LatchedFuture<Final> retVal = new LatchedFuture<Final>();

    channel.getPipeline().addLast(
        "last",
        new SimpleChannelUpstreamHandler()
        {
          private volatile ClientResponse<Intermediate> response = null;

          @Override
          public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
          {
            try {
              Object msg = e.getMessage();

              if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;

                response = httpResponseHandler.handleResponse(httpResponse);
                if (response.isFinished()) {
                  retVal.set((Final) response.getObj());
                }

                if (!httpResponse.isChunked()) {
                  finishRequest();
                }
              } else if (msg instanceof HttpChunk) {
                HttpChunk httpChunk = (HttpChunk) msg;

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
              log.warn("Exception thrown while processing message, closing channel.", ex);

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
                      "Didn't get a completed ClientResponse Object from [%s]", httpResponseHandler.getClass()
                  )
              );
            }
            if (!retVal.isDone()) {
              retVal.set(finalResponse.getObj());
            }
            channel.getPipeline().removeLast();
            channelResourceContainer.returnResource();
          }
        }
    );

    channel.write(request);

    return retVal;
  }

  private String getPoolKey(URL url)
  {
    return String.format(
        "%s://%s:%s", url.getProtocol(), url.getHost(), url.getPort() == -1 ? url.getDefaultPort() : url.getPort()
    );
  }
}
