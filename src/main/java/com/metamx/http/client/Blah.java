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
import com.metamx.http.client.pool.ChannelResourceFactory;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.pool.ResourcePoolConfig;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 */
public class Blah
{
  public static void main(String[] args) throws MalformedURLException, ExecutionException, InterruptedException
  {
    ChannelBufferFactory bufferFactory = HeapChannelBufferFactory.getInstance();
    ClientBootstrap bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()
        )
    );

    bootstrap.setPipelineFactory(new HttpClientPipelineFactory());

    HttpClient client = new HttpClient(
        new ResourcePool<String, Channel>(
            new ChannelResourceFactory(bootstrap),
            new ResourcePoolConfig(20, false)
        )
    );

    URL url = new URL("http://0.0.0.0:17071/billy");

    System.out.println(1);
    System.out.println("gotten: " + client.get(url, new ObjectHttpResponseHandler()).get());

    for (int i = 0; i < 10; ++i) {
      System.out.println("========================");
    }
    System.out.println(2);
    System.out.println("gotten: " + client.get(url, new ObjectHttpResponseHandler()).get());

    for (int i = 0; i < 10; ++i) {
      System.out.println("========================");
    }
    System.out.println(
        "gotten: " + client.post(
            url,
            bufferFactory.getBuffer(ByteBuffer.wrap("1234".getBytes())),
            ImmutableMultimap.<String, Object>of(
                HttpHeaders.Names.CONTENT_TYPE, "text/plain"
            ),
            new ObjectHttpResponseHandler()
        ).get()
    );

    for (int i = 0; i < 10; ++i) {
      System.out.println("========================");
    }
    byte[] blah = "abcdefghijklmnopqrstuvwxyz".getBytes();
    byte[] bytes = new byte[256 * 1024];
    for (int i = 0; i < bytes.length; ++i) {
      int charIndex = i / (bytes.length / (blah.length - 1));
      bytes[i] = blah[charIndex];
    }
    System.out.println(
        "gotten: " + client.post(
            url,
            bufferFactory.getBuffer(ByteBuffer.wrap(bytes)),
            ImmutableMultimap.<String, Object>of(
                HttpHeaders.Names.CONTENT_TYPE, "text/plain"
            ),
            new ObjectHttpResponseHandler()
        ).get()
    );

    client.stop();

    bootstrap.releaseExternalResources();
  }

  private static class ObjectHttpResponseHandler implements HttpResponseHandler<Object, Object>
  {
    @Override
    public ClientResponse<Object> handleResponse(HttpResponse response)
    {
      System.out.println("HttpResponse");
      System.out.println(response.getStatus().toString());
      System.out.println(response.getContent().toString(CharsetUtil.UTF_8));

      return ClientResponse.unfinished(null);
    }

    @Override
    public ClientResponse<Object> handleChunk(ClientResponse<Object> clientResponse, HttpChunk chunk)
    {
      System.out.println("HttpChunk");
      System.out.println(chunk.getContent().toString(CharsetUtil.UTF_8));

      return ClientResponse.unfinished(null);
    }

    @Override
    public ClientResponse<Object> done(ClientResponse<Object> clientResponse)
    {
      System.out.println("Done");

      return ClientResponse.finished(null);
    }
  }
}
