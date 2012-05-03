package com.metamx.http.client;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.metamx.http.client.response.HttpResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.handler.codec.base64.Base64;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

/**
 */
public class RequestBuilder
{
  private static final ChannelBufferFactory factory = HeapChannelBufferFactory.getInstance();

  private final HttpClient client;
  private final HttpMethod method;
  private final URL url;

  private final Multimap<String, Object> headers = Multimaps.newListMultimap(
      Maps.<String, Collection<Object>>newHashMap(),
      new Supplier<List<Object>>()
      {
        @Override
        public List<Object> get()
        {
          return Lists.newArrayList();
        }
      }
  );

  private volatile ChannelBuffer content = null;

  public RequestBuilder(
      HttpClient client,
      HttpMethod method,
      URL url
  )
  {
    this.client = client;
    this.method = method;
    this.url = url;
  }

  public RequestBuilder setHeader(String header, Object value)
  {
    headers.replaceValues(header, Arrays.asList(value));
    return this;
  }

  public RequestBuilder setHeaderValues(String header, Iterable<Object> value)
  {
    headers.replaceValues(header, value);
    return this;
  }

  public RequestBuilder addHeader(String header, Object value)
  {
    headers.put(header, value);
    return this;
  }

  public RequestBuilder addHeaderValues(String header, Iterable<Object> value)
  {
    headers.putAll(header, value);
    return this;
  }

  public RequestBuilder setContent(byte[] bytes)
  {
    return setContent(null, bytes);
  }

  public RequestBuilder setContent(byte[] bytes, int offset, int length)
  {
    return setContent(null, bytes, offset, length);
  }

  public RequestBuilder setContent(ChannelBuffer content)
  {
    return setContent(null, content);
  }

  public RequestBuilder setContent(String contentType, byte[] bytes)
  {
    return setContent(contentType, bytes, 0, bytes.length);
  }

  public RequestBuilder setContent(String contentType, byte[] bytes, int offset, int length)
  {
    return setContent(contentType, factory.getBuffer(bytes, offset, length));
  }

  public RequestBuilder setContent(String contentType, ChannelBuffer content)
  {
    if (contentType != null) {
      addHeader(HttpHeaders.Names.CONTENT_TYPE, contentType);
    }

    this.content = content;

    setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.writerIndex());

    return this;
  }

  public RequestBuilder setBasicAuthentication(String username, String password)
  {
    final String base64Value = base64Encode(String.format("%s:%s", username, password));
    setHeader(HttpHeaders.Names.AUTHORIZATION, String.format("Basic %s", base64Value));
    return this;
  }

  private String base64Encode(final String value)
  {
    final ChannelBufferFactory bufferFactory = HeapChannelBufferFactory.getInstance();

    return Base64
        .encode(bufferFactory.getBuffer(ByteBuffer.wrap(value.getBytes(Charsets.UTF_8))))
        .toString(Charsets.UTF_8);
  }

  public <IntermediateType, Final> Future<Final> go(
      HttpResponseHandler<IntermediateType, Final> responseHandler
  )
  {
    return client.go(method, url, headers, content, responseHandler);
  }
}
