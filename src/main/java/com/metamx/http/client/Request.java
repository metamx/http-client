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

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.AsciiString;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
public class Request
{
  private final HttpMethod method;
  private final URL url;
  private final Multimap<AsciiString, String> headers = Multimaps.newListMultimap(
      Maps.<AsciiString, Collection<String>>newHashMap(),
      new Supplier<List<String>>() {
        @Override
        public List<String> get() {
          return Lists.newArrayList();
        }
      }
  );

  private ByteBuf content;

  public Request(
      HttpMethod method,
      URL url
  )
  {
    this.method = method;
    this.url = url;
  }

  public HttpMethod getMethod()
  {
    return method;
  }

  public URL getUrl()
  {
    return url;
  }

  public Multimap<AsciiString, String> getHeaders()
  {
    return headers;
  }

  public boolean hasContent()
  {
    return content != null;
  }

  public ByteBuf getContent()
  {
    return content;
  }

  public Request copy() {
    Request retVal = new Request(method, url);
    retVal.headers.putAll(this.headers);
    retVal.content = content == null ? null : content.copy();
    return retVal;
  }

  public Request setHeader(AsciiString header, String value)
  {
    headers.replaceValues(header, Arrays.asList(value));
    return this;
  }

  public Request setHeaderValues(AsciiString header, Iterable<String> value)
  {
    headers.replaceValues(header, value);
    return this;
  }

  public Request setHeaderValues(Multimap<AsciiString, String> inHeaders) {
    for (Map.Entry<AsciiString, Collection<String>> entry : inHeaders.asMap().entrySet()) {
      this.setHeaderValues(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public Request addHeader(AsciiString header, String value)
  {
    headers.put(header, value);
    return this;
  }

  public Request addHeaderValues(AsciiString header, Iterable<String> value)
  {
    headers.putAll(header, value);
    return this;
  }

  public Request addHeaderValues(Multimap<AsciiString, String> inHeaders) {
    for (Map.Entry<AsciiString, Collection<String>> entry : inHeaders.asMap().entrySet()) {
      this.addHeaderValues(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public Request setContent(byte[] bytes)
  {
    return setContent(null, bytes);
  }

  public Request setContent(byte[] bytes, int offset, int length)
  {
    return setContent(null, bytes, offset, length);
  }

  public Request setContent(ByteBuf content)
  {
    return setContent(null, content);
  }

  public Request setContent(String contentType, byte[] bytes)
  {
    return setContent(contentType, bytes, 0, bytes.length);
  }

  public Request setContent(String contentType, byte[] bytes, int offset, int length)
  {
    return setContent(contentType, bytes, offset, length);
  }

  public Request setContent(String contentType, ByteBuf content)
  {
    if (contentType != null) {
      setHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
    }

    this.content = content;

    setHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(content.writerIndex()));

    return this;
  }

  public Request setBasicAuthentication(String username, String password)
  {
    final String base64Value = base64Encode(String.format("%s:%s", username, password));
    setHeader(HttpHeaderNames.AUTHORIZATION, String.format("Basic %s", base64Value));
    return this;
  }

  private String base64Encode(final String value)
  {
    return Base64
        .encode(Unpooled.wrappedBuffer(value.getBytes(Charsets.UTF_8)), false)
        .toString(Charsets.UTF_8);
  }
}