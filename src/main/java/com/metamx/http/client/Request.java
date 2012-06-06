package com.metamx.http.client;

import com.google.common.collect.Multimap;
import com.metamx.http.client.response.HttpResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.net.URL;

/**
 */
public class Request<Intermediate, Final>
{
  private final HttpMethod method;
  private final URL url;
  private final Multimap<String, Object> headers;
  private final ChannelBuffer content;
  private final HttpResponseHandler<Intermediate, Final> handler;

  public Request(
      HttpMethod method,
      URL url,
      Multimap<String, Object> headers,
      ChannelBuffer content,
      HttpResponseHandler<Intermediate, Final> handler
  )
  {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.content = content;
    this.handler = handler;
  }

  public HttpMethod getMethod()
  {
    return method;
  }

  public URL getUrl()
  {
    return url;
  }

  public Multimap<String, Object> getHeaders()
  {
    return headers;
  }

  public boolean hasContent()
  {
    return content != null;
  }

  public ChannelBuffer getContent()
  {
    return content;
  }

  public HttpResponseHandler<Intermediate, Final> getHandler()
  {
    return handler;
  }
}
