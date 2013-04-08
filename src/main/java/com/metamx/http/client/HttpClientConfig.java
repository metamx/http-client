package com.metamx.http.client;

import org.joda.time.Duration;

import javax.net.ssl.SSLContext;

/**
 */
public class HttpClientConfig
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final int numConnections;
  private final SSLContext sslContext;
  private final Duration readTimeout;

  @Deprecated
  public HttpClientConfig(
      int numConnections,
      SSLContext sslContext
  )
  {
    this(numConnections, sslContext, Duration.ZERO);
  }

  public HttpClientConfig(
      int numConnections,
      SSLContext sslContext,
      Duration readTimeout
  )
  {
    this.numConnections = numConnections;
    this.sslContext = sslContext;
    this.readTimeout = readTimeout;
  }

  public int getNumConnections()
  {
    return numConnections;
  }

  public SSLContext getSslContext()
  {
    return sslContext;
  }

  public Duration getReadTimeout()
  {
    return readTimeout;
  }

  public static class Builder
  {
    private int numConnections = 1;
    private SSLContext sslContext = null;
    private Duration readTimeout = null;

    private Builder(){}

    public Builder withNumConnections(int numConnections)
    {
      this.numConnections = numConnections;
      return this;
    }

    public Builder withSslContext(SSLContext sslContext)
    {
      this.sslContext = sslContext;
      return this;
    }

    public Builder withSslContext(String keyStorePath, String keyStorePassword)
    {
      this.sslContext = HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, keyStorePassword);
      return this;
    }

    public Builder withReadTimeout(Duration readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public HttpClientConfig build()
    {
      return new HttpClientConfig(numConnections, sslContext, readTimeout);
    }
  }
}
