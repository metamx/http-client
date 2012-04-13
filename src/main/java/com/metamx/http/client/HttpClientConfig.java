package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

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

  public HttpClientConfig(
      int numConnections,
      SSLContext sslContext
  )
  {
    this.numConnections = numConnections;
    this.sslContext = sslContext;
  }

  public int getNumConnections()
  {
    return numConnections;
  }

  public SSLContext getSslContext()
  {
    return sslContext;
  }

  public static class Builder
  {
    private int numConnections = 1;
    private SSLContext sslContext = null;

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

    public HttpClientConfig build()
    {
      return new HttpClientConfig(numConnections, sslContext);
    }
  }
}
