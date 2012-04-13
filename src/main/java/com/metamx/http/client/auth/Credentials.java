package com.metamx.http.client.auth;

import com.metamx.http.client.RequestBuilder;

/**
 */
public interface Credentials
{
  public RequestBuilder addCredentials(RequestBuilder builder);
}
