package com.metamx.http.client.auth;

import com.metamx.http.client.Request;

/**
 */
public interface Credentials
{
  public Request addCredentials(Request builder);
}
