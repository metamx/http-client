package com.metamx.http.client.auth;

import com.metamx.http.client.Request;

/**
 */
public class BasicCredentials implements Credentials
{
  private final String username;
  private final String password;

  public BasicCredentials(
      String username,
      String password
  )
  {
    this.username = username;
    this.password = password;
  }

  @Override
  public Request addCredentials(Request builder)
  {
    return builder.setBasicAuthentication(username, password);
  }
}
