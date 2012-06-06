package com.metamx.http.client;

import com.google.common.base.Throwables;

import java.util.concurrent.Future;

/**
 */
public class MockHttpClient extends HttpClient
{
  private volatile GoHandler goHandler;

  public MockHttpClient()
  {
    super(null);
  }

  public GoHandler getGoHandler()
  {
    return goHandler;
  }

  public void setGoHandler(GoHandler goHandler)
  {
    this.goHandler = goHandler;
  }

  @Override
  public <Intermediate, Final> Future<Final> go(
      Request<Intermediate, Final> request
  )
  {
    try {
      return goHandler.go(request);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
