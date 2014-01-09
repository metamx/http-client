package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

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

  public boolean succeeded()
  {
    return goHandler.succeeded();
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request<Intermediate, Final> request
  )
  {
    try {
      return goHandler.run(request);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
