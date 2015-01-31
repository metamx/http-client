package com.metamx.http.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.ISE;
import com.metamx.http.client.response.HttpResponseHandler;

import java.util.concurrent.Future;

/**
 */
public class GoHandlers
{
  public static GoHandler failingHandler()
  {
    return new GoHandler()
    {
      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler) throws Exception
      {
        throw new ISE("Shouldn't be called");
      }
    };
  }

  public static GoHandler passingHandler(final Object retVal)
  {
    return new GoHandler()
    {
      @SuppressWarnings("unchecked")
      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler) throws Exception
      {
        return Futures.immediateFuture((Final) retVal);
      }
    };
  }
}
