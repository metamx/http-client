package com.metamx.http.client;

import com.google.common.util.concurrent.Futures;
import com.metamx.common.ISE;

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
      public <Intermediate, Final> Future<Final> go(Request<Intermediate, Final> request) throws Exception
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
      public <Intermediate, Final> Future<Final> go(Request<Intermediate, Final> request) throws Exception
      {
        return Futures.immediateFuture((Final) retVal);
      }
    };
  }
}
