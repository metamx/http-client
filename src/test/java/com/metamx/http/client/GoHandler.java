package com.metamx.http.client;

import com.metamx.common.ISE;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
*/
public abstract class GoHandler
{
  public abstract <Intermediate, Final> Future<Final> go(Request<Intermediate, Final> request) throws Exception;

  public GoHandler times(final int n)
  {
    final GoHandler myself = this;

    return new GoHandler()
    {
      AtomicInteger counter = new AtomicInteger(0);

      @Override
      public <Intermediate, Final> Future<Final> go(Request<Intermediate, Final> request) throws Exception
      {
        if (counter.getAndIncrement() < n) {
          return myself.go(request);
        }
        throw new ISE("Called more than %d times", n);
      }
    };
  }
}
