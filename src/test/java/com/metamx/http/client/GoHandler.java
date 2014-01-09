package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.ISE;

import java.util.concurrent.atomic.AtomicInteger;

/**
*/
public abstract class GoHandler
{
  /******* Abstract Methods *********/
  protected abstract <Intermediate, Final> ListenableFuture<Final> go(Request<Intermediate, Final> request) throws Exception;

  /******* Non Abstract Methods ********/
  private volatile boolean succeeded = false;

  public boolean succeeded()
  {
    return succeeded;
  }

  public <Intermediate, Final> ListenableFuture<Final> run(Request<Intermediate, Final> request) throws Exception
  {
    try {
      final ListenableFuture<Final> retVal = go(request);
      succeeded = true;
      return retVal;
    }
    catch (Throwable e) {
      succeeded = false;
      Throwables.propagateIfPossible(e, Exception.class);
      throw Throwables.propagate(e);
    }
  }

  public GoHandler times(final int n)
  {
    final GoHandler myself = this;

    return new GoHandler()
    {
      AtomicInteger counter = new AtomicInteger(0);

      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(Request<Intermediate, Final> request) throws Exception
      {
        if (counter.getAndIncrement() < n) {
          return myself.go(request);
        }
        succeeded = false;
        throw new ISE("Called more than %d times", n);
      }
    };
  }
}
