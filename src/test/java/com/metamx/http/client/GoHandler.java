package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.ISE;
import com.metamx.http.client.response.HttpResponseHandler;

import java.util.concurrent.atomic.AtomicInteger;

/**
*/
public abstract class GoHandler
{
  /******* Abstract Methods *********/
  protected abstract <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler) throws Exception;

  /******* Non Abstract Methods ********/
  private volatile boolean succeeded = false;

  public boolean succeeded()
  {
    return succeeded;
  }

  public <Intermediate, Final> ListenableFuture<Final> run(Request request, HttpResponseHandler<Intermediate, Final> handler) throws Exception
  {
    try {
      final ListenableFuture<Final> retVal = go(request, handler);
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
      public <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler) throws Exception
      {
        if (counter.getAndIncrement() < n) {
          return myself.go(request, handler);
        }
        succeeded = false;
        throw new ISE("Called more than %d times", n);
      }
    };
  }
}
