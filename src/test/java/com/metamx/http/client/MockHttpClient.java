package com.metamx.http.client;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.http.client.pool.ResourceFactory;
import com.metamx.http.client.pool.ResourcePool;
import com.metamx.http.client.pool.ResourcePoolConfig;
import org.jboss.netty.channel.ChannelFuture;

/**
 */
public class MockHttpClient extends HttpClient
{
  private volatile GoHandler goHandler;

  public MockHttpClient()
  {
    super(
        new ResourcePool<String, ChannelFuture>(
            new ResourceFactory<String, ChannelFuture>()
            {
              @Override
              public ChannelFuture generate(String key)
              {
                return null;
              }

              @Override
              public boolean isGood(ChannelFuture resource)
              {
                return false;
              }

              @Override
              public void close(ChannelFuture resource)
              {

              }
            },
            new ResourcePoolConfig(1)
        )
    );
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
