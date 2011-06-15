package com.metamx.http.client.pool;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class ResourcePoolTest
{
  ResourceFactory<String, String> resourceFactory;
  ResourcePool<String, String> pool;

  @Before
  public void setUp() throws Exception
  {
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    EasyMock.replay(resourceFactory);
    pool = new ResourcePool<String, String>(
        resourceFactory,
        new ResourcePoolConfig(2, false)
    );

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify();
  }

  @Test
  public void testSanity() throws Exception
  {
    primePool();
    EasyMock.replay(resourceFactory);
  }

  private void primePool()
  {
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.generate("sally")).andAnswer(new StringIncrementingAnswer("sally")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("sally0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    String billyString = pool.take("billy");
    String sallyString = pool.take("sally");
    Assert.assertEquals("billy0", billyString);
    Assert.assertEquals("sally0", sallyString);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    pool.giveBack("billy", billyString);
    pool.giveBack("sally", sallyString);
  }

  @Test
  public void testFailedResource() throws Exception
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.replay(resourceFactory);

    Assert.assertEquals("billy2", pool.take("billy"));
    pool.giveBack("billy", "billy2");
  }

  @Test
  public void testTakeMoreThanAllowed() throws Exception
  {
    primePool();
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    Assert.assertEquals("billy0", blockedThread.getValue());
  }

  private static class StringIncrementingAnswer implements IAnswer<String>
  {
    int count = 0;
    private String string;

    public StringIncrementingAnswer(String string) {
      this.string = string;
    }

    @Override
    public String answer() throws Throwable
    {
      return string + count++;
    }
  }

  private class MyThread extends Thread
  {
    private final CountDownLatch gotValueLatch = new CountDownLatch(1);
    
    private final CountDownLatch latch1;
    private String resourceName;

    volatile String value = null;

    public MyThread(CountDownLatch latch1, String resourceName) {
      this.latch1 = latch1;
      this.resourceName = resourceName;
    }

    @Override
    public void run()
    {
      value = pool.take(resourceName);
      gotValueLatch.countDown();
      try {
        latch1.await();
      }
      catch (InterruptedException e) {

      }
      pool.giveBack(resourceName, value);
    }

    public String getValue()
    {
      return value;
    }

    public void waitForValueToBeGotten(long length, TimeUnit timeUnit) throws InterruptedException
    {
      gotValueLatch.await(length, timeUnit);
    }
  }
}
