package com.metamx.http.client.netty;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.ssl.SslBufferPool;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An SslHandler that remembers the handshake future.  There are some potential race conditions around handlers
 * if you take heavy advantage of renegotiations.  The future that gets returned is the "last" one to happen
 * where the ordering is determined by synchronization on the this object.
 */
public class HandshakeRememberingSslHandler extends SslHandler
{
  private volatile ChannelFuture handshakeFuture = null;
  private volatile boolean hasHandshook = false;

  public HandshakeRememberingSslHandler(SSLEngine engine)
  {
    super(engine);
  }

  public HandshakeRememberingSslHandler(SSLEngine engine, SslBufferPool bufferPool)
  {
    super(engine, bufferPool);
  }

  public HandshakeRememberingSslHandler(SSLEngine engine, boolean startTls)
  {
    super(engine, startTls);
  }

  public HandshakeRememberingSslHandler(SSLEngine engine, SslBufferPool bufferPool, boolean startTls)
  {
    super(engine, bufferPool, startTls);
  }

  public HandshakeRememberingSslHandler(SSLEngine engine, Executor delegatedTaskExecutor)
  {
    super(engine, delegatedTaskExecutor);
  }

  public HandshakeRememberingSslHandler(SSLEngine engine, SslBufferPool bufferPool, Executor delegatedTaskExecutor)
  {
    super(engine, bufferPool, delegatedTaskExecutor);
  }

  public HandshakeRememberingSslHandler(SSLEngine engine, boolean startTls, Executor delegatedTaskExecutor)
  {
    super(engine, startTls, delegatedTaskExecutor);
  }

  public HandshakeRememberingSslHandler(
      SSLEngine engine,
      SslBufferPool bufferPool,
      boolean startTls,
      Executor delegatedTaskExecutor
  )
  {
    super(engine, bufferPool, startTls, delegatedTaskExecutor);
  }

  public ChannelFuture getHandshakeFutureOrHandshake()
  {
    synchronized (this) {
      if (hasHandshook) {
        return handshakeFuture;
      }
      else {
        return handshake();
      }
    }
  }

  @Override
  public ChannelFuture handshake()
  {
    synchronized (this) {
      hasHandshook = true;
      return handshakeFuture = super.handshake();
    }
  }
}
