package com.metamx.http.client.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

/**
 */
public class AppendableByteArrayInputStream extends InputStream
{
  private final LinkedList<byte[]> bytes = new LinkedList<byte[]>();
  private final SingleByteReaderDoer singleByteReaderDoer = new SingleByteReaderDoer();

  private volatile boolean done = false;
  private volatile int available = 0;

  private byte[] curr = new byte[]{};
  private int currIndex = 0;

  public void add(byte[] bytesToAdd) {
    if (bytesToAdd.length == 0) {
      return;
    }

    synchronized (bytes) {
      bytes.addLast(bytesToAdd);
      available += bytesToAdd.length;
      bytes.notify();
    }
  }

  public void done()
  {
    synchronized (bytes) {
      done = true;
      bytes.notify();
    }
  }

  @Override
  public int read() throws IOException
  {
    if (scanThroughBytesAndDoSomething(1, singleByteReaderDoer) == 0) {
      return -1;
    }
    return singleByteReaderDoer.getRetVal();
  }

  @Override
  public int read(final byte[] b, final int off, int len) throws IOException
  {
    if (b == null) {
   	    throw new NullPointerException();
   	} else if (off < 0 || len < 0 || len > b.length - off) {
   	    throw new IndexOutOfBoundsException();
   	} else if (len == 0) {
   	    return 0;
   	}

    final long retVal = scanThroughBytesAndDoSomething(
        len,
        new Doer()
        {
          int currOff = off;

          @Override
          public void doSomethingWithByteArray(int numRead)
          {
            System.arraycopy(curr, currIndex, b, currOff, numRead);
            currOff += numRead;
          }
        }
    );
    return retVal == 0 ? -1 : (int) retVal;
  }

  @Override
  public long skip(long n) throws IOException
  {
    return scanThroughBytesAndDoSomething(
        n,
        new Doer()
        {
          @Override
          public void doSomethingWithByteArray(int numToScan)
          {
          }
        }
    );
  }

  private long scanThroughBytesAndDoSomething(long numToScan, Doer doer) throws IOException
  {
    long numScanned = 0;
    long numPulled = 0;

    while (numToScan > numScanned) {
      if (currIndex >= curr.length) {
        synchronized (bytes) {
          if (bytes.isEmpty()) {
            if (done) {
              break;
            }
            try {
              available -= numPulled;
              numPulled = 0;
              bytes.wait();
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("Interrupted!");
            }
          }

          if (bytes.isEmpty() && done) {
            break;
          }

          curr = bytes.removeFirst();
          currIndex = 0;
        }
      }

      final long numToPullFromCurr = Math.min(curr.length - currIndex, numToScan - numScanned);
      doer.doSomethingWithByteArray((int) numToPullFromCurr);
      numScanned += numToPullFromCurr;
      currIndex += numToPullFromCurr;
      numPulled += numToPullFromCurr;
    }

    synchronized (bytes) {
      available -= numPulled;
    }

    return numScanned;
  }

  @Override
  public int available() throws IOException
  {
    return available;
  }

  private static interface Doer
  {
    public void doSomethingWithByteArray(int numToScan);
  }

  private class SingleByteReaderDoer implements Doer
  {
    private int retVal;

    public SingleByteReaderDoer() {
    }

    @Override
    public void doSomethingWithByteArray(int numToScan)
    {
      retVal = curr[currIndex];
    }

    public int getRetVal()
    {
      return retVal;
    }
  }
}
