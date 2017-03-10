/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.metamx.http.client.response;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SequenceInputStreamResponseHandlerTest
{
  private static final int TOTAL_BYTES = 1 << 10;
  private static final ArrayList<byte[]> BYTE_LIST = new ArrayList<>();
  private static final Random RANDOM = new Random(378134789L);
  private static byte[] allBytes = new byte[TOTAL_BYTES];

  @BeforeClass
  public static void setUp()
  {
    final ByteBuffer buffer = ByteBuffer.wrap(allBytes);
    while (buffer.hasRemaining()) {
      final byte[] bytes = new byte[Math.min(Math.abs(RANDOM.nextInt()) % 128, buffer.remaining())];
      RANDOM.nextBytes(bytes);
      buffer.put(bytes);
      BYTE_LIST.add(bytes);
    }
  }

  @AfterClass
  public static void tearDown()
  {
    BYTE_LIST.clear();
    allBytes = null;
  }

  private static void fillBuff(InputStream inputStream, byte[] buff) throws IOException
  {
    int off = 0;
    while (off < buff.length) {
      final int read = inputStream.read(buff, off, buff.length - off);
      if (read < 0) {
        throw new IOException("Unexpected end of stream");
      }
      off += read;
    }
  }

  @Test(expected = TesterException.class)
  public void testExceptionalChunkedStream() throws IOException
  {
    Iterator<byte[]> it = BYTE_LIST.iterator();

    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response);
    final int failAt = Math.abs(RANDOM.nextInt()) % allBytes.length;
    while (it.hasNext()) {
      byte[] array = it.next();
      final DefaultHttpContent chunk = new DefaultHttpContent(
          new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, array, array.length)
          {
            @Override
            public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length)
            {
              if (dstIndex + length >= failAt) {
                throw new TesterException();
              }
              return super.getBytes(index, dst, dstIndex, length);
            }
          }
      );
      clientResponse = responseHandler.handleChunk(clientResponse, chunk);
    }
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final byte[] buff = new byte[allBytes.length];
    fillBuff(stream, buff);
  }

  public static class TesterException extends RuntimeException
  {
  }

  @Test(expected = TesterException.class)
  public void testExceptionalSingleStream() throws IOException
  {
    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, allBytes, allBytes.length)
        {
          @Override
          public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length)
          {
            if (dstIndex + length >= allBytes.length) {
              throw new TesterException();
            }
            return super.getBytes(index, dst, dstIndex, length);
          }
        }
    );
    HttpUtil.setTransferEncodingChunked(response, false);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response);
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final byte[] buff = new byte[allBytes.length];
    fillBuff(stream, buff);
  }

  @Test
  public void simpleMultiStreamTest() throws IOException
  {
    Iterator<byte[]> it = BYTE_LIST.iterator();

    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response);
    while (it.hasNext()) {
      final DefaultHttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(it.next()));
      clientResponse = responseHandler.handleChunk(clientResponse, chunk);
    }
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final InputStream expectedStream = new ByteArrayInputStream(allBytes);
    int read = 0;
    while (read < allBytes.length) {
      final byte[] expectedBytes = new byte[Math.min(Math.abs(RANDOM.nextInt()) % 128, allBytes.length - read)];
      final byte[] actualBytes = new byte[expectedBytes.length];
      fillBuff(stream, actualBytes);
      fillBuff(expectedStream, expectedBytes);
      Assert.assertArrayEquals(expectedBytes, actualBytes);
      read += expectedBytes.length;
    }
    Assert.assertEquals(allBytes.length, responseHandler.getByteCount());
  }

  @Test
  public void alignedMultiStreamTest() throws IOException
  {
    Iterator<byte[]> it = BYTE_LIST.iterator();

    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response);
    while (it.hasNext()) {
      final DefaultHttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(it.next()));
      clientResponse = responseHandler.handleChunk(clientResponse, chunk);
    }
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final InputStream expectedStream = new ByteArrayInputStream(allBytes);

    for (byte[] bytes : BYTE_LIST) {
      final byte[] expectedBytes = new byte[bytes.length];
      final byte[] actualBytes = new byte[expectedBytes.length];
      fillBuff(stream, actualBytes);
      fillBuff(expectedStream, expectedBytes);
      Assert.assertArrayEquals(expectedBytes, actualBytes);
      Assert.assertArrayEquals(expectedBytes, bytes);
    }
    Assert.assertEquals(allBytes.length, responseHandler.getByteCount());
  }

  @Test
  public void simpleSingleStreamTest() throws IOException
  {
    SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler();
    final HttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        Unpooled.wrappedBuffer(allBytes)
    );
    HttpUtil.setTransferEncodingChunked(response, false);
    ClientResponse<InputStream> clientResponse = responseHandler.handleResponse(response);
    clientResponse = responseHandler.done(clientResponse);

    final InputStream stream = clientResponse.getObj();
    final InputStream expectedStream = new ByteArrayInputStream(allBytes);
    int read = 0;
    while (read < allBytes.length) {
      final byte[] expectedBytes = new byte[Math.min(Math.abs(RANDOM.nextInt()) % 128, allBytes.length - read)];
      final byte[] actualBytes = new byte[expectedBytes.length];
      fillBuff(stream, actualBytes);
      fillBuff(expectedStream, expectedBytes);
      Assert.assertArrayEquals(expectedBytes, actualBytes);
      read += expectedBytes.length;
    }
    Assert.assertEquals(allBytes.length, responseHandler.getByteCount());
  }

}
