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

import com.metamx.http.client.io.AppendableByteArrayInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import java.io.InputStream;

/**
 */
public class InputStreamResponseHandler implements HttpResponseHandler<AppendableByteArrayInputStream, InputStream>
{
  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
  {
    ByteBuf content = response instanceof HttpContent ? ((HttpContent) response).content() : Unpooled.EMPTY_BUFFER;
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    in.add(getBytes(content));
    return ClientResponse.finished(in);
  }

  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleChunk(
      ClientResponse<AppendableByteArrayInputStream> clientResponse, HttpContent chunk
  )
  {
    clientResponse.getObj().add(getBytes(chunk.content()));
    return clientResponse;
  }

  @Override
  public ClientResponse<InputStream> done(ClientResponse<AppendableByteArrayInputStream> clientResponse)
  {
    final AppendableByteArrayInputStream obj = clientResponse.getObj();
    obj.done();
    return ClientResponse.<InputStream>finished(obj);
  }

  @Override
  public void exceptionCaught(
      ClientResponse<AppendableByteArrayInputStream> clientResponse,
      Throwable e
  )
  {
    final AppendableByteArrayInputStream obj = clientResponse.getObj();
    obj.exceptionCaught(e);
  }

  private byte[] getBytes(ByteBuf content)
  {
    byte[] bytes = new byte[content.readableBytes()];
    int readerIndex = content.readerIndex();
    content.getBytes(readerIndex, bytes);
    return bytes;
  }
}
