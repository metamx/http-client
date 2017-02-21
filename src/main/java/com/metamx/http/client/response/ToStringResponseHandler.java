/*
 * Copyright 2011 Metamarkets Group Inc.
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

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import java.nio.charset.Charset;

/**
 */
public class ToStringResponseHandler implements HttpResponseHandler<StringBuilder, String>
{
  private final Charset charset;

  public ToStringResponseHandler(Charset charset)
  {
    this.charset = charset;
  }

  @Override
  public ClientResponse<StringBuilder> handleResponse(FullHttpResponse response)
  {
    return ClientResponse.unfinished(new StringBuilder(response.content().toString(charset)));
  }

  @Override
  public ClientResponse<StringBuilder> handleChunk(
      ClientResponse<StringBuilder> response,
      HttpContent chunk
  )
  {
    final StringBuilder builder = response.getObj();
    if (builder == null) {
      return ClientResponse.finished(null);
    }

    builder.append(chunk.content().toString(charset));
    return response;
  }

  @Override
  public ClientResponse<String> done(ClientResponse<StringBuilder> response)
  {
    final StringBuilder builder = response.getObj();
    if (builder == null) {
      return ClientResponse.finished(null);
    }

    return ClientResponse.finished(builder.toString());
  }

  @Override
  public void exceptionCaught(
      ClientResponse<StringBuilder> clientResponse, Throwable e
  )
  {
    // Its safe to Ignore as the ClientResponse returned in handleChunk were unfinished
  }

}
