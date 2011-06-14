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

import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 */
public class ToStringResponseHandler implements HttpResponseHandler<StringBuilder, String>
{
  private static final Logger log = Logger.getLogger(ToStringResponseHandler.class);
  StringBuilder bob = new StringBuilder();
  private final String url;

  public ToStringResponseHandler(String url)
  {
    this.url = url;
  }

  @Override
  public ClientResponse<StringBuilder> handleResponse(HttpResponse response)
  {
    long newStartTime = System.currentTimeMillis();

    int statusCode = response.getStatus().getCode();

    if (statusCode / 100 == 2) {
      return ClientResponse.unfinished(new StringBuilder(new String(response.getContent().array())));
    }

    log.error(
        String.format(
            "Error status code[%s:%s] talking to [%s]", statusCode, response.getStatus().getReasonPhrase(), url
        )
    );
    return ClientResponse.finished(null);
  }

  @Override
  public ClientResponse<StringBuilder> handleChunk(
      ClientResponse<StringBuilder> response,
      HttpChunk chunk
  )
  {
    response.getObj().append(new String(chunk.getContent().array()));
    return response;
  }

  @Override
  public ClientResponse<String> done(ClientResponse<StringBuilder> response)
  {

    if (response.getObj() == null) {
      return ClientResponse.finished(null);
    }

    return ClientResponse.finished(response.getObj().toString());
  }
}
