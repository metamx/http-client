package com.metamx.http.client.response;

import com.google.common.base.Throwables;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.InputStream;

/**
 */
public class InputStreamResponseHandler implements HttpResponseHandler<AppendableByteArrayInputStream, InputStream>
{
  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    in.add(getContentBytes(response.getContent()));
    return ClientResponse.finished(in);
  }

  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleChunk(
      ClientResponse<AppendableByteArrayInputStream> clientResponse, HttpChunk chunk
  )
  {
    clientResponse.getObj().add(getContentBytes(chunk.getContent()));
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

  private byte[] getContentBytes(ChannelBuffer content)
  {
    byte[] contentBytes = new byte[content.readableBytes()];
    content.readBytes(contentBytes);
    return contentBytes;
  }
}
