package com.metamx.http.client.pool;

/**
 */
public interface ResourceContainer<ResourceType>
{
  public ResourceType get();
  public void returnResource();
}
