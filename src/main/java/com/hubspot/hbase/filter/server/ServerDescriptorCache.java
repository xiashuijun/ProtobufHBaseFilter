package com.hubspot.hbase.filter.server;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Descriptors;
import com.hubspot.hbase.filter.models.DescriptorSerializer;
import com.hubspot.hbase.filter.models.ProtobufKey;

import java.io.IOException;

import static com.google.protobuf.Descriptors.Descriptor;

public enum ServerDescriptorCache {
  INSTANCE;

  private final Cache<ProtobufKey, Descriptor> descriptorCache = CacheBuilder.newBuilder()
          .maximumSize(100)
          .softValues()
          .build();

  public Descriptor cachedDescriptorLoad(String descriptorName, long hash, byte[] serialized)
          throws IOException, Descriptors.DescriptorValidationException {
    ProtobufKey key = new ProtobufKey(descriptorName, hash);
    Descriptor cachedResult = descriptorCache.asMap().get(key);
    if (cachedResult == null) {
      Descriptor descriptor = DescriptorSerializer.deserialize(serialized);
      descriptorCache.asMap().put(key, descriptor);
      return descriptor;
    } else {
      return cachedResult;
    }
  }
}
