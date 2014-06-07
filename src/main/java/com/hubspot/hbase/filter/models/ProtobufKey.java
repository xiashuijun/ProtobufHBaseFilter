package com.hubspot.hbase.filter.models;

import com.google.common.base.Objects;

import static com.google.common.base.Objects.equal;

public class ProtobufKey {
  private final String messageName;
  private final long descriptorHash;

  public ProtobufKey(String messageName, long descriptorHash) {
    this.messageName = messageName;
    this.descriptorHash = descriptorHash;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(messageName, descriptorHash);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof ProtobufKey)) {
      return false;
    }

    ProtobufKey other = (ProtobufKey)obj;
    return equal(messageName, other.messageName) &&
            equal(descriptorHash, other.descriptorHash);
  }
}
