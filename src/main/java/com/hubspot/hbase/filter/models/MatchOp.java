package com.hubspot.hbase.filter.models;

public enum MatchOp {
  MATCH_EQUAL(0),
  MATCH_NOT_EQUAL(1),
  MATCH_EXACT(2, true),
  MATCH_NOT_EXACT(3, true),
  MATCH_ANY(4),
  MATCH_NONE(5),
  MATCH_SCALAR(6)
  ;

  private final byte key;
  private final boolean isExact;

  private MatchOp(int key, boolean isExact) {
    this.key = (byte)key;
    this.isExact = isExact;
  }

  private MatchOp(int key) {
    this(key, false);
  }

  public boolean isExact() {
    return isExact;
  }

  public byte getKey() {
    return key;
  }

  public static MatchOp fromKey(byte key) {
    for (MatchOp op : values()) {
      if (op.getKey() == key) {
        return op;
      }
    }
    return null;
  }
}
