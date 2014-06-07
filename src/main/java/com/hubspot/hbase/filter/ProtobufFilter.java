package com.hubspot.hbase.filter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.hubspot.hbase.filter.client.ClientDescriptorCache;
import com.hubspot.hbase.filter.models.MatchOp;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;

import java.io.IOException;

import static com.hubspot.hbase.filter.models.ProtobufFilterProtos.DynamicValue;
import static com.hubspot.hbase.filter.models.ProtobufFilterProtos.DynamicValue.Type;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class ProtobufFilter {
  private static final HashFunction MURMUR_128 = Hashing.murmur3_128();

  private ProtobufFilter() {
  }

  public static <T extends Message> Builder<T> forColumn(byte[] family, byte[] qualifier, Class<T> messageClass) throws IOException {
    byte[] descriptorBytes = ClientDescriptorCache.INSTANCE.getPayloadForDescriptor(messageClass);
    long descriptorHash = MURMUR_128.hashBytes(descriptorBytes).asLong();
    String name = messageClass.getCanonicalName();
    return new Builder<T>(family, qualifier, descriptorHash, name, descriptorBytes);
  }

  public static class Builder<T extends Message> {
    private byte[] columnFamily;
    private byte[] columnQualifier;
    private CompareOp compareOp = CompareOp.EQUAL;
    private MatchOp matchOp = MatchOp.MATCH_SCALAR;
    private boolean filterIfMissing = false;
    private boolean latestVersionOnly = true;
    private String operandFieldSpec;
    private DynamicValue operandValue;
    private byte[] operandMessageBytes;
    private String messageName;
    private long descriptorHash;
    private byte[] descriptorBytes;
    private WritableByteArrayComparable comparable;

    private Builder(byte[] family, byte[] qualifier,
                    long descriptorHash, String messageName,
                    byte[] descriptorBytes) {
      this.columnFamily = family;
      this.columnQualifier = qualifier;
      this.descriptorHash = descriptorHash;
      this.messageName = messageName;
      this.descriptorBytes = descriptorBytes;
    }

    private Filter build() {
      return new ProtobufColumnValueFilter(
              descriptorBytes, descriptorHash, messageName, operandMessageBytes,
              operandValue, operandFieldSpec, latestVersionOnly, filterIfMissing,
              matchOp, compareOp, columnQualifier, columnFamily, comparable);
    }

    public Builder<T> filterIfMissing(boolean filterIfMissing) {
      this.filterIfMissing = filterIfMissing;
      return this;
    }

    public Builder<T> latestVersionOnly(boolean latestVersionOnly) {
      this.latestVersionOnly = latestVersionOnly;
      return this;
    }

    public Filter containingFieldsMatched(T matcher) {
      return setMatchOp(matcher, MatchOp.MATCH_EQUAL);
    }

    public Filter containingFieldsNotMatched(T matcher) {
      return setMatchOp(matcher, MatchOp.MATCH_NOT_EQUAL);
    }

    public Filter isEqualTo(T matcher) {
      return setMatchOp(matcher, MatchOp.MATCH_EXACT);
    }

    public Filter isNotEqualTo(T matcher) {
      return setMatchOp(matcher, MatchOp.MATCH_NOT_EXACT);
    }

    public Filter anyFieldsMatched(T matcher) {
      return setMatchOp(matcher, MatchOp.MATCH_ANY);
    }

    public Filter noFieldsMatched(T matcher) {
      return setMatchOp(matcher, MatchOp.MATCH_NONE);
    }

    public Filter hasFieldByteComparableTo(String fieldSpec, WritableByteArrayComparable comparable, CompareOp compareOp) {
      this.operandFieldSpec = fieldSpec;
      this.compareOp = compareOp;
      this.comparable = comparable;
      this.matchOp = MatchOp.MATCH_SCALAR;
      return build();
    }

    public Filter hasFieldEqualTo(String fieldSpec, int value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_integer)
              .setValueInteger(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, long value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_long)
              .setValueLong(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, double value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_double)
              .setValueDouble(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, float value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_float)
              .setValueFloat(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, String value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, boolean value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bool)
              .setValueBool(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, byte[] value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bytes)
              .setValueBytes(ByteString.copyFrom(value))
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldEqualTo(String fieldSpec, Enum<?> value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value.name())
              .build();
      return setCompareOp(fieldSpec, CompareOp.EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, int value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_integer)
              .setValueInteger(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, long value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_long)
              .setValueLong(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, double value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_double)
              .setValueDouble(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, float value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_float)
              .setValueFloat(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, String value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, boolean value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bool)
              .setValueBool(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, byte[] value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bytes)
              .setValueBytes(ByteString.copyFrom(value))
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldNotEqualTo(String fieldSpec, Enum<?> value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value.name())
              .build();
      return setCompareOp(fieldSpec, CompareOp.NOT_EQUAL);
    }

    public Filter hasFieldLessThan(String fieldSpec, int value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_integer)
              .setValueInteger(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThan(String fieldSpec, long value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_long)
              .setValueLong(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThan(String fieldSpec, double value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_double)
              .setValueDouble(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThan(String fieldSpec, float value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_float)
              .setValueFloat(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThan(String fieldSpec, String value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThan(String fieldSpec, boolean value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bool)
              .setValueBool(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThan(String fieldSpec, byte[] value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bytes)
              .setValueBytes(ByteString.copyFrom(value))
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, int value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_integer)
              .setValueInteger(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, long value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_long)
              .setValueLong(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, double value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_double)
              .setValueDouble(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, float value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_float)
              .setValueFloat(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, String value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, boolean value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bool)
              .setValueBool(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldLessThanOrEqualTo(String fieldSpec, byte[] value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bytes)
              .setValueBytes(ByteString.copyFrom(value))
              .build();
      return setCompareOp(fieldSpec, CompareOp.LESS_OR_EQUAL);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, int value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_integer)
              .setValueInteger(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, long value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_long)
              .setValueLong(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, double value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_double)
              .setValueDouble(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, float value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_float)
              .setValueFloat(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, String value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, boolean value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bool)
              .setValueBool(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThan(String fieldSpec, byte[] value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bytes)
              .setValueBytes(ByteString.copyFrom(value))
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, int value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_integer)
              .setValueInteger(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, long value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_long)
              .setValueLong(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, double value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_double)
              .setValueDouble(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, float value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_float)
              .setValueFloat(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, String value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_string)
              .setValueString(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, boolean value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bool)
              .setValueBool(value)
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    public Filter hasFieldGreaterThanOrEqualTo(String fieldSpec, byte[] value) {
      operandValue = DynamicValue.newBuilder()
              .setTypeValue(Type.type_bytes)
              .setValueBytes(ByteString.copyFrom(value))
              .build();
      return setCompareOp(fieldSpec, CompareOp.GREATER_OR_EQUAL);
    }

    private Filter setMatchOp(T matcher, MatchOp matchOp) {
      this.operandValue = null;
      this.operandMessageBytes = matcher.toByteArray();
      this.matchOp = matchOp;
      return build();
    }

    private Filter setCompareOp(String fieldSpec, CompareOp compareOp) {
      this.comparable = null;
      this.operandFieldSpec = fieldSpec;
      this.operandMessageBytes = null;
      this.matchOp = MatchOp.MATCH_SCALAR;
      this.compareOp = compareOp;
      return build();
    }
  }
}
