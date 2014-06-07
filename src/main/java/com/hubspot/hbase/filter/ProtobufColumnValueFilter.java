package com.hubspot.hbase.filter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hubspot.hbase.filter.models.MatchOp;
import com.hubspot.hbase.filter.server.ServerDescriptorCache;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.EnumValueDescriptor;
import static com.hubspot.hbase.filter.models.ProtobufFilterProtos.DynamicValue;
import static com.hubspot.hbase.filter.models.ProtobufFilterProtos.getDescriptor;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class ProtobufColumnValueFilter extends FilterBase {
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
  private WritableByteArrayComparable byteArrayComparable;

  private boolean foundColumn = false;
  private boolean matchedColumn = false;

  private DynamicMessage operandMessage;

  public ProtobufColumnValueFilter() {
  }

  ProtobufColumnValueFilter(byte[] descriptorBytes, long descriptorHash,
                            String messageName, byte[] operandMessageBytes,
                            DynamicValue operandValue, String operandFieldSpec,
                            boolean latestVersionOnly, boolean filterIfMissing,
                            MatchOp matchOp, CompareOp compareOp,
                            byte[] columnQualifier, byte[] columnFamily,
                            WritableByteArrayComparable byteArrayComparable) {
    this.descriptorBytes = descriptorBytes;
    this.descriptorHash = descriptorHash;
    this.messageName = messageName;
    this.operandMessageBytes = operandMessageBytes;
    this.operandValue = operandValue;
    this.operandFieldSpec = operandFieldSpec;
    this.latestVersionOnly = latestVersionOnly;
    this.filterIfMissing = filterIfMissing;
    this.matchOp = matchOp;
    this.compareOp = compareOp;
    this.columnQualifier = columnQualifier;
    this.columnFamily = columnFamily;
    this.byteArrayComparable = byteArrayComparable;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, columnFamily);
    Bytes.writeByteArray(out, columnQualifier);
    WritableUtils.writeEnum(out, compareOp);
    out.writeByte(matchOp.getKey());
    out.writeBoolean(filterIfMissing);
    out.writeBoolean(latestVersionOnly);
    out.writeUTF(nullToEmpty(operandFieldSpec));
    WritableUtils.writeCompressedByteArray(out, operandValue == null ? null : operandValue.toByteArray());
    WritableUtils.writeCompressedByteArray(out, operandMessageBytes);
    out.writeUTF(messageName);
    out.writeLong(descriptorHash);
    Bytes.writeByteArray(out, descriptorBytes);
    out.writeBoolean(byteArrayComparable != null);
    if (byteArrayComparable != null) {
      HbaseObjectWritable.writeObject(out, byteArrayComparable, WritableByteArrayComparable.class, null);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    columnFamily = Bytes.readByteArray(in);
    columnQualifier = Bytes.readByteArray(in);
    compareOp = WritableUtils.readEnum(in, CompareOp.class);
    matchOp = MatchOp.fromKey(in.readByte());
    filterIfMissing = in.readBoolean();
    latestVersionOnly = in.readBoolean();
    operandFieldSpec = in.readUTF();
    byte[] operandValueBytes = WritableUtils.readCompressedByteArray(in);
    if (operandValueBytes == null) {
      operandValue = null;
    } else {
      operandValue = DynamicValue.parseFrom(operandValueBytes);
    }
    operandMessageBytes = WritableUtils.readCompressedByteArray(in);
    messageName = in.readUTF();
    descriptorHash = in.readLong();
    descriptorBytes = Bytes.readByteArray(in);
    if (in.readBoolean()) {
      byteArrayComparable =
              (WritableByteArrayComparable) HbaseObjectWritable.readObject(in, null);
    } else {
      byteArrayComparable = null;
    }
  }

  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn? !this.matchedColumn: this.filterIfMissing;
  }

  public void reset() {
    foundColumn = false;
    matchedColumn = false;
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    if (this.matchedColumn) {
      // We already found and matched the single column, all keys now pass
      return ReturnCode.INCLUDE;
    } else if (this.latestVersionOnly && this.foundColumn) {
      // We found but did not match the single column, skip to next row
      return ReturnCode.NEXT_ROW;
    }
    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    foundColumn = true;
    if (!keepColumnValue(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength())) {
      return this.latestVersionOnly ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
    }
    this.matchedColumn = true;
    return ReturnCode.INCLUDE;
  }

  @VisibleForTesting
  static boolean matchScalar(String fieldSpec, DynamicValue compareValue,
                             CompareOp compareOp, DynamicMessage value,
                             WritableByteArrayComparable comparable) {
    Object compareScalar = extractScalar(fieldSpec, value);
    // Only pass nulls if not equal.
    if (compareScalar == null) {
      return compareOp == CompareOp.NOT_EQUAL;
    }
    int compareResult;
    if (comparable == null) {
      compareResult = getCompareResult(compareScalar, compareValue);
    } else {
      compareResult = comparable.compareTo(getBytesFromScalar(compareScalar));
    }
    return interpretCompareResult(compareOp, compareResult);
  }

  private static int getCompareResult(Object compareScalar, DynamicValue compareOperand) {
    switch (compareOperand.getTypeValue()) {
      case type_string:
        return String.valueOf(compareScalar).compareTo(compareOperand.getValueString());
      case type_integer:
        return Ints.compare((Integer)compareScalar, compareOperand.getValueInteger());
      case type_long:
        return Longs.compare((Long)compareScalar, compareOperand.getValueLong());
      case type_float:
        return Floats.compare((Float)compareScalar, compareOperand.getValueFloat());
      case type_bool:
        return Booleans.compare((Boolean)compareScalar, compareOperand.getValueBool());
      case type_double:
        return Doubles.compare((Double)compareScalar, compareOperand.getValueDouble());
      case type_bytes:
        return Bytes.compareTo(((ByteString)compareScalar).toByteArray(), compareOperand.getValueBytes().toByteArray());
      default:
        throw new RuntimeException("Unknown dynamic type: " + compareOperand.getTypeValue());
    }
  }

  private static byte[] getBytesFromScalar(Object scalar) {
    if (scalar instanceof String) {
      return Bytes.toBytes((String)scalar);
    }
    else if (scalar instanceof Integer) {
      return Bytes.toBytes((Integer)scalar);
    }
    else if (scalar instanceof Long) {
      return Bytes.toBytes((Long)scalar);
    }
    else if (scalar instanceof Float) {
      return Bytes.toBytes((Float)scalar);
    }
    else if (scalar instanceof Double) {
      return Bytes.toBytes((Double)scalar);
    }
    else if (scalar instanceof ByteString) {
      return ((ByteString) scalar).toByteArray();
    }
    else if (scalar instanceof Boolean) {
      return Bytes.toBytes((Boolean)scalar);
    }
    else if (scalar == null) {
      return null;
    }
    else {
      throw new RuntimeException("Unknown scalar type: " + scalar.getClass());
    }
  }

  private static Object extractScalar(String fieldSpec, DynamicMessage value) {
    List<String> fields = Lists.newArrayList(Splitter.on('.').omitEmptyStrings().limit(2).split(fieldSpec));

    String fieldName = fields.get(0);

    for (Map.Entry<Descriptors.FieldDescriptor, Object> field : value.getAllFields().entrySet()) {
      if (fieldName.equals(field.getKey().getName())) {
        switch (field.getKey().getJavaType()) {
          case ENUM:
            return ((EnumValueDescriptor)field.getValue()).getName();
          case MESSAGE:
            return extractScalar(fields.get(1), (DynamicMessage)field.getValue());
          default:
            return field.getValue();
        }
      }
    }

    // check for defaults
    for (Descriptors.FieldDescriptor fieldDescriptor : value.getDescriptorForType().getFields()) {
      if (fieldName.equals(fieldDescriptor.getName())) {
        if (fieldDescriptor.hasDefaultValue()) {
          return fieldDescriptor.getDefaultValue();
        }
      }
    }
    return null;
  }

  private boolean keepColumnValue(byte [] data,
                                  int offset,
                                  int length) {
    Descriptor descriptor = getDescriptor();
    DynamicMessage dynamicMessage = getMessage(descriptor, data, offset, length);

    if (this.matchOp == MatchOp.MATCH_SCALAR) {
      return matchScalar(operandFieldSpec, operandValue, compareOp, dynamicMessage, byteArrayComparable);
    } else {
      DynamicMessage operandMessage = getOperandMessage(descriptor);
      return matchMessages(operandMessage, dynamicMessage, matchOp);
    }
  }

  @VisibleForTesting
  static boolean matchMessages(DynamicMessage operand, DynamicMessage value, MatchOp matchOp) {
    Map<String, Object> valueFields = Maps.newHashMap();

    for (Map.Entry<Descriptors.FieldDescriptor, Object> field : value.getAllFields().entrySet()) {
      valueFields.put(field.getKey().getFullName(), field.getValue());
    }

    boolean hasInequality = false;
    boolean hasEquality = false;

    for (Map.Entry<Descriptors.FieldDescriptor, Object> field : operand.getAllFields().entrySet()) {
      Object valueValue = valueFields.get(field.getKey().getFullName());
      Object operandValue = field.getValue();
      if (equal(operandValue, valueValue)) {
        hasEquality = true;
        if (matchOp == MatchOp.MATCH_ANY) {
          return true;
        }
      } else {
        if (matchOp == MatchOp.MATCH_EQUAL || matchOp == MatchOp.MATCH_NOT_EQUAL) {
          return matchOp == MatchOp.MATCH_NOT_EQUAL;
        } else {
          hasInequality = true;
        }
      }

      valueFields.remove(field.getKey().getFullName());
    }

    switch (matchOp) {
      case MATCH_EXACT:
      case MATCH_NOT_EXACT:
        if (valueFields.isEmpty() && !hasInequality) {
          return matchOp == MatchOp.MATCH_EXACT;
        } else {
          return matchOp == MatchOp.MATCH_NOT_EXACT;
        }
      case MATCH_NONE:
        return !hasEquality;
      default:
        return matchOp == MatchOp.MATCH_EQUAL;
    }
  }

  private DynamicMessage getOperandMessage(Descriptor descriptor) {
    if (operandMessage == null) {
      try {
        operandMessage = DynamicMessage.parseFrom(descriptor, operandMessageBytes);
      } catch (InvalidProtocolBufferException e) {
        throw Throwables.propagate(e);
      }
    }
    return operandMessage;
  }

  private static boolean interpretCompareResult(CompareOp compareOp, int compareResult) {
    switch (compareOp) {
      case LESS:
        return compareResult < 0;
      case LESS_OR_EQUAL:
        return compareResult <= 0;
      case EQUAL:
        return compareResult == 0;
      case NOT_EQUAL:
        return compareResult != 0;
      case GREATER_OR_EQUAL:
        return compareResult >= 0;
      case GREATER:
        return compareResult > 0;
      default:
        throw new RuntimeException("Unknown Compare op " + compareOp.name());
    }
  }

  private DynamicMessage getMessage(Descriptor descriptor, byte[] data,
                                    int offset, int length) {
    try {
      return DynamicMessage.parseFrom(descriptor, ByteSource.wrap(data).slice(offset, length).openBufferedStream());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Descriptor getDescriptor() {
    try {
      return ServerDescriptorCache.INSTANCE.cachedDescriptorLoad(messageName, descriptorHash, descriptorBytes);
    } catch (Descriptors.DescriptorValidationException e) {
      throw Throwables.propagate(e);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /* Useful for ProtoFilterList */
  String getMessageName() {
    return messageName;
  }

  byte[] getDescriptorBytes() {
    return descriptorBytes;
  }

  void setDescriptorBytes(byte[] descriptorBytes) {
    this.descriptorBytes = descriptorBytes;
  }
}
