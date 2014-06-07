package com.hubspot.hbase.filter;

import com.google.common.base.Throwables;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.hubspot.hbase.filter.example.ExampleProtos;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;

public class BaseProtoFilterTest {
  protected ProtobufFilter.Builder<ExampleProtos.ExampleProto> filterBuilder() throws IOException {
    return ProtobufFilter.forColumn(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, ExampleProtos.ExampleProto.class);
  }

  protected boolean matches(Filter filter, ExampleProtos.ExampleProto.Builder columnValue) {
    ProtobufColumnValueFilter protoFilter = serializeAndDeserialize((ProtobufColumnValueFilter) filter);
    protoFilter.filterKeyValue(buildKeyValue(columnValue));
    return !protoFilter.filterRow();
  }

  private ProtobufColumnValueFilter serializeAndDeserialize(ProtobufColumnValueFilter filter) {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    try {
      filter.write(dataOutput);
      ProtobufColumnValueFilter deserialized = new ProtobufColumnValueFilter();
      ByteArrayDataInput dataInput = ByteStreams.newDataInput(dataOutput.toByteArray());
      deserialized.readFields(dataInput);
      return deserialized;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  protected KeyValue buildKeyValue(ExampleProtos.ExampleProto.Builder columnValue) {
    return new KeyValue(
            EMPTY_BYTE_ARRAY,
            EMPTY_BYTE_ARRAY,
            EMPTY_BYTE_ARRAY,
            columnValue.build().toByteArray()
    );
  }
}
