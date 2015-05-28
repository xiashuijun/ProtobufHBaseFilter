package com.hubspot.hbase.filter;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.hubspot.hbase.filter.example.ExampleProtos;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.hubspot.hbase.filter.example.ExampleProtos.ExampleProto;
import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

public class ProtoFilterListSerializeTest {
  @Test
  public void itShouldSerializeAFlatList() throws Exception {
    List<Filter> filters = Lists.newArrayList();
    for (int i = 0; i < 1000; ++i) {
      filters.add(ProtobufFilter.forColumn(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, ExampleProto.class)
              .hasFieldEqualTo("id", (long)i));
    }

    Filter newFilterList = serializeAndDeserialize(ProtoFilterList.or(filters));

    newFilterList.filterKeyValue(buildKeyValue(ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setId(2)));

    assertThat(newFilterList.filterRow()).isFalse();

    newFilterList.reset();

    newFilterList.filterKeyValue(buildKeyValue(ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setId(1001)));

    assertThat(newFilterList.filterRow()).isTrue();


  }

  protected KeyValue buildKeyValue(ExampleProtos.ExampleProto.Builder columnValue) {
    return new KeyValue(
            EMPTY_BYTE_ARRAY,
            EMPTY_BYTE_ARRAY,
            EMPTY_BYTE_ARRAY,
            columnValue.build().toByteArray()
    );
  }

  private int getSize(Writable writable) throws IOException {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    writable.write(dataOutput);
    return dataOutput.toByteArray().length;
  }

  private Filter serializeAndDeserialize(Filter filter) {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    try {
      HbaseObjectWritable.writeObject(dataOutput, filter, ProtoFilterList.class, null);
      ByteArrayDataInput dataInput = ByteStreams.newDataInput(dataOutput.toByteArray());
      return (Filter)HbaseObjectWritable.readObject(dataInput, null);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
