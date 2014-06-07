package com.hubspot.hbase.filter;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hbase.filter.FilterList.Operator;

public class ProtoFilterList extends FilterBase {
  private static final Configuration conf = HBaseConfiguration.create();

  FilterList delegate;
  Operator operator;
  List<Filter> filters = Lists.newArrayList();
  Map<String, byte[]> descriptors = Maps.newHashMap();

  @Deprecated
  /* For internal use */
  public ProtoFilterList() {
  }

  private ProtoFilterList(List<Filter> filters, Operator operator) {
    this.filters = filters;
    this.operator = operator;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    compressDescriptors(this);
    ByteArrayDataOutput outputBuffer = ByteStreams.newDataOutput();
    outputBuffer.writeByte(operator.ordinal());
    outputBuffer.writeInt(filters.size());
    for (Filter filter : filters) {
      outputBuffer.writeUTF(filter.getClass().getCanonicalName());
      filter.write(outputBuffer);
    }
    outputBuffer.writeInt(descriptors.size());
    for (Map.Entry<String, byte[]> entry : descriptors.entrySet()) {
      outputBuffer.writeUTF(entry.getKey());
      Bytes.writeByteArray(outputBuffer, entry.getValue());
    }
    WritableUtils.writeCompressedByteArray(out, outputBuffer.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ByteArrayDataInput inputBuffer = ByteStreams.newDataInput(WritableUtils.readCompressedByteArray(in));
    descriptors.clear();
    filters.clear();
    operator = Operator.values()[inputBuffer.readByte()];
    int numFilters = inputBuffer.readInt();
    for (int i = 0; i < numFilters; ++i) {
      Filter filter = getFilter(inputBuffer);
      filter.readFields(inputBuffer);
      filters.add(filter);
    }
    int numDescriptors = inputBuffer.readInt();
    for (int i = 0; i < numDescriptors; ++i) {
      String key = inputBuffer.readUTF();
      descriptors.put(key, Bytes.readByteArray(inputBuffer));
    }
    decompressDescriptors(this);
    this.delegate = new FilterList(operator, filters);
  }

  private Filter getFilter(DataInput in) {
    try {
      String className = in.readUTF();
      Class clazz = getClassByName(conf, className);
      return (Filter) clazz.newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    return delegate.getNextKeyHint(currentKV);
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    return delegate.filterRowKey(rowKey, offset, length);
  }

  @Override
  public boolean filterAllRemaining() {
    return delegate.filterAllRemaining();
  }

  @Override
  public KeyValue transform(KeyValue v) {
    return delegate.transform(v);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    return delegate.filterKeyValue(v);
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    delegate.filterRow(kvs);
  }

  @Override
  public boolean hasFilterRow() {
    return delegate.hasFilterRow();
  }

  @Override
  public boolean filterRow() {
    return delegate.filterRow();
  }


  private void compressDescriptors(ProtoFilterList filterList) {
    Set<ProtoFilterList> protoFilterLists = Sets.newHashSet();

    for (Filter filter : filterList.filters) {
      if (filter instanceof ProtobufColumnValueFilter) {
        ProtobufColumnValueFilter protobufFilter = (ProtobufColumnValueFilter) filter;
        if (protobufFilter.getDescriptorBytes() != null && !descriptors.containsKey(protobufFilter.getMessageName())) {
          descriptors.put(protobufFilter.getMessageName(), protobufFilter.getDescriptorBytes());
        }
        protobufFilter.setDescriptorBytes(null);
      }
      else if (filter instanceof ProtoFilterList) {
        compressDescriptors((ProtoFilterList) filter);
        protoFilterLists.add((ProtoFilterList)filter);
      }
    }

    for (ProtoFilterList protoFilterList : protoFilterLists) {
      this.descriptors.putAll(protoFilterList.descriptors);
      protoFilterList.descriptors.clear();
    }
  }

  private void decompressDescriptors(ProtoFilterList filterList) {
    if (descriptors.isEmpty()) {
      return;
    }

    for (Filter filter : filterList.filters) {
      if (filter instanceof ProtobufColumnValueFilter) {
        ProtobufColumnValueFilter protobufFilter = (ProtobufColumnValueFilter) filter;
        if (hasNoDescriptor(protobufFilter) && descriptors.containsKey(protobufFilter.getMessageName())) {
          protobufFilter.setDescriptorBytes(descriptors.get(protobufFilter.getMessageName()));
        }
      }
      else if (filter instanceof ProtoFilterList) {
        decompressDescriptors((ProtoFilterList)filter);
      }
    }
  }

  private boolean hasNoDescriptor(ProtobufColumnValueFilter protobufColumnValueFilter) {
    return protobufColumnValueFilter.getDescriptorBytes() == null || protobufColumnValueFilter.getDescriptorBytes().length == 0;
  }

  @Override
  public String toString() {
    return toString(5);
  }

  protected String toString(int maxFilters) {
    int endIndex = this.filters.size() < maxFilters
            ? this.filters.size() : maxFilters;
    return String.format("%s %s (%d/%d): %s",
            this.getClass().getSimpleName(),
            this.operator == Operator.MUST_PASS_ALL ? "AND" : "OR",
            endIndex,
            this.filters.size(),
            this.filters.subList(0, endIndex).toString());
  }

  public static ProtoFilterList and(Filter... filters) {
    return new ProtoFilterList(Lists.newArrayList(filters), Operator.MUST_PASS_ALL);
  }

  public static ProtoFilterList and(List<Filter> filters) {
    return new ProtoFilterList(filters, Operator.MUST_PASS_ALL);
  }

  public static ProtoFilterList or(Filter... filters) {
    return new ProtoFilterList(Lists.newArrayList(filters), Operator.MUST_PASS_ONE);
  }

  public static ProtoFilterList or(List<Filter> filters) {
    return new ProtoFilterList(filters, Operator.MUST_PASS_ONE);
  }

  private static Class getClassByName(Configuration conf, String className)
          throws ClassNotFoundException {
    if(conf != null) {
      return conf.getClassByName(className);
    }
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if(cl == null) {
      cl = HbaseObjectWritable.class.getClassLoader();
    }
    return Class.forName(className, true, cl);
  }
}
