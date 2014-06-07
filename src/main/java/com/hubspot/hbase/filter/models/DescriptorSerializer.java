package com.hubspot.hbase.filter.models;

import com.google.common.base.Throwables;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.hubspot.hbase.filter.utils.Gzip;

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

public class DescriptorSerializer {
  private DescriptorSerializer() {
  }

  public static byte[] serialize(Descriptor descriptor) throws IOException {
    try {
      descriptor = DescriptorPrune.prune(descriptor);
    } catch (Descriptors.DescriptorValidationException e) {
      throw Throwables.propagate(e);
    }
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    out.writeUTF(descriptor.getName());
    writeFileDescriptor(out, descriptor.getFile());
    return Gzip.compress(out.toByteArray());
  }

  public static Descriptor deserialize(byte[] input) throws IOException, Descriptors.DescriptorValidationException {
    ByteArrayDataInput dataInput = ByteStreams.newDataInput(Gzip.decompress(input));
    String name = dataInput.readUTF();
    FileDescriptor fileDescriptor = readFileDescriptor(dataInput);
    return fileDescriptor.findMessageTypeByName(name);
  }

  private static FileDescriptor readFileDescriptor(DataInput in) throws IOException, Descriptors.DescriptorValidationException {
    FileDescriptorProto proto = FileDescriptorProto.parseFrom(Bytes.readByteArray(in));
    int numDependents = in.readInt();
    FileDescriptor[] dependents = new FileDescriptor[numDependents];
    for (int i = 0; i < numDependents; ++i) {
      dependents[i] = readFileDescriptor(in);
    }
    return FileDescriptor.buildFrom(proto, dependents);
  }

  public static void writeFileDescriptor(DataOutput out, FileDescriptor fileDescriptor) throws IOException {
    Bytes.writeByteArray(out, fileDescriptor.toProto().toByteArray());
    List<FileDescriptor> dependents = fileDescriptor.getDependencies();
    out.writeInt(dependents.size());
    for (FileDescriptor dependent : dependents) {
      writeFileDescriptor(out, dependent);
    }
  }
}
