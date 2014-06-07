package com.hubspot.hbase.filter.models;

import com.google.protobuf.Descriptors;

import java.util.List;

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class DescriptorPrune {
  public static Descriptor prune(Descriptor input) throws Descriptors.DescriptorValidationException {
    if (hasExternalFields(input.getFields())) {
      return input;
    } else {
      return pruneSingleDescriptor(input);
    }
  }

  private static Descriptor pruneSingleDescriptor(Descriptor input) throws Descriptors.DescriptorValidationException {
    FileDescriptorProto fileDescriptorProto = input.getFile().toProto();

    return Descriptors.FileDescriptor.buildFrom(
            fileDescriptorProto.toBuilder()
                    .clearEnumType()
                    .clearMessageType()
                    .clearDependency()
                    .addMessageType(input.toProto())
                    .build(),
            new Descriptors.FileDescriptor[]{}
    ).findMessageTypeByName(input.getName());
  }

  private static boolean hasExternalFields(List<FieldDescriptor> fieldDescriptors) {
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      if (fieldDescriptor.getJavaType() == JavaType.MESSAGE || fieldDescriptor.getJavaType() == JavaType.ENUM) {
        return true;
      }
    }
    return false;
  }
}
