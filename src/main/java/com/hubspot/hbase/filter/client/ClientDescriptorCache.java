package com.hubspot.hbase.filter.client;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Atomics;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.hubspot.hbase.filter.models.DescriptorSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.plexus.util.dag.CycleDetectedException;
import org.codehaus.plexus.util.dag.DAG;
import org.codehaus.plexus.util.dag.TopologicalSorter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

public enum ClientDescriptorCache {
  INSTANCE;

  private static final Log LOG = LogFactory.getLog(ClientDescriptorCache.class);

  private final Cache<Class<? extends Message>, Descriptor> descriptorCache
          = CacheBuilder.newBuilder()
          .maximumSize(100)
          .softValues()
          .build();

  private final Cache<Class<? extends Message>, byte[]> serializedCache
          = CacheBuilder.newBuilder()
          .maximumSize(100)
          .softValues()
          .build();

  private final AtomicReference<Set<String>> descriptorPaths = Atomics.newReference();

  public Descriptor getDescriptor(Class<? extends Message> clazz) {
    Descriptor descriptor = descriptorCache.getIfPresent(clazz);
    if (descriptor == null) {
      return loadDescriptor(clazz);
    } else {
      return descriptor;
    }
  }

  public byte[] getPayloadForDescriptor(Class<? extends Message> clazz) throws IOException {
    Descriptor descriptor = getDescriptor(clazz);
    byte[] cachedResult = serializedCache.getIfPresent(clazz);
    if (cachedResult == null) {
      byte[] serialized = DescriptorSerializer.serialize(descriptor);
      serializedCache.put(clazz, serialized);
      return serialized;
    } else {
      return cachedResult;
    }
  }

  private Descriptor loadDescriptor(Class<? extends Message> clazz) {
    loadDescriptorPaths();
    for (String resourceName : descriptorPaths.get()) {
      InputStream inputStream = getClass().getResourceAsStream('/' + resourceName);
      try {
        Optional<Descriptor> maybeDescriptor = loadDescriptorFile(clazz, inputStream);
        if (maybeDescriptor.isPresent()) {
          return maybeDescriptor.get();
        }
      } catch (Exception e) {
        LOG.error(e, e);
        // invalid descriptor file was guessed.
      } finally {
        try {
          inputStream.close();
        } catch (IOException e) {}
      }
    }
    throw new IllegalArgumentException("Could not find descriptor for message " + clazz.getSimpleName());
  }

  private Optional<Descriptor> loadDescriptorFile(Class<? extends Message> clazz, InputStream inputStream) throws IOException, Descriptors.DescriptorValidationException {
    Optional<Descriptor> maybeDescriptor = Optional.absent();
    FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(inputStream);

    Map<String, FileDescriptor> descriptorsByName = Maps.newHashMap();

    for (FileDescriptorProto fdp: sortedDependencies(descriptorSet.getFileList())) {
      FileDescriptor fd = FileDescriptor.buildFrom(fdp, buildDependencies(descriptorsByName, fdp));
      descriptorsByName.put(fdp.getName(), fd);
      for (Descriptor descriptor : fd.getMessageTypes()) {
       Optional<Class<? extends Message>> maybeClass = getProtoClass(clazz, fdp, descriptor);
        if (maybeClass.isPresent()) {
          descriptorCache.asMap().put(maybeClass.get(), descriptor);
          if (maybeClass.get().equals(clazz)) {
            maybeDescriptor = Optional.of(descriptor);
          }
        }
      }
    }
    return maybeDescriptor;
  }

  private FileDescriptor[] buildDependencies(Map<String, FileDescriptor> descriptorsByName, FileDescriptorProto fdp) {
    FileDescriptor[] result = new FileDescriptor[fdp.getDependencyCount()];
    for (int i = 0; i < result.length; ++i) {
      result[i] = descriptorsByName.get(fdp.getDependency(i));
    }
    return result;
  }

  private List<FileDescriptorProto> sortedDependencies(List<FileDescriptorProto> descriptorProtos) {
    DAG dag = new DAG();
    Map<String, FileDescriptorProto> lookup = Maps.newHashMap();
    try {
      for (FileDescriptorProto descriptorProto : descriptorProtos) {
        lookup.put(descriptorProto.getName(), descriptorProto);
        dag.addVertex(descriptorProto.getName());
        for (String dependency : descriptorProto.getDependencyList()) {
          dag.addEdge(descriptorProto.getName(), dependency);
        }
      }
    } catch (CycleDetectedException e) {
      throw Throwables.propagate(e);
    }
    List<FileDescriptorProto> result = Lists.newArrayList();

    for (String name : (List<String>)TopologicalSorter.sort(dag)) {
      result.add(lookup.get(name));
    }
    return result;
  }

  private String qualifiedClassName(FileDescriptorProto fdp, Descriptor descriptor) {
    return fdp.getPackage() + "."
            + fdp.getOptions().getJavaOuterClassname() + "$"
            + descriptor.getName();
  }

  private Optional<Class<? extends Message>> getProtoClass(Class<? extends Message> searchKey, FileDescriptorProto fdp, Descriptor descriptor) {
    try {
      return Optional.<Class<? extends Message>>of((Class<? extends Message>)searchKey.getClassLoader().loadClass(qualifiedClassName(fdp, descriptor)));
    } catch (ClassNotFoundException e) {
      return Optional.absent();
    }
  }

  private void loadDescriptorPaths() {
    if (descriptorPaths.get() == null) {
      synchronized (this) {
        if (descriptorPaths.get() == null) {
          descriptorPaths.set(ImmutableSet.copyOf(ClasspathScanner.forPattern("\\w+\\.desc")));
        }
      }
    }
  }
}
