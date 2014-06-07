package com.hubspot.hbase.filter.client;

import com.google.common.collect.ImmutableSet;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.reflections.vfs.Vfs;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ClasspathScanner implements Iterable<String> {
  // The following code should never be executed more than once because getDefaultUrlTypes & setDefaultURLTypes are static
  static {
    final List<Vfs.UrlType> urlTypes = Vfs.getDefaultUrlTypes();
    // IgnoreUrlType must be first in the list of url types, it must catch the null classpath before it's matched by latter types
    urlTypes.add(0, FilesToIgnoreUrlType.newBuilder().setIgnoreNonExistentFiles(true).addUrlSuffixToIgnore(".pom").build());
    Vfs.setDefaultURLTypes(urlTypes);
  }

  private final Set<String> resources;

  public ClasspathScanner(FilterBuilder filter) {
    ConfigurationBuilder cfg = new ConfigurationBuilder().addUrls(ClasspathHelper.forJavaClassPath())
            .addUrls(ClasspathHelper.forPackage("com.hubspot"))
            .addUrls(ClasspathHelper.forPackage("com.performable"))
            .addUrls(ClasspathHelper.forPackage(""))
            .filterInputsBy(filter)
            .setScanners(new ResourcesScanner());

    resources = ImmutableSet.copyOf(new Reflections(cfg).getResources(filter));
  }

  public static ClasspathScanner forPattern(String pattern) {
    return new ClasspathScanner(new FilterBuilder().include(pattern));
  }

  @Override
  public Iterator<String> iterator() {
    return resources.iterator();
  }
}