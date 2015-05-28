package com.hubspot.hbase.filter.client;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.reflections.vfs.Vfs;

import com.google.common.collect.Lists;

/**
 * A configurable URL Type handler for Reflections VFS which matches classpath
 * entries to be ignored, currently supporting 2 types of files to be ignored
 * 1. files that do not actually exist (configure ignore or not)
 * 2. files with specified suffix (configure 0 to many)
 * <p>
 * Instances are fully immutable.
 */
public final class FilesToIgnoreUrlType implements Vfs.UrlType {

  public static class Builder {
    private List<String> urlSuffixesToIgnore = Lists.newArrayList();
    private boolean ignoreNonExistentFiles = false;

    private Builder() {
    }
    public Builder setIgnoreNonExistentFiles(boolean ignoreNonExistentFiles) {
      this.ignoreNonExistentFiles = ignoreNonExistentFiles;
      return this;
    }
    public Builder addUrlSuffixToIgnore(String urlSuffix) {
      urlSuffixesToIgnore.add(urlSuffix);
      return this;
    }
    public boolean getIgnoreNonExistentFiles() {
      return ignoreNonExistentFiles;
    }
    public List<String> getUrlSuffixesToIgnore() {
      return urlSuffixesToIgnore;
    }
    public FilesToIgnoreUrlType build() {
      return new FilesToIgnoreUrlType(urlSuffixesToIgnore, ignoreNonExistentFiles);
    }
  }

  private final List<String> urlSuffixesToIgnore;
  private final boolean ignoreNonExistentFiles;

  public static Builder newBuilder() {
    return new Builder();
  }

  private FilesToIgnoreUrlType(List<String> urlSuffixesToIgnore, boolean ignoreNonExistentFiles) {
    this.urlSuffixesToIgnore = urlSuffixesToIgnore;
    this.ignoreNonExistentFiles = ignoreNonExistentFiles;
  }

  @Override
  public boolean matches(final URL url) {
    if (ignoreNonExistentFiles) {
      try {
        if (url.getProtocol().equals("file") && !new File(url.toURI().getSchemeSpecificPart()).exists()) {
          return true;
        }
      } catch (URISyntaxException ignore) { }
    }

    for (String urlSuffix: urlSuffixesToIgnore) {
      if (url.toExternalForm().endsWith(urlSuffix)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Vfs.Dir createDir(final URL url) {
    return new EmptyVfsDir(url);
  }

  @Override
  public String toString() {
    StringBuilder desc = new StringBuilder();
    desc.append("URLType to ignore ");
    if (ignoreNonExistentFiles) {
      desc.append("nonexistent classpath and ");
    }
    desc.append("classpath with suffixes: ");
    for (String urlSuffix: urlSuffixesToIgnore) {
      desc.append(urlSuffix).append(" ");
    }
    return desc.toString();
  }

  private static final class EmptyVfsDir implements Vfs.Dir {
    private final URL url;

    private EmptyVfsDir(URL url) {
      this.url = url;
    }

    @Override
    public String getPath() {
      return url.getPath();
    }

    @Override
    public Iterable<org.reflections.vfs.Vfs.File> getFiles() {
      return Collections.emptyList();
    }

    @Override
    public void close() {
      // do nothing
    }
  }
}
