/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.plugin;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.TreeMap;

/** Owns one explicit plugin bundle. Only trusted JARs may be loaded. */
public final class PluginDiscovery<P extends Plugin> implements AutoCloseable {
  private final BundleClassLoader loader;
  private final Map<String, P> providers;
  private boolean closed;

  private PluginDiscovery(BundleClassLoader loader, Map<String, P> providers) {
    this.loader = loader;
    this.providers = providers;
  }

  /** Loads providers from a bundle plus its dependencies, without scanning the host classpath. */
  public static <P extends Plugin> PluginDiscovery<P> load(Class<P> service, List<Path> jars) {
    Objects.requireNonNull(service, "service");
    if (!service.isInterface() || !Plugin.class.isAssignableFrom(service)) {
      throw new IllegalArgumentException("Service must be a shared Plugin interface");
    }
    List<Path> paths = List.copyOf(jars);
    if (paths.isEmpty()) {
      throw new IllegalArgumentException("A bundle must contain at least one JAR");
    }
    URL[] urls = new URL[paths.size()];
    try {
      for (int i = 0; i < paths.size(); i++) {
        Path path = paths.get(i);
        if (!Files.isRegularFile(path) || !path.toString().endsWith(".jar")) {
          throw new PluginException("Not a plugin JAR: " + path);
        }
        urls[i] = path.toUri().toURL();
      }
    } catch (IOException e) {
      throw new PluginException("Invalid plugin bundle path", e);
    }
    BundleClassLoader loader = new BundleClassLoader(urls, service.getClassLoader(), service);
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(loader);
      Map<String, P> providers = new TreeMap<>();
      for (P provider : ServiceLoader.load(service, loader)) {
        String id = provider.identifier();
        if (id == null || !id.matches("[a-z][a-z0-9-]*")) {
          throw new PluginException("Invalid plugin identifier: " + id);
        }
        if (providers.putIfAbsent(id, provider) != null) {
          throw new PluginException("Duplicate plugin identifier: " + id);
        }
      }
      return new PluginDiscovery<>(loader, providers);
    } catch (RuntimeException | Error failure) {
      try {
        loader.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      if (failure instanceof VirtualMachineError fatal) {
        throw fatal;
      }
      if (failure instanceof ThreadDeath fatal) {
        throw fatal;
      }
      throw new PluginException("Failed to discover " + service.getName(), failure);
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  public synchronized List<String> identifiers() {
    ensureOpen();
    return List.copyOf(providers.keySet());
  }

  public synchronized P get(String identifier) {
    ensureOpen();
    P provider = providers.get(Objects.requireNonNull(identifier, "identifier"));
    if (provider == null) {
      throw new PluginException("Plugin not found: " + identifier);
    }
    return provider;
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Plugin bundle is closed");
    }
  }

  /** Releases JAR handles, not resources owned by provider instances. */
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      providers.clear();
      loader.close();
    }
  }
}
