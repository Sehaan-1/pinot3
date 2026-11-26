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
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;

/** Child-first for bundle implementation classes, parent-only for shared contracts. */
final class BundleClassLoader extends URLClassLoader {
  private final String descriptor;
  private final String serviceName;
  private static final List<String> SHARED = List.of(
      "java.", "javax.", "jdk.", "sun.", "org.apache.seatunnel.api.",
      "org.apache.seatunnel.common.", "org.apache.seatunnel.plugin.", "org.slf4j.");

  BundleClassLoader(URL[] urls, ClassLoader parent, Class<?> service) {
    super(urls, parent);
    serviceName = service.getName();
    descriptor = "META-INF/services/" + serviceName;
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      Class<?> loaded = findLoadedClass(name);
      if (loaded == null) {
        if (name.equals(serviceName) || SHARED.stream().anyMatch(name::startsWith)) {
          loaded = getParent().loadClass(name);
        } else {
          try {
            loaded = findClass(name);
          } catch (ClassNotFoundException absent) {
            loaded = getParent().loadClass(name);
          }
        }
      }
      if (resolve) {
        resolveClass(loaded);
      }
      return loaded;
    }
  }

  @Override
  public URL getResource(String name) {
    URL own = findResource(name);
    return own != null || name.equals(descriptor) ? own : getParent().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    var urls = new LinkedHashSet<URL>(Collections.list(findResources(name)));
    if (!name.equals(descriptor)) {
      urls.addAll(Collections.list(getParent().getResources(name)));
    }
    return Collections.enumeration(urls);
  }
}
