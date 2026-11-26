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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginDiscoveryTest {
  @TempDir Path temporary;
  private int sequence;

  /** Builds real provider classes and service registrations, without mocks or checked-in binaries. */
  private Path bundle(List<String> identifiers, boolean brokenDescriptor) throws IOException {
    Path root = Files.createDirectory(temporary.resolve("bundle-" + sequence++));
    Path classes = Files.createDirectory(root.resolve("classes"));
    List<String> sources = new ArrayList<>();
    StringBuilder descriptor = new StringBuilder();
    for (int i = 0; i < identifiers.size(); i++) {
      String className = "Provider" + i;
      String code = "package fixture; public class " + className
          + " implements org.apache.seatunnel.plugin.Plugin {"
          + " public " + className + "() { if (Thread.currentThread().getContextClassLoader()"
          + " != getClass().getClassLoader()) throw new IllegalStateException(\"wrong TCCL\"); }"
          + " public String identifier() { return \"" + identifiers.get(i) + "\"; }"
          + " public String toString() { return Dependency.value(); }}";
      Path source = root.resolve(className + ".java");
      Files.writeString(source, code);
      sources.add(source.toString());
      descriptor.append("fixture.").append(className).append('\n');
    }
    Path dependency = root.resolve("Dependency.java");
    Files.writeString(dependency,
        "package fixture; public class Dependency { public static String value() { return \"bundle\"; }}");
    sources.add(dependency.toString());
    List<String> arguments = new ArrayList<>(List.of("-classpath", System.getProperty("java.class.path"),
        "-d", classes.toString()));
    arguments.addAll(sources);
    assertThat(ToolProvider.getSystemJavaCompiler()).as("JDK compiler is required").isNotNull();
    assertThat(ToolProvider.getSystemJavaCompiler().run(null, null, null,
        arguments.toArray(String[]::new))).isZero();
    Path jar = root.resolve("plugin.jar");
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar));
        var paths = Files.walk(classes)) {
      for (Path file : paths.filter(Files::isRegularFile).toList()) {
        output.putNextEntry(new JarEntry(classes.relativize(file).toString().replace('\\', '/')));
        Files.copy(file, output);
        output.closeEntry();
      }
      output.putNextEntry(new JarEntry("META-INF/services/" + Plugin.class.getName()));
      output.write((brokenDescriptor ? "missing.Provider\n" : descriptor.toString())
          .getBytes(StandardCharsets.UTF_8));
      output.closeEntry();
      // A bundled API copy must never replace the parent's shared contract.
      String contract = Plugin.class.getName().replace('.', '/') + ".class";
      output.putNextEntry(new JarEntry(contract));
      try (var input = Plugin.class.getClassLoader().getResourceAsStream(contract)) {
        input.transferTo(output);
      }
      output.closeEntry();
    }
    return jar;
  }

  @Test
  void discoversRealServicesAndIsolatesBundleDependenciesWhileSharingApiTypes() throws Exception {
    Path first = bundle(List.of("zeta", "alpha"), false);
    Path second = bundle(List.of("alpha"), false);
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (var one = PluginDiscovery.load(Plugin.class, List.of(first));
        var two = PluginDiscovery.load(Plugin.class, List.of(second))) {
      assertThat(one.identifiers()).containsExactly("alpha", "zeta");
      assertThat(one.get("alpha")).isInstanceOf(Plugin.class);
      assertThat(one.get("alpha").toString()).isEqualTo("bundle");
      assertThat(fixture.Dependency.value()).isEqualTo("host");
      assertThat(one.get("alpha").getClass()).isNotEqualTo(two.get("alpha").getClass());
      assertThat(one.get("alpha").getClass().getClassLoader())
          .isNotSameAs(Plugin.class.getClassLoader());
      assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(previous);
      assertThatThrownBy(() -> one.get("missing")).isInstanceOf(PluginException.class);
      assertThatThrownBy(() -> one.identifiers().clear())
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  void rejectsDuplicateIdsBadIdsAndBrokenRegistrationsAndRestoresTccl() throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    for (Path jar : List.of(bundle(List.of("same", "same"), false),
        bundle(List.of("Bad Id"), false), bundle(List.of(), true))) {
      assertThatThrownBy(() -> PluginDiscovery.load(Plugin.class, List.of(jar)))
          .isInstanceOf(PluginException.class).hasCauseInstanceOf(Throwable.class);
      assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(previous);
    }
  }

  @Test
  void rejectsInvalidPathsAndEmptyBundles() {
    assertThatThrownBy(() -> PluginDiscovery.load(Plugin.class, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> PluginDiscovery.load(Plugin.class, List.of(temporary.resolve("absent.jar"))))
        .isInstanceOf(PluginException.class).hasMessageContaining("Not a plugin JAR");
  }

  @Test
  void emptyBundleDoesNotInheritHostServiceDescriptorsAndCloseIsIdempotent() throws Exception {
    Path jar = bundle(List.of(), false);
    var discovery = PluginDiscovery.load(Plugin.class, List.of(jar));
    assertThat(discovery.identifiers()).isEmpty();
    discovery.close();
    discovery.close();
    assertThatThrownBy(discovery::identifiers).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> discovery.get("any")).isInstanceOf(IllegalStateException.class);
    Files.delete(jar);
  }
}
