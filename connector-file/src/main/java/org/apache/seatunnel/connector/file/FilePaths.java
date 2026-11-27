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
package org.apache.seatunnel.connector.file;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/** Best-effort no-symlink validation in a trusted, exclusively owned directory tree. */
final class FilePaths {
  private FilePaths() {}

  static Path resolve(FileConfig config, String relative) throws IOException {
    FileConfig.child(relative);
    Path current = config.root.getRoot();
    for (Path part : config.root) {
      current = current.resolve(part);
      directory(current);
    }
    Path target = config.root.resolve(relative);
    current = config.root;
    for (Path part : config.root.relativize(target.getParent())) {
      current = current.resolve(part);
      directory(current);
    }
    if (Files.isSymbolicLink(target)) throw FileConfig.failure();
    return target;
  }

  static void directory(Path path) throws IOException {
    if (!Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
            .isDirectory()
        || Files.isSymbolicLink(path)) throw FileConfig.failure();
  }

  static FileSource.Split stat(FileConfig config, String relative) throws IOException {
    Path path = resolve(config, relative);
    var a = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!a.isRegularFile() || a.size() > config.maxFile) throw FileConfig.failure();
    return new FileSource.Split(
        relative, a.size(), a.lastModifiedTime().toString(), String.valueOf(a.fileKey()));
  }

  static void unchanged(FileConfig config, FileSource.Split split) throws IOException {
    if (!stat(config, split.path()).equals(split)) throw FileConfig.failure();
  }
}
