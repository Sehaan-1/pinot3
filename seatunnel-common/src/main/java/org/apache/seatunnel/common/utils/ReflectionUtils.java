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
package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.common.exception.SeaTunnelException;

/** Reflection helpers for loading classes and creating instances by name. */
public final class ReflectionUtils {

  private ReflectionUtils() {}

  /** Loads the class with the given fully-qualified name. */
  public static Class<?> loadClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new SeaTunnelException("Class not found: " + className, e);
    }
  }

  /** Creates a new instance of the given class via its no-arg constructor. */
  public static <T> T newInstance(Class<T> clazz) {
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new SeaTunnelException("Failed to instantiate class: " + clazz.getName(), e);
    }
  }

  /** Loads the class with the given name and creates a new instance of it. */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className) {
    return (T) newInstance(loadClass(className));
  }
}
