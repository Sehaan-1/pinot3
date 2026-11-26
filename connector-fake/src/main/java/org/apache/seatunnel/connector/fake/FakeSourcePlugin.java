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
package org.apache.seatunnel.connector.fake;

import java.util.Map;
import java.util.Set;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.exception.ConfigException;
import org.apache.seatunnel.plugin.SourcePlugin;

/** ServiceLoader entry point for the bounded deterministic fake source. */
public final class FakeSourcePlugin implements SourcePlugin {
  @Override
  public String identifier() {
    return "fake";
  }

  @Override
  public FakeSource createSource(Map<String, Object> values) {
    Set<String> allowed = Set.of("rows", "start", "split-size");
    for (String key : values.keySet()) {
      if (!allowed.contains(key)) {
        throw new ConfigException("Unknown fake option: " + key);
      }
    }
    Options options = new Options(values);
    return new FakeSource(
        options.get(Option.withDefault("rows", Integer.class, 10)),
        options.get(Option.withDefault("start", Integer.class, 0)),
        options.get(Option.withDefault("split-size", Integer.class, 1000)));
  }
}
