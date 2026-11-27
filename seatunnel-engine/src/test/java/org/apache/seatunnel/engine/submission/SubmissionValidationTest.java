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
package org.apache.seatunnel.engine.submission;

import static org.assertj.core.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SubmissionValidationTest {
  @Test
  void boundsAndDetachesParametersAndValidatesNames() {
    assertThatThrownBy(() -> JobSubmissionService.validateDefinition("../class"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> JobSubmissionService.validateDefinition(null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> JobSubmissionService.validateParameters(null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> JobSubmissionService.validateParameters(Map.of("key", "x".repeat(4097))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> JobSubmissionService.validateParameters(Map.of("bad key", "value")))
        .isInstanceOf(IllegalArgumentException.class);
    var tooMany = new java.util.HashMap<String, String>();
    for (int i = 0; i < 65; i++) tooMany.put("key" + i, "value");
    assertThatThrownBy(() -> JobSubmissionService.validateParameters(tooMany))
        .isInstanceOf(IllegalArgumentException.class);
    var original = new java.util.HashMap<String, String>();
    original.put("key", "value");
    var detached = JobSubmissionService.validateParameters(original);
    original.clear();
    assertThat(detached).containsEntry("key", "value");
    assertThatThrownBy(() -> detached.put("x", "y"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
