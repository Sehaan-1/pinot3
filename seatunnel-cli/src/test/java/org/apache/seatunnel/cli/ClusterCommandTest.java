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
package org.apache.seatunnel.cli;

import static org.assertj.core.api.Assertions.*;

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

class ClusterCommandTest {
  @Test
  void helpDoesNotRequireLocalConfigAndInvalidEndpointNeverMakesRequest() {
    var out = new StringWriter();
    var err = new StringWriter();
    assertThat(SeaTunnelCommand.execute(new String[] {"cluster", "--help"}, out, err)).isZero();
    assertThat(out.toString()).contains("submit", "cancel");
    assertThat(err.toString()).isEmpty();
    out = new StringWriter();
    err = new StringWriter();
    assertThat(
            SeaTunnelCommand.execute(
                new String[] {
                  "cluster",
                  "status",
                  "--endpoint",
                  "file:///tmp/no",
                  "--job",
                  "00000000-0000-0000-0000-000000000000"
                },
                out,
                err))
        .isEqualTo(1);
    assertThat(out.toString()).isEmpty();
    assertThat(err.toString()).contains("Cluster request failed");
  }

  @Test
  void missingLocalConfigStillReturnsUsageError() {
    assertThat(SeaTunnelCommand.execute(new String[] {}, new StringWriter(), new StringWriter()))
        .isEqualTo(2);
  }
}
