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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.engine.cluster.EngineNode;
import org.apache.seatunnel.engine.cluster.EngineSettings;
import org.apache.seatunnel.engine.dag.*;
import org.apache.seatunnel.engine.plan.*;
import org.apache.seatunnel.engine.rest.JobRestServer;
import org.apache.seatunnel.engine.submission.*;
import org.apache.seatunnel.engine.task.EngineTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Packaged CLI subprocess -> real Jetty -> real embedded engine, no HTTP or task mocks. */
class ClusterClientIT {
  @TempDir Path temporary;
  private int sequence;
  private record Result(int code,String output,String errors) {}
  private Result run(String... arguments) throws Exception {
    List<String> command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-jar",
        Path.of("target","seatunnel-cli-3.0.0-SNAPSHOT.jar").toAbsolutePath().toString()));
    command.addAll(List.of(arguments));
    Path output=temporary.resolve("stdout-"+sequence); Path errors=temporary.resolve("stderr-"+sequence++);
    Process process=new ProcessBuilder(command).redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
    try {
      assertThat(process.waitFor(30,TimeUnit.SECONDS)).as("CLI completed").isTrue();
      return new Result(process.exitValue(),Files.readString(output),Files.readString(errors));
    } finally { if(process.isAlive()) process.destroyForcibly().waitFor(); }
  }
  @Test void packagedClientSubmitsPollsCancelsAndDeletesRealJob() throws Exception {
    var settings=new EngineSettings("cli-rest-"+UUID.randomUUID(),"127.0.0.1",0,List.of(),2,16,4,Duration.ofSeconds(5));
    try(var node=EngineNode.start(settings);
        var service=new JobSubmissionService(node.master(),Map.of("installed", parameters -> {
          if(!parameters.equals(Map.of("mode","block"))) throw new IllegalArgumentException("mode required");
          EngineTask task=context -> new java.util.concurrent.CountDownLatch(1).await();
          return new SubmissionPlan(plan(),Map.of(new TaskId("s",0),task,new TaskId("t",0),task),null);
        }),1);
        var server=new JobRestServer(service,"127.0.0.1",0)) {
      server.start(); String endpoint="http://127.0.0.1:"+server.port();
      Path parameters=temporary.resolve("parameters.json"); Files.writeString(parameters,"{"mode":"block"}");
      Result submitted=run("cluster","submit","--endpoint",endpoint,"--definition","installed","--parameters",parameters.toString());
      assertThat(submitted.code()).as(submitted.errors()).isZero(); assertThat(submitted.errors()).isEmpty();
      String id=new ObjectMapper().readTree(submitted.output()).get("jobId").asText();
      Result listed=run("cluster","list","--endpoint",endpoint);
      assertThat(listed.code()).as(listed.errors()).isZero(); assertThat(listed.output()).contains(id);
      Result status=run("cluster","status","--endpoint",endpoint,"--job",id);
      assertThat(status.code()).as(status.errors()).isZero(); assertThat(status.output()).contains("RUNNING");
      Result premature=run("cluster","delete","--endpoint",endpoint,"--job",id);
      assertThat(premature.code()).isEqualTo(1); assertThat(premature.output()).isEmpty(); assertThat(premature.errors()).contains("409");
      Result cancelled=run("cluster","cancel","--endpoint",endpoint,"--job",id);
      assertThat(cancelled.code()).as(cancelled.errors()).isZero(); assertThat(cancelled.output()).contains("CANCELLED");
      Result deleted=run("cluster","delete","--endpoint",endpoint,"--job",id);
      assertThat(deleted.code()).as(deleted.errors()).isZero(); assertThat(deleted.output()).isEqualTo("{}\n");
      Result missing=run("cluster","status","--endpoint",endpoint,"--job",id);
      assertThat(missing.code()).isEqualTo(1); assertThat(missing.output()).isEmpty(); assertThat(missing.errors()).contains("404");
      Files.writeString(parameters,"{"mode":1}");
      Result invalid=run("cluster","submit","--endpoint",endpoint,"--definition","installed","--parameters",parameters.toString());
      assertThat(invalid.code()).isEqualTo(1); assertThat(invalid.output()).isEmpty();
    }
  }
  private static PhysicalPlan plan() {
    var table=new CatalogTable("events",List.of(new CatalogTable.Column("id",CatalogTable.DataType.LONG,false)));
    return new PhysicalPlanner().plan(new LogicalDag(List.of(
      new LogicalNode("s",LogicalNode.Kind.SOURCE,"installed-source",1,table),
      new LogicalNode("t",LogicalNode.Kind.SINK,"installed-sink",1,table)),
      List.of(new LogicalEdge("s","t",LogicalEdge.Exchange.FORWARD))));
  }
}
