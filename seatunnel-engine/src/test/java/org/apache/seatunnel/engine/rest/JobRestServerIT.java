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
package org.apache.seatunnel.engine.rest;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.engine.cluster.EngineNode;
import org.apache.seatunnel.engine.cluster.EngineSettings;
import org.apache.seatunnel.engine.dag.*;
import org.apache.seatunnel.engine.plan.*;
import org.apache.seatunnel.engine.submission.*;
import org.apache.seatunnel.engine.task.*;
import org.junit.jupiter.api.*;

class JobRestServerIT {
  private static EngineNode leader, worker;
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private JobSubmissionService service;
  private JobRestServer server;
  private String origin;
  private String resultMap;

  @BeforeAll
  static void cluster() throws Exception {
    String cluster = "rest-it-" + UUID.randomUUID();
    leader = EngineNode.start(settings(cluster, List.of()));
    try {
      worker =
          EngineNode.start(
              settings(
                  cluster,
                  List.of(
                      "127.0.0.1:"
                          + leader.member().getCluster().getLocalMember().getAddress().getPort())));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(() -> assertThat(leader.member().getCluster().getMembers()).hasSize(2));
    } catch (Throwable failure) {
      shutdown();
      throw failure;
    }
  }

  private static EngineSettings settings(String name, List<String> seeds) {
    return new EngineSettings(name, "127.0.0.1", 0, seeds, 2, 16, 8, Duration.ofSeconds(5));
  }

  @AfterAll
  static void shutdown() throws Exception {
    try {
      if (worker != null) worker.close();
    } finally {
      if (leader != null) leader.close();
    }
  }

  @BeforeEach
  void http() throws Exception {
    resultMap = "rest-results-" + UUID.randomUUID();
    service =
        new JobSubmissionService(
            leader.master(),
            Map.of(
                "installed",
                parameters -> {
                  String action = parameters.getOrDefault("action", "success");
                  if (!List.of("success", "block", "fail").contains(action))
                    throw new IllegalArgumentException("bad action");
                  EngineTask task = new Work(resultMap, action);
                  return new SubmissionPlan(
                      plan(),
                      Map.of(new TaskId("s", 0), task, new TaskId("t", 0), task),
                      worker.member().getCluster().getLocalMember().getUuid());
                }),
            1);
    server = new JobRestServer(service, "127.0.0.1", 0);
    server.start();
    origin = "http://127.0.0.1:" + server.port();
  }

  @AfterEach
  void stopHttp() throws Exception {
    try {
      if (server != null) server.close();
    } finally {
      if (service != null) service.close();
      leader.member().getMap(resultMap).destroy();
    }
  }

  private static PhysicalPlan plan() {
    CatalogTable table =
        new CatalogTable(
            "events", List.of(new CatalogTable.Column("id", CatalogTable.DataType.LONG, false)));
    return new PhysicalPlanner()
        .plan(
            new LogicalDag(
                List.of(
                    new LogicalNode("s", LogicalNode.Kind.SOURCE, "installed-source", 1, table),
                    new LogicalNode("t", LogicalNode.Kind.SINK, "installed-sink", 1, table)),
                List.of(new LogicalEdge("s", "t", LogicalEdge.Exchange.FORWARD))));
  }

  private HttpResponse<String> request(String method, String path, String body) throws Exception {
    return CLIENT.send(
        HttpRequest.newBuilder(URI.create(origin + path))
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private String submit(String action) throws Exception {
    var response =
        request(
            "POST",
            "/v1/jobs",
            JSON.writeValueAsString(
                Map.of("definition", "installed", "parameters", Map.of("action", action))));
    assertThat(response.statusCode()).as(response.body()).isEqualTo(202);
    String id = JSON.readTree(response.body()).get("jobId").asText();
    assertThat(response.headers().firstValue("Location")).contains("/v1/jobs/" + id);
    return "/v1/jobs/" + id;
  }

  private String status(String path) throws Exception {
    return JSON.readTree(request("GET", path, "").body()).get("status").asText();
  }

  @Test
  void submitsRealRemoteTasksAndRetainsTerminalStatusUntilDelete() throws Exception {
    assertThat(JSON.readTree(request("GET", "/v1/jobs", "").body()).get("jobs").size()).isZero();
    String path = submit("success");
    assertThat(JSON.readTree(request("GET", "/v1/jobs", "").body()).get("jobs").size())
        .isEqualTo(1);
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(status(path)).isEqualTo("SUCCEEDED"));
    Map<TaskId, UUID> results = leader.member().getMap(resultMap);
    assertThat(results).hasSize(2);
    assertThat(results.values())
        .containsOnly(worker.member().getCluster().getLocalMember().getUuid());
    assertThat(
            request("POST", "/v1/jobs", "{\"definition\":\"installed\",\"parameters\":{}}")
                .statusCode())
        .isEqualTo(409);
    assertThat(request("DELETE", path, "").statusCode()).isEqualTo(204);
    assertThat(request("GET", path, "").statusCode()).isEqualTo(404);
    submit("success");
  }

  @Test
  void cancelsOwnedWorkAndRejectsDeletingRunningJob() throws Exception {
    String path = submit("block");
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(leader.member().getMap(resultMap).size()).isEqualTo(2));
    assertThat(request("DELETE", path, "").statusCode()).isEqualTo(409);
    assertThat(request("POST", path + "/cancel", "").statusCode()).isEqualTo(200);
    assertThat(status(path)).isEqualTo("CANCELLED");
    assertThat(request("DELETE", path, "").statusCode()).isEqualTo(204);
  }

  @Test
  void reportsTaskFailureWithoutLeakingExceptionDetails() throws Exception {
    String path = submit("fail");
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(status(path)).isEqualTo("FAILED"));
    assertThat(request("GET", path, "").body()).doesNotContain("secret-failure");
  }

  @Test
  void rejectsMalformedAmbiguousOversizedAndUnknownRequests() throws Exception {
    for (String body :
        List.of(
            "null",
            "{}",
            "{",
            "{\"definition\":\"installed\",\"parameters\":{},\"extra\":1}",
            "{\"definition\":\"installed\",\"definition\":\"other\",\"parameters\":{}}",
            "{\"definition\":\"installed\",\"parameters\":{\"action\":12}}",
            "{\"definition\":\"installed\",\"parameters\":{}} {}"))
      assertThat(request("POST", "/v1/jobs", body).statusCode()).as(body).isEqualTo(400);
    assertThat(request("POST", "/v1/jobs", " ".repeat(65537)).statusCode()).isEqualTo(413);
    assertThat(
            request("POST", "/v1/jobs", "{\"definition\":\"unknown\",\"parameters\":{}}")
                .statusCode())
        .isEqualTo(404);
    var method = request("PUT", "/v1/jobs", "");
    assertThat(method.statusCode()).isEqualTo(405);
    assertThat(method.headers().firstValue("Allow")).contains("GET, POST");
    assertThat(request("GET", "/v1/jobs/bad-id", "").statusCode()).isEqualTo(400);
    assertThat(request("GET", "/unknown", "").statusCode()).isEqualTo(404);
    var unsupported =
        CLIENT.send(
            HttpRequest.newBuilder(URI.create(origin + "/v1/jobs"))
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(unsupported.statusCode()).isEqualTo(415);
  }

  @Test
  void nonLeaderAndClosedServiceRejectSubmissionsWithoutOwningNode() throws Exception {
    try (var follower =
        new JobSubmissionService(
            worker.master(),
            Map.of(
                "installed",
                parameters -> {
                  throw new AssertionError("must not invoke factory");
                }),
            1)) {
      assertThatThrownBy(() -> follower.submit("installed", Map.of()))
          .isInstanceOf(SubmissionException.class);
    }
    service.close();
    service.close();
    assertThat(
            request("POST", "/v1/jobs", "{\"definition\":\"installed\",\"parameters\":{}}")
                .statusCode())
        .isEqualTo(409);
    assertThat(leader.member().getLifecycleService().isRunning()).isTrue();
  }

  private record Work(String map, String action) implements EngineTask {
    public void run(TaskContext context) throws Exception {
      context.cluster().getMap(map).put(context.key().taskId(), context.workerId());
      if (action.equals("fail")) throw new IllegalStateException("secret-failure");
      if (action.equals("block")) new java.util.concurrent.CountDownLatch(1).await();
    }
  }
}
