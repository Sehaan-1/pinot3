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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.seatunnel.engine.submission.JobSubmissionService;
import org.apache.seatunnel.engine.submission.SubmissionException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/** Explicitly bound, trusted-network HTTP adapter. Does not own the service or engine node. */
public final class JobRestServer implements AutoCloseable {
  public static final int MAX_BODY_BYTES = 65536;
  private final Server server;
  private final ServerConnector connector;
  private final ObjectMapper json = JsonMapper.builder()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
      .withCoercionConfig(com.fasterxml.jackson.databind.type.LogicalType.Textual, config -> {
        config.setCoercion(com.fasterxml.jackson.databind.cfg.CoercionInputShape.Integer, com.fasterxml.jackson.databind.cfg.CoercionAction.Fail);
        config.setCoercion(com.fasterxml.jackson.databind.cfg.CoercionInputShape.Float, com.fasterxml.jackson.databind.cfg.CoercionAction.Fail);
        config.setCoercion(com.fasterxml.jackson.databind.cfg.CoercionInputShape.Boolean, com.fasterxml.jackson.databind.cfg.CoercionAction.Fail);
      }).build();
  private record SubmitRequest(String definition, Map<String, String> parameters) {}

  public JobRestServer(JobSubmissionService service, String bindAddress, int port) {
    Objects.requireNonNull(service, "service");
    if (bindAddress == null || bindAddress.isBlank() || port < 0 || port > 65535)
      throw new IllegalArgumentException("Explicit bind address and valid port required");
    QueuedThreadPool threads = new QueuedThreadPool(16, 4, 30000, new ArrayBlockingQueue<>(128));
    threads.setName("seatunnel-rest");
    threads.setReservedThreads(0);
    server = new Server(threads);
    server.setStopTimeout(5000);
    server.setStopAtShutdown(false);
    HttpConfiguration http = new HttpConfiguration();
    http.setRequestHeaderSize(8192);
    http.setSendServerVersion(false);
    connector = new ServerConnector(server, 1, 1, new HttpConnectionFactory(http));
    connector.setHost(bindAddress);
    connector.setPort(port);
    connector.setIdleTimeout(10000);
    server.addConnector(connector);
    server.setHandler(new AbstractHandler() {
      @Override public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response) throws IOException {
        base.setHandled(true);
        response.setHeader("Cache-Control", "no-store");
        response.setHeader("X-Content-Type-Options", "nosniff");
        try { route(service, target, request, response); }
        catch (SubmissionException error) {
          send(response, error.reason() == SubmissionException.Reason.NOT_FOUND ? 404 : 409, Map.of("error",error.getMessage()));
        } catch (JsonProcessingException | IllegalArgumentException invalid) {
          send(response,400,Map.of("error","Invalid request"));
        } catch (RuntimeException failure) {
          send(response,500,Map.of("error","Internal server error"));
        }
      }
    });
  }
  public synchronized void start() throws Exception {
    if (server.isStarted()) throw new IllegalStateException("Server already started");
    try { server.start(); }
    catch (Exception failure) {
      try { server.stop(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }
  public int port() {
    int port = connector.getLocalPort();
    if (port <= 0) throw new IllegalStateException("Server not started");
    return port;
  }
  private void route(JobSubmissionService service, String path, HttpServletRequest request, HttpServletResponse response) throws IOException {
    String method = request.getMethod();
    if ("/v1/jobs".equals(path)) {
      if ("GET".equals(method)) { send(response,200,Map.of("jobs",service.list())); return; }
      if (!"POST".equals(method)) { method(response,"GET, POST"); return; }
      String type = request.getContentType();
      if (type == null || !type.split(";",2)[0].trim().toLowerCase(Locale.ROOT).equals("application/json")) {
        send(response,415,Map.of("error","application/json required")); return;
      }
      if (request.getContentLengthLong() > MAX_BODY_BYTES) { tooLarge(response); return; }
      byte[] body = request.getInputStream().readNBytes(MAX_BODY_BYTES+1);
      if (body.length > MAX_BODY_BYTES) { tooLarge(response); return; }
      SubmitRequest submission = json.readValue(body,SubmitRequest.class);
      if (submission == null) throw new IllegalArgumentException("Request required");
      var accepted = service.submit(submission.definition(), submission.parameters());
      response.setHeader("Location", "/v1/jobs/"+accepted.jobId());
      send(response,202,accepted); return;
    }
    if (!path.startsWith("/v1/jobs/")) { send(response,404,Map.of("error","Unknown endpoint")); return; }
    String suffix = path.substring("/v1/jobs/".length());
    boolean cancel = suffix.endsWith("/cancel");
    String text = cancel ? suffix.substring(0,suffix.length()-7) : suffix;
    if (!text.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
      throw new IllegalArgumentException("Invalid job ID");
    UUID id = UUID.fromString(text);
    if (cancel) {
      if (!"POST".equals(method)) { method(response,"POST"); return; }
      send(response,200,service.cancel(id));
    } else if ("GET".equals(method)) send(response,200,service.status(id));
    else if ("DELETE".equals(method)) { service.delete(id); response.setStatus(204); }
    else method(response,"GET, DELETE");
  }
  private void tooLarge(HttpServletResponse response) throws IOException { send(response,413,Map.of("error","Request body too large")); }
  private void method(HttpServletResponse response,String allow) throws IOException {
    response.setHeader("Allow",allow); send(response,405,Map.of("error","Method not allowed"));
  }
  private void send(HttpServletResponse response,int status,Object value) throws IOException {
    response.setStatus(status);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    response.getOutputStream().write(json.writeValueAsBytes(value));
  }
  @Override public synchronized void close() throws Exception { server.stop(); }
}
