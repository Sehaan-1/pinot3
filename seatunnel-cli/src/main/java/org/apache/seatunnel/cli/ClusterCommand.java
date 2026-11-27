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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

/** HTTP-only client: no Hazelcast/engine dependency in the packaged CLI. Never retries POST. */
@Command(
    name = "cluster",
    mixinStandardHelpOptions = true,
    subcommands = {
      ClusterCommand.Submit.class,
      ClusterCommand.ListJobs.class,
      ClusterCommand.Status.class,
      ClusterCommand.Cancel.class,
      ClusterCommand.Delete.class
    })
public final class ClusterCommand implements Runnable {
  @Spec CommandSpec spec;

  @Override
  public void run() {
    spec.commandLine().usage(spec.commandLine().getOut());
  }

  abstract static class RequestCommand implements Callable<Integer> {
    @Option(
        names = "--endpoint",
        required = true,
        description = "Trusted server origin, e.g. http://127.0.0.1:8080")
    URI endpoint;

    @Spec CommandSpec spec;
    final ObjectMapper json =
        JsonMapper.builder()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    abstract String method();

    abstract String path();

    String body() throws Exception {
      return "";
    }

    @Override
    public Integer call() {
      try {
        if (!("http".equals(endpoint.getScheme()) || "https".equals(endpoint.getScheme()))
            || endpoint.getHost() == null
            || endpoint.getUserInfo() != null
            || endpoint.getRawQuery() != null
            || endpoint.getRawFragment() != null
            || !(endpoint.getPath().isEmpty() || endpoint.getPath().equals("/")))
          throw new IllegalArgumentException(
              "Endpoint must be an HTTP(S) origin without credentials, query or path");
        URI uri = endpoint.resolve(path());
        String payload = body();
        HttpRequest request =
            HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(20))
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .method(method(), HttpRequest.BodyPublishers.ofString(payload))
                .build();
        HttpClient client =
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        var pending = client.sendAsync(request, ignored -> new LimitedBody());
        HttpResponse<byte[]> response;
        try {
          response = pending.get(20, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
          pending.cancel(true);
        }
        byte[] responseBody = response.body();
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          spec.commandLine()
              .getErr()
              .println("Cluster request failed: HTTP " + response.statusCode());
          return 1;
        }
        if (response.statusCode() == 204) {
          spec.commandLine().getOut().println("{}");
          return 0;
        }
        if (!response
            .headers()
            .firstValue("Content-Type")
            .orElse("")
            .split(";", 2)[0]
            .trim()
            .equalsIgnoreCase("application/json"))
          throw new IllegalArgumentException("Expected JSON response");
        var decoded = json.readTree(responseBody);
        if (decoded == null || !decoded.isObject())
          throw new IllegalArgumentException("Expected JSON object");
        spec.commandLine().getOut().println(json.writeValueAsString(decoded));
        return 0;
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        spec.commandLine()
            .getErr()
            .println("Cluster request interrupted; submission outcome may be unknown");
        return 1;
      } catch (Exception error) {
        spec.commandLine()
            .getErr()
            .println("Cluster request failed; inspect server state before retrying a submission");
        return 1;
      }
    }
  }

  /** Wrap the JDK subscriber solely to impose an allocation bound; HttpClient owns transport. */
  private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final HttpResponse.BodySubscriber<byte[]> delegate =
        HttpResponse.BodySubscribers.ofByteArray();
    private java.util.concurrent.Flow.Subscription subscription;
    private long bytes;

    public java.util.concurrent.CompletionStage<byte[]> getBody() {
      return delegate.getBody();
    }

    public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
      this.subscription = subscription;
      delegate.onSubscribe(subscription);
    }

    public void onNext(java.util.List<java.nio.ByteBuffer> chunks) {
      for (var chunk : chunks) bytes += chunk.remaining();
      if (bytes > 65536) {
        subscription.cancel();
        delegate.onError(new java.io.IOException("Response too large"));
      } else delegate.onNext(chunks);
    }

    public void onError(Throwable error) {
      delegate.onError(error);
    }

    public void onComplete() {
      delegate.onComplete();
    }
  }

  @Command(name = "submit", mixinStandardHelpOptions = true)
  public static final class Submit extends RequestCommand {
    @Option(names = "--definition", required = true)
    String definition;

    @Option(names = "--parameters", description = "UTF-8 JSON object of string parameters")
    Path parameters;

    String method() {
      return "POST";
    }

    String path() {
      return "/v1/jobs";
    }

    @Override
    String body() throws Exception {
      if (!definition.matches("[a-z][a-z0-9-]{0,63}"))
        throw new IllegalArgumentException("Invalid definition");
      Map<String, String> values = Map.of();
      if (parameters != null) {
        byte[] data;
        try (InputStream input = Files.newInputStream(parameters)) {
          data = input.readNBytes(65537);
        }
        if (data.length > 65536) throw new IllegalArgumentException("Parameters too large");
        var tree = json.readTree(data);
        if (tree == null || !tree.isObject() || tree.size() > 64)
          throw new IllegalArgumentException("Invalid parameters");
        var fields = tree.fields();
        while (fields.hasNext()) {
          var entry = fields.next();
          if (!entry.getKey().matches("[a-zA-Z][a-zA-Z0-9_.-]{0,63}")
              || !entry.getValue().isTextual()
              || entry.getValue().textValue().length() > 4096)
            throw new IllegalArgumentException("Invalid parameter");
        }
        values = json.convertValue(tree, new TypeReference<Map<String, String>>() {});
      }
      String body = json.writeValueAsString(Map.of("definition", definition, "parameters", values));
      if (body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 65536)
        throw new IllegalArgumentException("Request too large");
      return body;
    }
  }

  @Command(name = "list", mixinStandardHelpOptions = true)
  public static final class ListJobs extends RequestCommand {
    String method() {
      return "GET";
    }

    String path() {
      return "/v1/jobs";
    }
  }

  abstract static class JobCommand extends RequestCommand {
    @Option(names = "--job", required = true)
    UUID job;

    String path() {
      return "/v1/jobs/" + job;
    }
  }

  @Command(name = "status", mixinStandardHelpOptions = true)
  public static final class Status extends JobCommand {
    String method() {
      return "GET";
    }
  }

  @Command(name = "cancel", mixinStandardHelpOptions = true)
  public static final class Cancel extends JobCommand {
    String method() {
      return "POST";
    }

    @Override
    String path() {
      return super.path() + "/cancel";
    }
  }

  @Command(name = "delete", mixinStandardHelpOptions = true)
  public static final class Delete extends JobCommand {
    String method() {
      return "DELETE";
    }
  }
}
