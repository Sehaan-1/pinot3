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

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.seatunnel.config.Config;
import org.apache.seatunnel.core.JobConfig;
import org.apache.seatunnel.core.LocalRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Local-only CLI; stdout is reserved for data rows, stderr for diagnostics. */
@Command(
    name = "seatunnel",
    mixinStandardHelpOptions = true,
    version = "SeaTunnel local baseline 3.0.0-SNAPSHOT")
public final class SeaTunnelCommand implements Callable<Integer> {
  @Option(names = "--config", required = true, description = "HOCON job configuration file")
  private Path config;

  @Option(names = "--mode", defaultValue = "local", description = "Execution mode (local only)")
  private String mode;

  private final Writer output;

  private SeaTunnelCommand(Writer output) {
    this.output = output;
  }

  @Override
  public Integer call() throws Exception {
    if (!"local".equals(mode)) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "Only --mode local is supported");
    }
    new LocalRunner().run(JobConfig.parse(Config.parseFile(config.toFile())), output);
    return 0;
  }

  public static int execute(String[] arguments, Writer output, Writer errors) {
    CommandLine command = new CommandLine(new SeaTunnelCommand(output));
    command.setOut(new PrintWriter(output, true));
    command.setErr(new PrintWriter(errors, true));
    command.setExecutionExceptionHandler(
        (failure, cli, parsed) -> {
          if (failure instanceof CommandLine.ParameterException) {
            cli.getErr().println(failure.getMessage());
            return 2;
          }
          cli.getErr().println("Job failed: " + failure.getMessage());
          return 1;
        });
    return command.execute(arguments);
  }

  public static void main(String[] arguments) {
    int code =
        execute(
            arguments,
            new OutputStreamWriter(System.out, StandardCharsets.UTF_8),
            new OutputStreamWriter(System.err, StandardCharsets.UTF_8));
    if (code == 0 && System.out.checkError()) {
      // PrintStream swallows IOExceptions; do not report success after a broken stdout write.
      System.err.println("Job failed: standard output write failed");
      code = 1;
    }
    System.exit(code);
  }
}
