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
package org.apache.seatunnel.connector.jdbc;

import java.sql.*;
import java.util.*;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.table.*;

/** INSERT-only batched transactions, no retries, external committer or exactly-once claim. */
public final class JdbcSink implements SeaTunnelSink<JdbcSource.State, String> {
  private final JdbcConfig config;
  private final JdbcSession.Factory sessions;

  JdbcSink(JdbcConfig config) {
    this(config, JdbcSession::open);
  }

  JdbcSink(JdbcConfig config, JdbcSession.Factory sessions) {
    this.config = config;
    this.sessions = sessions;
  }

  public Optional<SinkCommitter<String>> createCommitter() {
    return Optional.empty();
  }

  public SinkWriter<JdbcSource.State, String> createWriter(
      int id, Optional<JdbcSource.State> restored) throws Exception {
    JdbcSource.initial(id, restored);
    var writer = new Writer();
    writer.initialize();
    return writer;
  }

  private final class Writer extends JdbcLifecycle implements SinkWriter<JdbcSource.State, String> {
    private JdbcSession session;
    private PreparedStatement statement;
    private int pending;

    void initialize() throws Exception {
      begin();
      try {
        session = sessions.open(config, false);
        statement =
            session.connection.prepareStatement(
                "INSERT INTO "
                    + config.relation()
                    + " ("
                    + config.columnSql()
                    + ") VALUES ("
                    + String.join(",", Collections.nCopies(config.catalog.columns().size(), "?"))
                    + ")");
        statement.setQueryTimeout(config.timeout);
      } catch (Exception e) {
        failed = true;
        try {
          close();
        } catch (Exception ignored) {
        }
        throw JdbcSession.databaseFailure();
      }
    }

    public void write(SeaTunnelRow row) throws Exception {
      active();
      try {
        Objects.requireNonNull(row);
        if (row.getRowKind() != RowKind.INSERT)
          throw new IllegalArgumentException("JDBC sink supports INSERT only");
        if (row.getArity() != config.catalog.columns().size())
          throw new IllegalArgumentException("JDBC row arity mismatch");
        for (int i = 0; i < row.getArity(); i++)
          JdbcTypes.bind(statement, i + 1, row.getField(i), config.catalog.columns().get(i));
        statement.addBatch();
        pending++;
        if (pending >= config.batchSize) flush(false);
      } catch (Exception failure) {
        abort(failure);
      }
    }

    public void flush(boolean endOfInput) throws Exception {
      active();
      try {
        if (pending == 0) return;
        int[] results = statement.executeBatch();
        if (results.length != pending) throw new SQLException("Unexpected JDBC batch result");
        for (int result : results)
          if (result != Statement.SUCCESS_NO_INFO && result != 1)
            throw new SQLException("JDBC insert not acknowledged");
        statement.clearBatch();
        session.connection.commit();
        pending = 0;
      } catch (Exception failure) {
        abort(failure);
      }
    }

    private void abort(Exception failure) throws Exception {
      failed = true;
      try {
        close();
      } catch (Exception ignored) {
      }
      if (failure instanceof SQLException) throw JdbcSession.databaseFailure();
      throw failure;
    }

    public List<String> prepareCommit(long id) {
      throw new UnsupportedOperationException("JDBC external commits are not supported");
    }

    public void close() throws Exception {
      if (closed) return;
      closed = true;
      pending = 0;
      try {
        try {
          if (statement != null) statement.close();
        } finally {
          if (session != null) session.close();
        }
      } catch (Exception failure) {
        throw JdbcSession.databaseFailure();
      }
    }
  }
}
