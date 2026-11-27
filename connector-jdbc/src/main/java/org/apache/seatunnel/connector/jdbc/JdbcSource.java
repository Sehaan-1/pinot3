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
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.*;

/** Bounded JDBC cursor scan; explicit half-open ranges, no ordering or restore claim. */
public final class JdbcSource
    implements SeaTunnelSource<JdbcSource.Split, JdbcSource.State, JdbcSource.State> {
  private final JdbcConfig config;
  private final JdbcSession.Factory sessions;

  JdbcSource(JdbcConfig config) {
    this(config, JdbcSession::open);
  }

  JdbcSource(JdbcConfig config, JdbcSession.Factory sessions) {
    this.config = config;
    this.sessions = sessions;
  }

  public record State() implements CheckpointState {}

  public record Split(int index, Long lower, Long upper) implements SourceSplit {
    public Split {
      if (index < 0 || (lower == null) != (upper == null) || (lower != null && lower >= upper))
        throw new IllegalArgumentException("Invalid JDBC split");
    }

    public String splitId() {
      return Integer.toString(index);
    }
  }

  public CatalogTable producedTable() {
    return config.catalog;
  }

  public Boundedness boundedness() {
    return Boundedness.BOUNDED;
  }

  static void initial(int id, Optional<State> restored) {
    if (id != 0 || restored.isPresent())
      throw new IllegalArgumentException("JDBC requires reader0 without restore");
  }

  public SourceReader<Split, State> createReader(int id, Optional<State> restored) {
    initial(id, restored);
    return new Reader();
  }

  public SplitEnumerator<Split, State> createEnumerator(
      SplitEnumerator.Context<Split> context, Optional<State> restored) {
    initial(0, restored);
    Objects.requireNonNull(context);
    if (context.currentParallelism() != 1)
      throw new IllegalArgumentException("JDBC baseline supports one reader");
    return new Enumerator(context);
  }

  private final class Enumerator extends JdbcLifecycle implements SplitEnumerator<Split, State> {
    private final Context<Split> context;
    boolean registered, assigned;

    Enumerator(Context<Split> context) {
      this.context = context;
    }

    public void start() {
      begin();
    }

    public void registerReader(int id) {
      active();
      initial(id, Optional.empty());
      registered = true;
    }

    public void handleSplitRequest(int id) throws Exception {
      active();
      initial(id, Optional.empty());
      if (!registered) throw new IllegalStateException("Reader not registered");
      if (!assigned) {
        try {
          context.assignSplits(0, config.splits);
          context.signalNoMoreSplits(0);
          assigned = true;
        } catch (Exception e) {
          failed = true;
          throw e;
        }
      }
    }

    public void addSplitsBack(List<Split> splits, int id) {
      throw new UnsupportedOperationException("JDBC recovery is not supported");
    }

    public void close() {
      closed = true;
    }
  }

  private final class Reader extends JdbcLifecycle implements SourceReader<Split, State> {
    private final Deque<Split> pending = new ArrayDeque<>();
    private final Set<Split> accepted = new HashSet<>();
    private JdbcSession session;
    private PreparedStatement statement;
    private ResultSet result;
    private boolean noMore;

    public void start() throws Exception {
      begin();
      try {
        session = sessions.open(config, true);
      } catch (Exception e) {
        failed = true;
        throw JdbcSession.databaseFailure();
      }
    }

    public void addSplits(List<Split> splits) {
      active();
      if (noMore) throw new IllegalStateException("No more JDBC splits allowed");
      var copy = List.copyOf(splits);
      var unique = new HashSet<>(copy);
      if (unique.size() != copy.size()
          || !config.splits.containsAll(copy)
          || copy.stream().anyMatch(accepted::contains))
        throw new IllegalArgumentException("Foreign or duplicate JDBC split");
      pending.addAll(copy);
      accepted.addAll(copy);
    }

    public void notifyNoMoreSplits() {
      active();
      noMore = true;
    }

    public ReadStatus pollNext(Output output) throws Exception {
      active();
      Objects.requireNonNull(output);
      try {
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("JDBC read interrupted");
        if (result == null) {
          Split split = pending.peekFirst();
          if (split == null) return noMore ? ReadStatus.END_OF_INPUT : ReadStatus.NOTHING_AVAILABLE;
          String sql = "SELECT " + config.columnSql() + " FROM " + config.relation();
          if (config.partitionColumn != null)
            sql +=
                " WHERE "
                    + config.quote(config.partitionColumn)
                    + ">=? AND "
                    + config.quote(config.partitionColumn)
                    + "<?";
          statement =
              session.connection.prepareStatement(
                  sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
          statement.setFetchSize(config.fetchSize);
          statement.setQueryTimeout(config.timeout);
          if (config.partitionColumn != null) {
            statement.setLong(1, split.lower());
            statement.setLong(2, split.upper());
          }
          result = statement.executeQuery();
          JdbcTypes.validateMetadata(result.getMetaData(), config);
        }
        if (!result.next()) {
          closeCursor();
          pending.removeFirst();
          return pending.isEmpty() && noMore ? ReadStatus.END_OF_INPUT : ReadStatus.MORE_AVAILABLE;
        }
        Object[] fields = new Object[config.catalog.columns().size()];
        for (int i = 0; i < fields.length; i++)
          fields[i] = JdbcTypes.read(result, i + 1, config.catalog.columns().get(i));
        output.collect(new SeaTunnelRow(fields));
        return ReadStatus.MORE_AVAILABLE;
      } catch (Exception failure) {
        failed = true;
        try {
          close();
        } catch (Exception ignored) {
          /* driver details deliberately redacted */
        }
        if (failure instanceof SQLException) throw JdbcSession.databaseFailure();
        throw failure;
      }
    }

    private void closeCursor() throws SQLException {
      try {
        if (result != null) result.close();
      } finally {
        result = null;
        try {
          if (statement != null) statement.close();
        } finally {
          statement = null;
        }
      }
    }

    public void close() throws Exception {
      if (closed) return;
      closed = true;
      pending.clear();
      accepted.clear();
      try {
        try {
          closeCursor();
        } finally {
          if (session != null) session.close();
        }
      } catch (Exception e) {
        throw JdbcSession.databaseFailure();
      }
    }
  }
}
