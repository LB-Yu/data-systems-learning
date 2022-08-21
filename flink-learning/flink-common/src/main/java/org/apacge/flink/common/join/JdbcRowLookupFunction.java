package org.apacge.flink.common.join;

import org.apacge.flink.common.converter.JdbcRowConverter;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JdbcRowLookupFunction extends RowLookupFunction {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcRowLookupFunction.class);
  private static final long serialVersionUID = 2L;

  private final String lookupSQL;
  private final int[] fieldPositions;
  private final JoinFunction<Row, Row, Row> joinFunction;

  private final JdbcConnectionOptions connectionOptions;
  private JdbcConnectionProvider connectionProvider;
  private PreparedStatement statement;
  private final JdbcRowConverter jdbcRowConverter;

  // cache
  private final long cacheMaxSize;
  private final long cacheExpireMs;
  private final long maxRetryTimes;
  private final boolean cacheMissingKey;

  private transient Cache<Row, List<Row>> cache;

  public JdbcRowLookupFunction(
          final JdbcConnectionOptions connectionOptions,
          final JdbcLookupOptions lookupOptions,
          final String lookupSQL,
          final int[] fieldPositions,
          final JoinFunction<Row, Row, Row> joinFunction,
          final boolean isOuterJoin) {
    this.connectionOptions = connectionOptions;
    this.lookupSQL = lookupSQL;
    this.fieldPositions = fieldPositions;
    this.joinFunction = joinFunction;
    this.jdbcRowConverter = new JdbcRowConverter();
    this.isOuterJoin = isOuterJoin;

    this.cacheMaxSize = lookupOptions.getCacheMaxSize();
    this.cacheExpireMs = lookupOptions.getCacheExpireMs();
    this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
    this.cacheMissingKey = lookupOptions.getCacheMissingKey();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    connectionProvider = new SimpleJdbcConnectionProvider(connectionOptions);
    try {
      establishConnectionAndStatement();
      this.cache =
              cacheMaxSize == -1 || cacheExpireMs == -1
                      ? null
                      : CacheBuilder.newBuilder()
                      .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                      .maximumSize(cacheMaxSize)
                      .build();
    } catch (SQLException sqe) {
      throw new IllegalArgumentException("open() failed.", sqe);
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
    }
  }

  @Override
  public void processElement(Row queryRow,
                             Context context,
                             Collector<Row> collector) throws Exception {
    if (cache != null) {
      List<Row> cachedRows = cache.getIfPresent(queryRow);
      if (cachedRows != null) {
        for (Row cachedRow : cachedRows) {
          collector.collect(joinFunction.join(queryRow, cachedRow));
        }
        return;
      }
    }

    for (int retry = 0; retry <= maxRetryTimes; retry++) {
      try {
        statement.clearParameters();
        for (int i = 0; i < fieldPositions.length; ++i) {
          statement.setObject(i + 1, queryRow.getField(fieldPositions[i]));
        }
        try (ResultSet resultSet = statement.executeQuery()) {
          if (cache == null) {
            boolean hasJoinRow = false;
            while (resultSet.next()) {
              Row resultRow = jdbcRowConverter.toInternal(resultSet);
              collector.collect(joinFunction.join(queryRow, resultRow));
              hasJoinRow = true;
            }
            if (!hasJoinRow && isOuterJoin) {
              collector.collect(joinFunction.join(queryRow, null));
            }
          } else {
            ArrayList<Row> rows = new ArrayList<>();
            boolean hasJoinRow = false;
            while (resultSet.next()) {
              Row resultRow = jdbcRowConverter.toInternal(resultSet);
              rows.add(resultRow);
              collector.collect(joinFunction.join(queryRow, resultRow));
              hasJoinRow = true;
            }
            if (!hasJoinRow && isOuterJoin) {
              collector.collect(joinFunction.join(queryRow, null));
            }
            rows.trimToSize();
            if (!rows.isEmpty() || cacheMissingKey) {
              cache.put(queryRow, rows);
            }
          }
        }
        break;
      } catch (SQLException e) {
        LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
        if (retry >= maxRetryTimes) {
          throw new RuntimeException("Execution of JDBC statement failed.", e);
        }

        try {
          if (!connectionProvider.isConnectionValid()) {
            statement.close();
            connectionProvider.closeConnection();
            establishConnectionAndStatement();
          }
        } catch (SQLException | ClassNotFoundException exception) {
          LOG.error(
                  "JDBC connection is not valid, and reestablish connection failed",
                  exception);
          throw new RuntimeException("Reestablish JDBC connection failed", exception);
        }

        try {
          Thread.sleep(1000 * retry);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (cache != null) {
      cache.cleanUp();
      cache = null;
    }
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOG.info("JDBC statement could not be closed: " + e.getMessage());
      } finally {
        statement = null;
      }
    }

    connectionProvider.closeConnection();
  }

  private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
    Connection dbConn = connectionProvider.getOrEstablishConnection();
    statement = dbConn.prepareStatement(lookupSQL);
  }
}
