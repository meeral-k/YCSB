/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
//import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.MutationApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
//import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import javax.annotation.Nullable;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.InputStreamByteIterator;
import site.ycsb.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Google Bigtable native client for YCSB framework.
 *
 * <p>Bigtable offers two APIs. These include a native API as well as an HBase API
 * wrapper for the GRPC API. This client implements the native API to test the underlying calls
 * wrapped up in the HBase API. To use the HBase API, see the hbase10 client binding.
 */
public class GoogleBigtableClient extends site.ycsb.DB {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigtableClient.class);
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");

  /** Property names for the CLI. */
  static final String EMULATOR_HOST_KEY = "google.bigtable.emulator_host";

  static final String PROJECT_KEY = "google.bigtable.project.id";
  static final String INSTANCE_KEY = "google.bigtable.instance.id";
  static final String JSON_KEY_FILE_KEY = "google.bigtable.auth.json.keyfile";
  static final String DEBUG_KEY = "debug";
  static final String COLUMN_FAMILY_KEY = "columnfamily";
  static final String DATA_ENDPOINT = "google.bigtable.data.endpoint";

  static final String ASYNC_MUTATOR_MAX_MEMORY = "mutatorMaxMemory";
  static final String ASYNC_MAX_INFLIGHT_RPCS = "mutatorMaxInflightRPCs";
  static final String CLIENT_SIDE_BUFFERING = "clientbuffering";

  /** Shared bigtable client instance. */
  private static BigtableDataClient client = null;

  private static int clientRefcount = 0;

  /** Print debug information to standard out. */
  private boolean debug = false;

  /** The column family use for the workload. */
  private String columnFamily;

  /**
   * If true, buffer mutations on the client. For measuring insert/update/delete latencies, client
   * side buffering should be disabled.
   */
  private boolean clientSideBuffering = false;

  private Map<String, Batcher<RowMutationEntry, Void>> batchers = new HashMap<>();

  private static synchronized BigtableDataClient getOrCreateClient(Properties properties)
      throws DBException {
    if (clientRefcount > 0) {
      clientRefcount++;
      return client;
    }

    // Extract the properties
    @Nullable String emulatorHost = properties.getProperty(EMULATOR_HOST_KEY, null);

    String projectId =
        Preconditions.checkNotNull(
            properties.getProperty(PROJECT_KEY), "%s property must be set", PROJECT_KEY);
    String instanceId =
        Preconditions.checkNotNull(
            properties.getProperty(INSTANCE_KEY), "%s property must be set", INSTANCE_KEY);
    @Nullable String jsonKeyFilePath = properties.getProperty(JSON_KEY_FILE_KEY, null);
    boolean clientBufferingEnabled =
        Boolean.parseBoolean(properties.getProperty(CLIENT_SIDE_BUFFERING, "true"));
    @Nullable Long maxMemory = null;
    if (properties.contains(ASYNC_MUTATOR_MAX_MEMORY)) {
      maxMemory = Long.parseLong(properties.getProperty(ASYNC_MUTATOR_MAX_MEMORY));
    }
    @Nullable Long maxRpcs = null;
    if (properties.contains(ASYNC_MAX_INFLIGHT_RPCS)) {
      maxRpcs = Long.parseLong(properties.getProperty(ASYNC_MAX_INFLIGHT_RPCS));
    }

    RetrySettings retrySettings = RetrySettings.newBuilder()
          .setInitialRpcTimeout(Duration.ofMillis(4 * 1000)) // each attempt's timeout is 4 seconds
          .setMaxRpcTimeout(Duration.ofMillis(4 * 1000)) // max timeout is the same as initial
          .setTotalTimeout(Duration.ofMillis(60 * 1000)) // the entire operation 
          .build();
    BigtableDataSettings.Builder builder;
    if (emulatorHost != null) {
      int index = emulatorHost.lastIndexOf(":");
      String host = "localhost";
      int port;
      if (index > 0) {
        host = emulatorHost.substring(0, index);
        port = Integer.parseInt(emulatorHost.substring(index + 1));
      } else {
        port = Integer.parseInt(emulatorHost);
      }
      builder = BigtableDataSettings.newBuilderForEmulator(host, port);
    } else {
      builder = BigtableDataSettings.newBuilder();
    }

    //try{
    //  BigtableDataSettings.enableBuiltinMetrics();
    //} catch (IOException e) {
    //  throw new DBException("Failed to enable build in metrics", e);
    //}
    builder
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        // Enable GFE metric views
        .setRefreshingChannel(true);

    if (jsonKeyFilePath != null) {
      try (FileInputStream fin = new FileInputStream(jsonKeyFilePath)) {
        builder
            .stubSettings()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(GoogleCredentials.fromStream(fin)));
      } catch (IOException e) {
        //LOG.log(null, jsonKeyFilePath);(e.getMessage());
        throw new DBException(
            String.format("Failed to load credentials specified at path %s", jsonKeyFilePath), e);
      }
    }

    if (clientBufferingEnabled) {
      BatchingSettings defaultBatchingSettings =
          builder.stubSettings().bulkMutateRowsSettings().getBatchingSettings();
      FlowControlSettings.Builder flowControlSettings =
          defaultBatchingSettings.getFlowControlSettings().toBuilder();

      if (maxMemory != null) {
        flowControlSettings.setMaxOutstandingRequestBytes(maxMemory);
      }
      if (maxRpcs != null) {
        flowControlSettings.setMaxOutstandingElementCount(
            defaultBatchingSettings.getElementCountThreshold() * maxRpcs);
      }
      builder
          .stubSettings()
          .bulkMutateRowsSettings()
          .setRetrySettings(retrySettings) 
          .setBatchingSettings(
              defaultBatchingSettings.toBuilder()
                  .setFlowControlSettings(flowControlSettings.build())
                  .build());
    }
    String dataEndpoint = properties.getProperty(DATA_ENDPOINT);
    if (dataEndpoint != null) {
      builder
          .stubSettings()
          .setEndpoint(dataEndpoint);
    }

    builder.stubSettings().readRowsSettings().setRetrySettings(retrySettings);
    builder.stubSettings().readRowSettings().setRetrySettings(retrySettings);
    builder.stubSettings().bulkReadRowsSettings().setRetrySettings(retrySettings);
    builder.stubSettings().readModifyWriteRowSettings().setRetrySettings(retrySettings);
    builder.stubSettings().sampleRowKeysSettings().setRetrySettings(retrySettings);

   // builder.stubSettings().setTransportChannelProvider(EnhancedBigtableStubSettings
   //   .defaultGrpcTransportProviderBuilder()
   //   .setChannelPoolSettings(ChannelPoolSettings.builder()
   //   .setMaxChannelCount(32)
   //   .setMinChannelCount(32)
   //   .build()).build());
    try {
      client = BigtableDataClient.create(builder.build());
    } catch (IOException e) {
      throw new DBException("Failed to construct the Bigtable client", e);
    }
    clientRefcount++;
    return client;
  }

  private static synchronized void releaseClient() {
    if (--clientRefcount >= 0) {
      return;
    }
    client.close();
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    clientSideBuffering =
        Boolean.parseBoolean(getProperties().getProperty(CLIENT_SIDE_BUFFERING, "false"));
    String enabled_directpath = System.getenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS");
    System.out.println("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS value: " + enabled_directpath);
    LOG.info(
        "Running Google Bigtable with Proto API"
            + (clientSideBuffering ? " and client side buffering." : "."));

    getOrCreateClient(props);

    debug = Boolean.parseBoolean(props.getProperty(DEBUG_KEY, "false"));

    columnFamily = getProperties().getProperty(COLUMN_FAMILY_KEY);
    if (columnFamily == null) {
      throw new DBException(String.format("%s must be specified", COLUMN_FAMILY_KEY));
    }
  }

  @Override
  public void cleanup() throws DBException {
    List<Exception> exceptions = new ArrayList<>();

    for (Entry<String, Batcher<RowMutationEntry, Void>> enry : batchers.entrySet()) {
      try {
        enry.getValue().close();
      } catch (RuntimeException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        exceptions.add(e);
      }
    }

    try {
      releaseClient();
    } catch (RuntimeException e) {
      exceptions.add(e);
    }

    if (!exceptions.isEmpty()) {
      DBException parentError = new DBException("Failed to cleanup Bigtable client resources");
      exceptions.forEach(parentError::addSuppressed);
      throw parentError;
    }
  }

  private Batcher<RowMutationEntry, Void> getOrCreateBatcher(String tableId) {
    return batchers.computeIfAbsent(tableId, client::newBulkMutationBatcher);
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (debug) {
      LOG.info("Doing read from Bigtable columnfamily " + columnFamily);
      LOG.info("Doing read for key: " + key);
    }

    Filter filter = FILTERS.family().exactMatch(columnFamily);
    if (fields != null && !fields.isEmpty()) {
      filter =
          FILTERS
              .chain()
              .filter(filter)
              .filter(FILTERS.limit().cellsPerColumn(1))
              .filter(FILTERS.qualifier().regex(Joiner.on("|").join(fields)));
    }

    try {
      for (RowCell cell : client.readRow(table, key, filter).getCells()) {
        result.put(cell.getQualifier().toString(UTF8_CHARSET), wrapByteString(cell.getValue()));

        if (debug) {
          LOG.info(
              "Result for field: "
                  + cell.getQualifier().toString(UTF8_CHARSET)
                  + " is: "
                  + cell.getValue().toString(UTF8_CHARSET));
        }
      }

      return Status.OK;
    } catch (RuntimeException e) {
      //LOG.error("Failed to read key: " + key + " " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    Filter filter = FILTERS.family().exactMatch(columnFamily);
    if (fields != null && !fields.isEmpty()) {
      filter =
          FILTERS
              .chain()
              .filter(filter)
              .filter(FILTERS.limit().cellsPerColumn(1))
              .filter(FILTERS.qualifier().regex(Joiner.on("|").join(fields)));
    }

    Query query =
        Query.create(table)
            .filter(filter)
            .range(ByteStringRange.unbounded().startClosed(startkey))
            .limit(recordcount);

    List<Row> rows;
    try {
      rows = client.readRowsCallable().all().call(query);
    } catch (RuntimeException e) {
      LOG.info("Exception during scan: " + e);
      return Status.ERROR;
    }

    if (rows.isEmpty()) {
      return Status.NOT_FOUND;
    }

    for (Row row : rows) {
      HashMap<String, ByteIterator> rowResult = new HashMap<>();
      for (RowCell cell : row.getCells()) {
        rowResult.put(cell.getQualifier().toString(UTF8_CHARSET), wrapByteString(cell.getValue()));
        if (debug) {
          LOG.info(
              "Result for field: "
                  + cell.getQualifier().toString(UTF8_CHARSET)
                  + " is: "
                  + cell.getValue().toString(UTF8_CHARSET));
        }
      }
      result.add(rowResult);
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (debug) {
      LOG.info("Setting up put for key: " + key);
    }

    if (clientSideBuffering) {
      RowMutationEntry entry = RowMutationEntry.create(key);
      populateMutations(values, entry);
      getOrCreateBatcher(table).add(entry);
      return Status.BATCHED_OK;
    } else {
      RowMutation rowMutation = RowMutation.create(table, key);
      populateMutations(values, rowMutation);
      try {
        client.mutateRow(rowMutation);
        return Status.OK;
      } catch (RuntimeException e) {
        LOG.info("Failed to insert key: " + key + " " + e.getMessage());
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      LOG.info("Doing delete for key: " + key);
    }

    if (clientSideBuffering) {
      getOrCreateBatcher(table).add(RowMutationEntry.create(key).deleteRow());
      return Status.BATCHED_OK;
    } else {
      try {
        client.mutateRow(RowMutation.create(table, key).deleteRow());
        return Status.OK;
      } catch (RuntimeException e) {
        LOG.info("Failed to delete key: " + key + " " + e.getMessage());
        return Status.ERROR;
      }
    }
  }

  private void populateMutations(Map<String, ByteIterator> input, MutationApi<?> output) {
    for (Entry<String, ByteIterator> entry : input.entrySet()) {
      output.setCell(columnFamily, wrapString(entry.getKey()), wrapByteIterator(entry.getValue()));
    }
  }

  private static ByteIterator wrapByteString(ByteString byteString) {
    return new InputStreamByteIterator(byteString.newInput(), byteString.size());
  }

  private static ByteString wrapString(String s) {
    return ByteString.copyFromUtf8(s);
  }

  private static ByteString wrapByteIterator(ByteIterator iterator) {
    return UnsafeByteOperations.unsafeWrap(iterator.toArray());
  }
}