/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.pinot;

import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.pql.parsers.Pql2CompilationException;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.lang.String.format;

public class PinotScatterGatherQueryClient
{
    private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
    private static final String PRESTO_HOST_PREFIX = "presto-pinot-";

    private final String prestoHostId;
    private final BrokerMetrics brokerMetrics;
    private final Queue<QueryRouter> queryRouters = new ConcurrentLinkedQueue<>();
    private final Config config;
    private final Map<String, AtomicInteger> concurrentQueriesCountMap = new ConcurrentHashMap<>();

    public enum ErrorCode
    {
        PINOT_INSUFFICIENT_SERVER_RESPONSE(true),
        PINOT_INVALID_PQL_GENERATED(false),
        PINOT_UNCLASSIFIED_ERROR(false),
        PINOT_QUERY_BACKLOG_FULL(false);

        private final boolean retriable;

        ErrorCode(boolean retriable)
        {
            this.retriable = retriable;
        }

        public boolean isRetriable()
        {
            return retriable;
        }
    }

    public static class PinotException
            extends RuntimeException
    {
        private final ErrorCode errorCode;

        public PinotException(ErrorCode errorCode, String message, Throwable t)
        {
            super(message, t);
            this.errorCode = errorCode;
        }

        public PinotException(ErrorCode errorCode, String message)
        {
            this(errorCode, message, null);
        }

        public ErrorCode getErrorCode()
        {
            return errorCode;
        }
    }

    public static class Config
    {
        private final int threadPoolSize;

        private final int maxBacklogPerServer;

        @Deprecated
        private final long idleTimeoutMillis;
        @Deprecated
        private final int minConnectionsPerServer;
        @Deprecated
        private final int maxConnectionsPerServer;

        public Config(long idleTimeoutMillis, int threadPoolSize, int minConnectionsPerServer, int maxBacklogPerServer,
                      int maxConnectionsPerServer)
        {
            this.idleTimeoutMillis = idleTimeoutMillis;
            this.threadPoolSize = threadPoolSize;
            this.minConnectionsPerServer = minConnectionsPerServer;
            this.maxBacklogPerServer = maxBacklogPerServer;
            this.maxConnectionsPerServer = maxConnectionsPerServer;
        }

        public int getThreadPoolSize()
        {
            return threadPoolSize;
        }

        public int getMaxBacklogPerServer()
        {
            return maxBacklogPerServer;
        }

        @Deprecated
        public long getIdleTimeoutMillis()
        {
            return idleTimeoutMillis;
        }

        @Deprecated
        public int getMinConnectionsPerServer()
        {
            return minConnectionsPerServer;
        }

        @Deprecated
        public int getMaxConnectionsPerServer()
        {
            return maxConnectionsPerServer;
        }
    }

    public PinotScatterGatherQueryClient(Config pinotConfig)
    {
        prestoHostId = getDefaultPrestoId();
        brokerMetrics = new BrokerMetrics(new YammerMetricsRegistry());
        brokerMetrics.initializeGlobalMeters();

        // Setup QueryRouters
        for (int i = 0; i < pinotConfig.getThreadPoolSize(); i++) {
            queryRouters.add(new QueryRouter(String.format("%s-%d", prestoHostId, i), brokerMetrics));
        }

        config = pinotConfig;
    }

    private static <T> T doWithRetries(int retries, Function<Integer, T> caller)
    {
        PinotException firstError = null;
        for (int i = 0; i < retries; ++i) {
            try {
                return caller.apply(i);
            }
            catch (PinotException e) {
                if (firstError == null) {
                    firstError = e;
                }
                if (!e.getErrorCode().isRetriable()) {
                    throw e;
                }
            }
        }
        throw firstError;
    }

    private String getDefaultPrestoId()
    {
        String defaultBrokerId;
        try {
            defaultBrokerId = PRESTO_HOST_PREFIX + InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            defaultBrokerId = PRESTO_HOST_PREFIX;
        }
        return defaultBrokerId;
    }

    public Map<ServerInstance, DataTable> queryPinotServerForDataTable(
            String pql,
            String serverHost,
            List<String> segments,
            long connectionTimeoutInMillis,
            boolean ignoreEmptyResponses,
            int pinotRetryCount)
    {
        BrokerRequest brokerRequest;
        try {
            brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
        }
        catch (Pql2CompilationException e) {
            throw new PinotException(ErrorCode.PINOT_INVALID_PQL_GENERATED,
                    format("Parsing error with on %s, Error = %s", serverHost, e.getMessage()), e);
        }

        Map<org.apache.pinot.core.transport.ServerInstance, List<String>> routingTable = new HashMap<>();
        routingTable.put(new org.apache.pinot.core.transport.ServerInstance(new InstanceConfig(serverHost)), new ArrayList<>(segments));

        // Unfortunately the retries will all hit the same server because the routing decision has already been made by the pinot broker
        Map<ServerInstance, DataTable> serverResponseMap = doWithRetries(pinotRetryCount, (requestId) -> {
            String rawTableName = TableNameBuilder.extractRawTableName(brokerRequest.getQuerySource().getTableName());
            if (!concurrentQueriesCountMap.containsKey(serverHost)) {
                concurrentQueriesCountMap.put(serverHost, new AtomicInteger(0));
            }
            int concurrentQueryNum = concurrentQueriesCountMap.get(serverHost).get();
            if (concurrentQueryNum > config.getMaxBacklogPerServer()) {
                throw new PinotException(ErrorCode.PINOT_QUERY_BACKLOG_FULL, "Reaching server query max backlog size is - " + config.getMaxBacklogPerServer());
            }
            concurrentQueriesCountMap.get(serverHost).incrementAndGet();
            AsyncQueryResponse asyncQueryResponse;
            QueryRouter nextAvailableQueryRouter = getNextAvailableQueryRouter();
            if (TableNameBuilder.getTableTypeFromTableName(brokerRequest.getQuerySource().getTableName())
                    == TableType.REALTIME) {
                asyncQueryResponse = nextAvailableQueryRouter.submitQuery(requestId, rawTableName, null, null, brokerRequest, routingTable, connectionTimeoutInMillis);
            }
            else {
                asyncQueryResponse = nextAvailableQueryRouter
                        .submitQuery(requestId, rawTableName, brokerRequest, routingTable, null, null, connectionTimeoutInMillis);
            }
            Map<ServerInstance, DataTable> serverInstanceDataTableMap = gatherServerResponses(
                    ignoreEmptyResponses,
                    routingTable,
                    asyncQueryResponse,
                    brokerRequest.getQuerySource().getTableName());
            queryRouters.offer(nextAvailableQueryRouter);
            concurrentQueriesCountMap.get(serverHost).decrementAndGet();
            return serverInstanceDataTableMap;
        });
        return serverResponseMap;
    }

    private QueryRouter getNextAvailableQueryRouter()
    {
        QueryRouter queryRouter = queryRouters.poll();
        while (queryRouter == null) {
            try {
                Thread.sleep(200L);
            }
            catch (InterruptedException e) {
                // Swallow the exception
            }
            queryRouter = queryRouters.poll();
        }
        return queryRouter;
    }

    private Map<ServerInstance, DataTable> gatherServerResponses(boolean ignoreEmptyResponses,
                                                                 Map<org.apache.pinot.core.transport.ServerInstance, List<String>> routingTable, AsyncQueryResponse asyncQueryResponse, String tableNameWithType)
    {
        try {
            Map<ServerRoutingInstance, ServerResponse> queryResponses = asyncQueryResponse.getResponse();
            if (!ignoreEmptyResponses) {
                if (queryResponses.size() != routingTable.size()) {
                    Map<String, String> routingTableForLogging = new HashMap<>();
                    routingTable.entrySet().forEach(entry -> {
                        String valueToPrint = entry.getValue().size() > 10 ? format("%d segments", entry.getValue().size())
                                : entry.getValue().toString();
                        routingTableForLogging.put(entry.getKey().toString(), valueToPrint);
                    });
                    throw new PinotException(ErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE, String
                            .format("%d of %d servers responded with routing table servers: %s, query stats: %s",
                                    queryResponses.size(), routingTable.size(), routingTableForLogging, asyncQueryResponse.getStats()));
                }
            }
            Map<ServerInstance, DataTable> serverResponseMap = new HashMap<>();
            queryResponses.entrySet().forEach(entry -> serverResponseMap.put(
                    new ServerInstance(new org.apache.pinot.core.transport.ServerInstance(new InstanceConfig(String.format("Server_%s_%d", entry.getKey().getHostname(), entry.getKey().getPort())))),
                    entry.getValue().getDataTable()));
            return serverResponseMap;
        }
        catch (InterruptedException e) {
            throw new PinotException(ErrorCode.PINOT_UNCLASSIFIED_ERROR,
                    String.format("Caught exception while fetching responses for table: %s", tableNameWithType), e);
        }
    }
}
