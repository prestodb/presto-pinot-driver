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
package com.facebook.presto.pinot.grpc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Grpc based Pinot query client.
 */
public class PinotStreamingQueryClient
{
    private final Map<String, GrpcQueryClient> grpcQueryClientMap = new HashMap<>();
    private final Config config;

    public PinotStreamingQueryClient(Config config)
    {
        this.config = config;
    }

    public Iterator<ServerResponse> submit(String host, int port, GrpcRequestBuilder requestBuilder)
    {
        GrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
        return client.submit(requestBuilder);
    }

    private GrpcQueryClient getOrCreateGrpcQueryClient(String host, int port)
    {
        String key = String.format("%s_%d", host, port);
        if (!grpcQueryClientMap.containsKey(key)) {
            grpcQueryClientMap.put(key, new GrpcQueryClient(host, port, config));
        }
        return grpcQueryClientMap.get(key);
    }

    public static class Config
    {
        // Default max message size to 128MB
        private static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;
        private final int maxInboundMessageSizeBytes;
        private final boolean usePlainText;

        public Config()
        {
            this(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE, false);
        }

        public Config(int maxInboundMessageSizeBytes, boolean usePlainText)
        {
            this.maxInboundMessageSizeBytes = maxInboundMessageSizeBytes;
            this.usePlainText = usePlainText;
        }

        public int getMaxInboundMessageSizeBytes()
        {
            return maxInboundMessageSizeBytes;
        }

        public boolean isUsePlainText()
        {
            return usePlainText;
        }
    }
}
