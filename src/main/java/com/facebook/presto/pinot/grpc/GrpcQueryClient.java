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

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;

import java.util.Iterator;

public class GrpcQueryClient
{
    private final Channel channel;
    private final PinotQueryServerGrpc.PinotQueryServerBlockingStub blockingStub;

    public GrpcQueryClient(String host, int port, PinotStreamingQueryClient.Config config)
    {
        ManagedChannelBuilder managedChannelBuilder = ManagedChannelBuilder
                    .forAddress(host, port)
                    .maxInboundMessageSize(config.getMaxInboundMessageSizeBytes());
        if (config.isUsePlainText()) {
            managedChannelBuilder.usePlaintext();
        }
        channel = managedChannelBuilder.build();
        blockingStub = PinotQueryServerGrpc.newBlockingStub(channel);
    }

    public Iterator<ServerResponse> submit(GrpcRequestBuilder requestBuilder)
    {
        Iterator<Server.ServerResponse> iterator = blockingStub.submit(requestBuilder.build());
        return new Iterator<ServerResponse>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public ServerResponse next()
            {
                return ServerResponse.of(iterator.next());
            }
        };
    }
}
