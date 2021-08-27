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

import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A wrapper class for org.apache.pinot.common.proto.Server.ServerResponse to simplify the usage of a proto generated class.
 *
 */
public class ServerResponse
{
    private final Server.ServerResponse serverResponse;

    public ServerResponse(Server.ServerResponse serverResponse)
    {
        this.serverResponse = serverResponse;
    }

    public DataTable getDataTable(ByteBuffer byteBuffer) throws IOException
    {
        return DataTableFactory.getDataTable(byteBuffer);
    }

    public String getResponseType()
    {
        return serverResponse.getMetadataMap().get(Constants.Response.MetadataKeys.RESPONSE_TYPE);
    }

    public byte[] getPayload()
    {
        return serverResponse.getPayload().toByteArray();
    }

    public ByteBuffer getPayloadReadOnlyByteBuffer()
    {
        return serverResponse.getPayload().asReadOnlyByteBuffer();
    }

    public Map<String, String> getMetadataMap()
    {
        return serverResponse.getMetadataMap();
    }

    public int getSerializedSize()
    {
        return serverResponse.getSerializedSize();
    }

    public static ServerResponse of(Server.ServerResponse response)
    {
        return new ServerResponse(response);
    }
}
