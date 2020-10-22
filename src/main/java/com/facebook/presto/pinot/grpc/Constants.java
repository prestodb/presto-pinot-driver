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

/**
 * Common constants defined in `org.apache.pinot.common.utils.CommonConstants` from Pinot common 0.6.0
 */
public class Constants
{
    public static class Request
    {
        public static class MetadataKeys
        {
            public static final String REQUEST_ID = "requestId";
            public static final String BROKER_ID = "brokerId";
            public static final String ENABLE_TRACE = "enableTrace";
            public static final String ENABLE_STREAMING = "enableStreaming";
            public static final String PAYLOAD_TYPE = "payloadType";
        }

        public static class PayloadType
        {
            public static final String SQL = "sql";
            public static final String BROKER_REQUEST = "brokerRequest";
        }
    }

    public static class Response
    {
        public static class MetadataKeys
        {
            public static final String RESPONSE_TYPE = "responseType";
        }

        public static class ResponseType
        {
            // For streaming response, multiple (could be 0 if no data should be returned, or query encounters exception)
            // data responses will be returned, followed by one single metadata response
            public static final String DATA = "data";
            public static final String METADATA = "metadata";
            // For non-streaming response
            public static final String NON_STREAMING = "nonStreaming";
        }
    }
}
