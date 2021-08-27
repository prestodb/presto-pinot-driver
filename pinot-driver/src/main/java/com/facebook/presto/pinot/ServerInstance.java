package com.facebook.presto.pinot;
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

import org.apache.helix.model.InstanceConfig;

/**
 * Since `pinot-core` module is shaded, `org.apache.pinot.core.transport.ServerInstance` won't be exposed to external usage.
 * So we provide a wrapper class to expose same APIs as `org.apache.pinot.core.transport.ServerInstance`.
 *
 */
public class ServerInstance
{
    private final org.apache.pinot.core.transport.ServerInstance serverInstance;

    public ServerInstance(org.apache.pinot.core.transport.ServerInstance serverInstance)
    {
        this.serverInstance = serverInstance;
    }

    public ServerInstance(String serverInstance)
    {
        this.serverInstance = new org.apache.pinot.core.transport.ServerInstance(new InstanceConfig(serverInstance));
    }

    public String getHostname()
    {
        return serverInstance.getHostname();
    }

    public int getPort()
    {
        return serverInstance.getPort();
    }

    @Override
    public int hashCode()
    {
        return serverInstance.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ServerInstance) {
            ServerInstance that = (ServerInstance) obj;
            return getHostname().equals(that.getHostname()) && getPort() == that.getPort();
        }
        return false;
    }

    /**
     * Use default format {@code Server_<hostname>_<port>} for backward-compatibility.
     */
    @Override
    public String toString()
    {
        return serverInstance.toString();
    }
}
