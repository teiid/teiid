/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.net;

import org.teiid.client.security.LogonResult;

public interface ServerConnection {

    public static final int PING_INTERVAL = 120000;

    <T> T getService(Class<T> iface);

    void close();

    boolean isOpen(long msToTest);

    LogonResult getLogonResult();

    boolean isSameInstance(ServerConnection conn) throws CommunicationException;

    void authenticate() throws ConnectionException, CommunicationException;

    boolean supportsContinuous();

    boolean isLocal();

    String getServerVersion();

}