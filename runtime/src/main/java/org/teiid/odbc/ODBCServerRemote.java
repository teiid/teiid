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
package org.teiid.odbc;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Properties;

import javax.net.ssl.SSLEngine;

import org.teiid.transport.PgFrontendProtocol.NullTerminatedStringDataInputStream;

public interface ODBCServerRemote {

    void initialize(Properties props, SocketAddress remoteAddress, SSLEngine sslEngine);

    void logon(String databaseName, String userid, NullTerminatedStringDataInputStream data, SocketAddress remoteAddress);

    void prepare(String prepareName, String sql, int[] paramType);

    void bindParameters(String bindName, String prepareName, Object[] paramdata, int resultCodeCount, short[] resultColumnFormat, Charset charset);

    void execute(String bindName, int maxrows);

    void getParameterDescription(String prepareName);

    void getResultSetMetaDataDescription(String bindName);

    void sync();

    void executeQuery(String sql);

    void terminate();

    void closePreparedStatement(String preparedName);

    void closeBoundStatement(String bindName);

    void unsupportedOperation(String msg);

    void flush();

    void functionCall(int oid, Object[] params, short resultFormat);

    void sslRequest();

    void cancel(int pid, int key);

    //  unimplemented frontend messages
    //    CopyData (F & B)
    //    CopyDone (F & B)
    //    CopyFail (F)
}


