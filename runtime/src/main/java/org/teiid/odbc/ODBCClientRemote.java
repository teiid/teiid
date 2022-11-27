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

import java.util.List;
import java.util.Properties;

import org.teiid.client.util.ResultsFuture;
import org.teiid.jdbc.ResultSetImpl;
import org.teiid.odbc.PGUtil.PgColInfo;

public interface ODBCClientRemote {

    enum CursorDirection {
        FORWARD,
        FIRST,
        ABSOLUTE,
        RELATIVE,
        LAST
    }

    void initialized(Properties props);

    void setEncoding(String value, boolean init);

    //    AuthenticationCleartextPassword (B)
    void useClearTextAuthentication();

    // AuthenticationGSS (B)
    void useAuthenticationGSS();

    // AuthenticationGSSContinue (B)
    void authenticationGSSContinue(byte[] serviceToken);

    //    AuthenticationOk (B)
    //    BackendKeyData (B)
    //    ParameterStatus (B)
    void authenticationSucess(int processId, int screctKey);

    //    ParseComplete (B)
    void prepareCompleted(String preparedName);

    //    ErrorResponse (B)
    void errorOccurred(String msg);

    //    ErrorResponse (B)
    void errorOccurred(Throwable e);

    void terminated();

    //    ParameterDescription (B)
    void sendParameterDescription(int[] paramType);

    //    BindComplete (B)
    void bindComplete();

    //    RowDescription (B)
    //    NoData (B)
    void sendResultSetDescription(List<PgColInfo> cols, short[] resultColumnFormat);

    //    DataRow (B)
    //    CommandComplete (B)
    void sendResults(String sql, ResultSetImpl rs, List<PgColInfo> cols, ResultsFuture<Integer> result, CursorDirection direction, int rowCount, boolean describeRows, short[] resultColumnFormat);

    void sendCommandComplete(String sql, Integer... count);

    //    ReadyForQuery (B)
    void ready(boolean inTransaction, boolean failedTransaction);

    void statementClosed();

    //    EmptyQueryResponse (B)
    void emptyQueryReceived();

    void flush();

    // FunctionCallResponse (B)
    void functionCallResponse(Object data, boolean binary);

    void sendSslResponse();

    // unimplemented backend messages

    //    AuthenticationKerberosV5 (B)
    //    AuthenticationMD5Password (B)
    //    AuthenticationSCMCredential (B)
    //    AuthenticationSSPI (B)

    //    CloseComplete (B)

    //    CopyData (F & B)
    //    CopyDone (F & B)
    //    CopyInResponse (B)
    //    CopyOutResponse (B)

    //    NoticeResponse (B)
    //    NotificationResponse (B)

    void sendPortalSuspended();

    void sendParameterStatus(String param, String value);
}
