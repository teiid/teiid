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

package org.postgresql.core.v3;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Deque;
import java.util.Properties;

import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.ResultHandler;

/**
 * Allows for simple query executions against an implied cursor portal
 *
 * This is hack to test cursoring through jdbc
 *
 */
@SuppressWarnings({"rawtypes", "nls"})
public class ExtendedQueryExecutorImpl extends QueryExecutorImpl {

    public static String simplePortal;

    public PGStream stream;
    public Deque<ExecuteRequest> pendingExecute;
    public Deque<SimpleQuery> pendingDescribe;

    public ExtendedQueryExecutorImpl(PGStream pgStream, String user, String database, int cancelSignalTimeout, Properties info) throws SQLException, IOException {
        super(pgStream, user, database, cancelSignalTimeout, info);
        this.stream = pgStream;
        try {
            Field f = QueryExecutorImpl.class.getDeclaredField("pendingExecuteQueue");
            f.setAccessible(true);
            pendingExecute = (Deque<ExecuteRequest>) f.get(this);
            f = QueryExecutorImpl.class.getDeclaredField("pendingDescribePortalQueue");
            f.setAccessible(true);
            pendingDescribe = (Deque<SimpleQuery>) f.get(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void execute(Query query, ParameterList parameters,
            ResultHandler handler, int maxRows, int fetchSize, int flags)
            throws SQLException {
        if (simplePortal != null) {
            try {
                byte[] bytes = query.toString().getBytes("UTF-8");

                stream.sendChar('Q');
                stream.sendInteger4(bytes.length + 6);
                stream.send(bytes);
                stream.sendInteger2(0);
                stream.flush();
                if (pendingExecute.isEmpty()) {
                    pendingExecute.add(new ExecuteRequest((SimpleQuery)query, new Portal((SimpleQuery)query, simplePortal), true));
                }
                if (pendingDescribe.isEmpty()) {
                    pendingDescribe.add((SimpleQuery)query);
                }
                processResults(handler, flags);
                handler.handleCompletion();
                return;
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
        super.execute(query, parameters, handler, maxRows, fetchSize, flags);
    }

}
