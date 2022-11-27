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

package org.teiid.translator.cassandra;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.GuavaCompatibility;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class CassandraQueryExecution implements ResultSetExecution {

    private Command query;
    private CassandraConnection connection;
    private ResultSetFuture resultSetFuture;
    private ResultSet resultSet;
    private ExecutionContext executionContext;
    protected boolean returnsArray;

    public CassandraQueryExecution(Command query, CassandraConnection connection, ExecutionContext context){
        this.query = query;
        this.connection = connection;
        this.executionContext = context;
    }

    @Override
    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraExecutionFactory.UTIL.getString("close_query")); //$NON-NLS-1$
        this.resultSet = null;
        this.resultSetFuture = null;
    }

    @Override
    public void cancel() throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$
        if (resultSetFuture != null) {
            resultSetFuture.cancel(true);
        }
    }

    @Override
    public void execute() throws TranslatorException {
        CassandraSQLVisitor visitor = new CassandraSQLVisitor();
        visitor.translateSQL(query);
        String cql = visitor.getTranslatedSQL();
        execute(cql);
    }

    protected void execute(String cql) {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-Query:", cql); //$NON-NLS-1$
        this.executionContext.logCommand(cql);
        resultSetFuture = connection.executeQuery(cql);
        resultSetFuture.addListener(new Runnable() {

            @Override
            public void run() {
                executionContext.dataAvailable();
            }
        }, GuavaCompatibility.INSTANCE.sameThreadExecutor());
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (!resultSetFuture.isDone()) {
            throw DataNotAvailableException.NO_POLLING;
        }
        if (resultSet == null) {
            this.resultSet = this.resultSetFuture.getUninterruptibly();
        }
        //TODO: use asynch for results fetching
        return getRow(resultSet.one());
    }

    /**
     * Iterates through all columns in the {@code row}. For each column, returns its value as Java type
     * that matches the CQL type in switch part. Otherwise returns the value as bytes composing the value.
     * @param row the row returned by the ResultSet
     * @return list of values in {@code row}
     */
    List<Object> getRow(Row row) {
        if(row == null){
            return null;
        }
        ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final List<Object> values = new ArrayList<Object>(columnDefinitions.size());
        for(int i = 0; i < columnDefinitions.size(); i++){
            if (row.isNull(i)) {
                values.add(null);
                continue;
            }
            switch(columnDefinitions.getType(i).getName()){
            case ASCII:
                values.add(row.getString(i));
                break;
            case BIGINT:
                values.add(Long.valueOf(row.getLong(i)));
                break;
            case BOOLEAN:
                values.add(Boolean.valueOf(row.getBool(i)));
                break;
            case COUNTER:
                values.add(Long.valueOf(row.getLong(i)));
                break;
            case DECIMAL:
                values.add(row.getDecimal(i));
                break;
            case DOUBLE:
                values.add(Double.valueOf(row.getDouble(i)));
                break;
            case FLOAT:
                values.add(Float.valueOf(row.getFloat(i)));
                break;
            case INET:
                values.add(row.getInet(i));
                break;
            case INT:
                values.add(Integer.valueOf(row.getInt(i)));
                break;
            case LIST:
                values.add(row.getList(i, Object.class));
                break;
            case MAP:
                values.add(row.getMap(i, Object.class, Object.class));
                break;
            case SET:
                values.add(row.getSet(i, Object.class));
                break;
            case TEXT:
                values.add(row.getString(i));
                break;
            case TIME:
                long val = row.getTime(i);
                Timestamp ts = new Timestamp(val/1000000);
                ts.setNanos((int)(val%1000000));
                values.add(ts);
                break;
            case DATE:
                values.add(Date.valueOf(row.getDate(i).toString()));
                break;
            case TIMESTAMP:
                values.add(row.getTimestamp(i));
                break;
            case TIMEUUID:
                values.add(row.getUUID(i));
                break;
            case UUID:
                values.add(row.getUUID(i));
                break;
            case VARCHAR:
                values.add(row.getString(i));
                break;
            case VARINT:
                values.add(row.getVarint(i));
                break;
            default:
                //read as a varbinary
                ByteBuffer bytesUnsafe = row.getBytesUnsafe(i);
                byte[] b = new byte[bytesUnsafe.remaining()];
                bytesUnsafe.get(b);
                values.add(b);
                break;
            }

        }
        if (returnsArray) {
            return Arrays.asList((Object)values.toArray());
        }
        return values;
    }

}
