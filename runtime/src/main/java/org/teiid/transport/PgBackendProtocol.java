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

package org.teiid.transport;

import static org.teiid.odbc.PGUtil.*;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StreamCorruptedException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLEngine;

import org.teiid.client.util.ResultsFuture;
import org.teiid.core.types.AbstractGeospatialType;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.SqlUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.jdbc.ResultSetImpl;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.socket.ServiceInvocationStruct;
import org.teiid.odbc.ODBCClientRemote;
import org.teiid.odbc.ODBCClientRemote.CursorDirection;
import org.teiid.odbc.PGUtil.PgColInfo;
import org.teiid.query.function.GeometryUtils;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.pg.PGbytea;
import org.teiid.transport.pg.TimestampUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
/**
 * Represents the messages going from Server --&gt; PG ODBC Client
 * Some parts of this code is taken from H2's implementation of ODBC
 */
@SuppressWarnings("nls")
public class PgBackendProtocol extends ChannelOutboundHandlerAdapter implements ODBCClientRemote {

    public static final String APPLICATION_NAME = "application_name"; //$NON-NLS-1$
    public static final String DEFAULT_APPLICATION_NAME = "ODBC"; //$NON-NLS-1$

    public static final String SSL_HANDLER_KEY = "sslHandler";

    private final class SSLEnabler implements ChannelFutureListener {

        private SSLEngine engine;

        public SSLEnabler(SSLEngine engine) {
            this.engine = engine;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                SslHandler handler = new SslHandler(engine);
                future.channel().pipeline().addFirst(SSL_HANDLER_KEY, handler);
            }
        }
    }

    private final class ResultsWorkItem implements Runnable {
        private final List<PgColInfo> cols;
        private final ResultSetImpl rs;
        private final ResultsFuture<Integer> result;
        private final short[] resultColumnFormat;
        private int rows2Send;
        private int rowsSent = 0;
        private int rowsInBuffer = 0;
        String sql;

        private ResultsWorkItem(List<PgColInfo> cols, ResultSetImpl rs, ResultsFuture<Integer> result, int rows2Send, short[] resultColumnFormat) {
            this.cols = cols;
            this.rs = rs;
            this.result = result;
            this.rows2Send = rows2Send;
            this.resultColumnFormat = resultColumnFormat;
            initBuffer(maxBufferSize / 8);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    nextFuture = rs.submitNext();
                    synchronized (nextFuture) {
                        if (!nextFuture.isDone()) {
                            nextFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
                                @Override
                                public void onCompletion(ResultsFuture<Boolean> future) {
                                    if (processRow(future)) {
                                        if (rowsSent != rows2Send) {
                                            ResultsWorkItem.this.run();
                                        }
                                    }
                                }
                            });
                            return;
                        }
                    }
                    if (!processRow(nextFuture)) {
                        break;
                    }
                } catch (Throwable t) {
                    result.getResultsReceiver().exceptionOccurred(t);
                }
            }
        }

        private boolean processRow(ResultsFuture<Boolean> future) {
            //synchronize the local state as this can be
            //used by both nio and the engine threads
            synchronized (PgBackendProtocol.this) {
                return processRowInternal(future);
            }
        }

        private boolean processRowInternal(ResultsFuture<Boolean> future) {
            nextFuture = null;
            boolean processNext = true;
            try {
                if (future.get()) {
                    sendDataRow(rs, cols, resultColumnFormat);
                    rowsSent++;
                    rowsInBuffer++;
                    boolean done = rowsSent == rows2Send;
                    flushResults(done);
                    processNext = !done;
                    if (done) {
                        if (sql != null) {
                            sendCommandComplete(sql, rowsSent);
                        }
                        result.getResultsReceiver().receiveResults(rowsSent);
                    }
                } else {
                    sendContents();
                    if (sql != null) {
                        sendCommandComplete(sql, rowsSent);
                    }
                    result.getResultsReceiver().receiveResults(rowsSent);
                    processNext = false;
                }
            } catch (Throwable t) {
                result.getResultsReceiver().exceptionOccurred(t);
                return false;
            }
            return processNext;
        }

        private void flushResults(boolean force) {
            int avgRowsize = dataOut.writerIndex()/rowsInBuffer;
            if (force || (maxBufferSize - dataOut.writerIndex()) < (avgRowsize*2)) {
                sendContents();
                initBuffer(maxBufferSize / 8);
                rowsInBuffer = 0;
            }
        }
    }

    public static final String DEFAULT_ENCODING = "UTF8";
    public static final String CLIENT_ENCODING = "client_encoding";

    private ByteBuf dataOut;
    private OutputStreamWriter writer;

    private Properties props;
    private Charset encoding = Charset.forName("UTF-8");
    private String clientEncoding = DEFAULT_ENCODING;
    private ReflectionHelper clientProxy = new ReflectionHelper(ODBCClientRemote.class);
    private ChannelHandlerContext ctx;
    private int maxLobSize = (2*1024*1024); // 2 MB
    private final int maxBufferSize;
    private boolean requireSecure;

    private volatile ResultsFuture<Boolean> nextFuture;

    private SSLConfiguration config;

    public PgBackendProtocol(int maxLobSize, int maxBufferSize, SSLConfiguration config, boolean requireSecure) {
        this.maxLobSize = maxLobSize;
        this.maxBufferSize = maxBufferSize;
        this.config = config;
        this.requireSecure = requireSecure;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        this.ctx = ctx;
        ServiceInvocationStruct serviceStruct = (ServiceInvocationStruct)msg;

        try {
            Method m = this.clientProxy.findBestMethodOnTarget(serviceStruct.methodName, serviceStruct.args);
            try {
                //synchronize the local state as this can be
                //used by both nio and the engine threads
                synchronized (this) {
                    m.invoke(this, serviceStruct.args);
                }
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        } catch (Throwable e) {
            synchronized (this) {
                terminate(e);
            }
        }
    }

    @Override
    public void initialized(Properties props) {
        this.props = props;
        setEncoding(props.getProperty("client_encoding", this.clientEncoding), true);
    }

    @Override
    public void useClearTextAuthentication() {
        if (requireSecure && config != null && config.isClientEncryptionEnabled()) {
            sendErrorResponse(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40125));
        } else {
            sendAuthenticationCleartextPassword();
        }
    }

    @Override
    public void useAuthenticationGSS() {
        sendAuthenticationGSS();
    }

    @Override
    public void authenticationGSSContinue(byte[] serviceToken) {
        sendAuthenticationGSSContinue(serviceToken);
    }

    @Override
    public void authenticationSucess(int processId, int screctKey) {
        sendAuthenticationOk();
        // server_version, server_encoding, client_encoding, application_name,
        // is_superuser, session_authorization, DateStyle, IntervalStyle, TimeZone,
        // integer_datetimes, and standard_conforming_strings.
        // (server_encoding, TimeZone, and integer_datetimes were not reported
        // by releases before 8.0; standard_conforming_strings was not reported by
        // releases before 8.1; IntervalStyle was not reported by releases before 8.4;
        // application_name was not reported by releases before 9.0.)

        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", this.props.getProperty("DateStyle", "ISO"));
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", "8.2");
        sendParameterStatus("session_authorization", this.props.getProperty("user"));
        sendParameterStatus("standard_conforming_strings", "on");
        sendParameterStatus("integer_datetimes", "on");
        sendParameterStatus(APPLICATION_NAME, this.props.getProperty(APPLICATION_NAME, DEFAULT_APPLICATION_NAME));

        // TODO PostgreSQL TimeZone
        sendParameterStatus("TimeZone", "CET");

        sendBackendKeyData(processId, screctKey);
    }

    @Override
    public void prepareCompleted(String preparedName) {
        sendParseComplete();
    }

    @Override
    public void bindComplete() {
        sendBindComplete();
    }

    @Override
    public void errorOccurred(String msg) {
        sendErrorResponse(msg);
    }

    @Override
    public void errorOccurred(Throwable t) {
        sendErrorResponse(t);
    }

    @Override
    public void ready(boolean inTransaction, boolean failedTransaction) {
        sendReadyForQuery(inTransaction, failedTransaction);
    }

    public void setEncoding(String value, boolean init) {
        if (value == null || value.equals(this.clientEncoding)) {
            return;
        }
        this.clientEncoding = value;
        Charset cs = PGCharsetConverter.getCharset(value);
        if (cs != null) {
            this.encoding = cs;
            if (!init) {
                sendParameterStatus(CLIENT_ENCODING, value);
            }
        } else {
            LogManager.logWarning(LogConstants.CTX_ODBC, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40105, value));
        }
    }

    public String getClientEncoding() {
        return clientEncoding;
    }

    public Charset getEncoding() {
        return encoding;
    }

    @Override
    public void sendParameterDescription(int[] paramType) {
        startMessage('t');
        writeShort(paramType.length);
        for (int i = 0; i < paramType.length; i++) {
            writeInt(paramType[i]);
        }
        sendMessage();
    }

    @Override
    public void sendResultSetDescription(List<PgColInfo> cols,
            short[] resultColumnFormat) {
        sendRowDescription(cols, resultColumnFormat);
    }

    @Override
    public void sendResults(String sql, ResultSetImpl rs, List<PgColInfo> cols,
            ResultsFuture<Integer> result, CursorDirection direction,
            int rowCount, boolean describeRows, short[] resultColumnFormat) {
        if (nextFuture != null) {
            sendErrorResponse(new IllegalStateException("Pending results have not been sent")); //$NON-NLS-1$
        }

        if (describeRows) {
            sendRowDescription(cols, resultColumnFormat);
        }
        ResultsWorkItem r;
        try {
            Boolean singleResult = null;
            //TODO: all of these need a non-blocking form
            if (direction != CursorDirection.FORWARD) {
                switch (direction) {
                case ABSOLUTE:
                    singleResult = rs.absolute(rowCount);
                    break;
                case RELATIVE:
                    singleResult = rs.relative(rowCount);
                    break;
                case FIRST:
                    singleResult = rs.first();
                    break;
                case LAST:
                    singleResult = rs.last();
                    break;
                }
                rowCount = 1;
            }
            r = new ResultsWorkItem(cols, rs, result, rowCount, resultColumnFormat);
            r.sql = sql;
            if (singleResult != null) {
                ResultsFuture<Boolean> resultsFuture = new ResultsFuture<Boolean>();
                resultsFuture.getResultsReceiver().receiveResults(singleResult);
                r.processRow(resultsFuture);
            } else {
                r.run();
            }
        } catch (SQLException e) {
            result.getResultsReceiver().exceptionOccurred(e);
        }
    }

    @Override
    public void statementClosed() {
        startMessage('3');
        sendMessage();
    }

    @Override
    public void terminated() {
        trace("channel being terminated");
        // no need to send any reply; this is showing as malformed packet.
        this.ctx.channel().close();
    }

    @Override
    public void flush() {
        this.dataOut = null;
        this.writer = null;
        this.ctx.flush();
    }

    @Override
    public void emptyQueryReceived() {
        sendEmptyQueryResponse();
    }

    private void terminate(Throwable t) {
        LogManager.logDetail(LogConstants.CTX_ODBC, "channel being terminated - ", t.getMessage());
        this.ctx.channel().close();
    }

    private void sendEmptyQueryResponse() {
        startMessage('I');
        sendMessage();
    }

    @Override
    public void sendCommandComplete(String sql, Integer... count) {
        startMessage('C');
        String tag = getCompletionTag(sql, count);
        writeString(tag);
        sendMessage();
    }

    public static String getCompletionTag(String sql, Integer... count) {
        String tag;
        boolean useCount = false;
        if (StringUtil.startsWithIgnoreCase(sql, "BEGIN")) {
            tag = "BEGIN";
        } else if (StringUtil.startsWithIgnoreCase(sql, "START TRANSACTION")) {
            tag = "START TRANSACTION";
        } else if (sql.indexOf(' ') == -1 || sql.equals("CLOSE CURSOR")) {
            //should already be a completion tag
            tag = sql.toUpperCase();
            useCount = true;
        } else if (StringUtil.startsWithIgnoreCase(sql, "SET ")) {
            tag = "SET";
        } else {
            tag = SqlUtil.getKeyword(sql).toUpperCase();
            if (tag.equals("EXEC") || tag.equals("CALL")) {
                tag = "SELECT";
            }
            useCount = true;
        }
        if (useCount && count != null && !(tag.equalsIgnoreCase("ROLLBACK") || tag.equalsIgnoreCase("SAVEPOINT") || tag.equalsIgnoreCase("RELEASE"))) {
            for (int i = 0; i < count.length; i++) {
                tag += " " + count[i];
            }
        }
        return tag;
    }

    private void sendDataRow(ResultSet rs, List<PgColInfo> cols, short[] resultColumnFormat) throws SQLException, IOException {
        startMessage('D', -1);
        int lengthIndex = this.dataOut.writerIndex() - 4;
        writeShort(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            int dataBytesIndex = this.dataOut.writerIndex();
            writeInt(-1);
            if (!isBinary(cols.get(i).type)
                    || (resultColumnFormat==null || (resultColumnFormat.length==1?resultColumnFormat[0]==0:resultColumnFormat[i]==0))) {
                getContent(rs, cols.get(i), i+1);
            } else {
                getBinaryContent(rs, cols.get(i), i+1);
            }
            writer.flush();
            if (!rs.wasNull()) {
                int bytes = this.dataOut.writerIndex() - dataBytesIndex - 4;
                this.dataOut.setInt(dataBytesIndex, bytes);
            }
        }
        this.dataOut.setInt(lengthIndex, this.dataOut.writerIndex() - lengthIndex);
    }

    private void getBinaryContent(ResultSet rs, PgColInfo col, int column) throws SQLException, TeiidSQLException, IOException {
        switch (col.type) {
        case PG_TYPE_INT2:
            short sval = rs.getShort(column);
            if (!rs.wasNull()) {
                dataOut.writeShort(sval);
            }
            break;
        case PG_TYPE_INT4:
            int ival = rs.getInt(column);
            if (!rs.wasNull()) {
                dataOut.writeInt(ival);
            }
            break;
        case PG_TYPE_INT8:
            long lval = rs.getLong(column);
            if (!rs.wasNull()) {
                dataOut.writeLong(lval);
            }
            break;
        case PG_TYPE_FLOAT4:
            float fval = rs.getFloat(column);
            if (!rs.wasNull()) {
                dataOut.writeInt(Float.floatToIntBits(fval));
            }
            break;
        case PG_TYPE_FLOAT8:
            double dval = rs.getDouble(column);
            if (!rs.wasNull()) {
                dataOut.writeLong(Double.doubleToLongBits(dval));
            }
            break;
        case PG_TYPE_BYTEA:
            Blob blob = rs.getBlob(column);
            if (blob != null) {
                try {
                    byte[] bytes = ObjectConverterUtil.convertToByteArray(blob.getBinaryStream(), this.maxLobSize);
                    write(bytes);
                } catch(OutOfMemoryError e) {
                    throw new StreamCorruptedException("data too big: " + e.getMessage()); //$NON-NLS-1$
                }
            }
            break;
        case PG_TYPE_DATE:
            Date d = rs.getDate(column);
            if (d != null) {
                long millis = d.getTime();
                millis += TimestampWithTimezone.getCalendar().getTimeZone().getOffset(millis);
                long secs = TimestampUtils.toPgSecs(millis / 1000);
                dataOut.writeInt((int) (secs / 86400));
            }
            break;
        case PG_TYPE_TIME:
            Time time = rs.getTime(column);
            if (time != null) {
                long millis = time.getTime();
                millis += TimestampWithTimezone.getCalendar().getTimeZone().getOffset(millis);
                millis *= 1000;
                dataOut.writeLong(millis);
            }
            break;
        case PG_TYPE_TIMESTAMP_NO_TMZONE:
            Timestamp t = rs.getTimestamp(column);
            if (t != null) {
                long millis = t.getTime();
                millis += TimestampWithTimezone.getCalendar().getTimeZone().getOffset(millis);
                long secs = TimestampUtils.toPgSecs(millis / 1000);
                //convert from secs / millis to micro
                long pgMicros = secs * 1000000 + (millis % 1000)*1000;
                pgMicros += t.getNanos()/1000;
                dataOut.writeLong(pgMicros);
            }
            break;
        default:
            throw new AssertionError();
        }
    }

    private void getContent(ResultSet rs, PgColInfo col, int column) throws SQLException, TeiidSQLException, IOException {
        switch (col.type) {
            case PG_TYPE_BOOL:
                boolean b = rs.getBoolean(column);
                if (!rs.wasNull()) {
                    writer.write(b?"t":"f"); //$NON-NLS-1$ //$NON-NLS-2$
                }
                break;
            case PG_TYPE_BPCHAR:
            case PG_TYPE_DATE:
            case PG_TYPE_FLOAT4:
            case PG_TYPE_FLOAT8:
            case PG_TYPE_INT2:
            case PG_TYPE_INT4:
            case PG_TYPE_INT8:
            case PG_TYPE_NUMERIC:
            case PG_TYPE_TIME:
            case PG_TYPE_TIMESTAMP_NO_TMZONE:
            case PG_TYPE_VARCHAR:
                String value = rs.getString(column);
                if (value != null) {
                    writer.write(value);
                }
                break;
            case PG_TYPE_GEOGRAPHY:
            case PG_TYPE_GEOMETRY:
                Object val = rs.getObject(column);
                if (val != null) {
                    Blob blob = GeometryUtils.geometryToEwkb((AbstractGeospatialType)rs.unwrap(ResultSetImpl.class).getRawCurrentValue());
                    String hexewkb = PropertiesUtils.toHex(blob.getBytes(1, (int) blob.length()));
                    writer.write(hexewkb);
                }
                break;
            case PG_TYPE_XML:
            case PG_TYPE_TEXT:
            case PG_TYPE_JSON:
                Reader r = rs.getCharacterStream(column);
                if (r != null) {
                    try {
                        ObjectConverterUtil.write(writer, r, this.maxLobSize, false);
                    } finally {
                        r.close();
                    }
                }
                break;

            case PG_TYPE_BYTEA:
                Blob blob = rs.getBlob(column);
                if (blob != null) {
                    try {
                        String blobString = PGbytea.toPGString(ObjectConverterUtil.convertToByteArray(blob.getBinaryStream(), this.maxLobSize));
                        writer.write(blobString);
                    } catch(OutOfMemoryError e) {
                        throw new StreamCorruptedException("data too big: " + e.getMessage()); //$NON-NLS-1$
                    }
                }
                break;

            case PG_TYPE_CHARARRAY:
            case PG_TYPE_TEXTARRAY:
            case PG_TYPE_OIDARRAY:
            case PG_TYPE_BOOLARRAY:
            case PG_TYPE_INT2ARRAY:
            case PG_TYPE_INT4ARRAY:
            case PG_TYPE_INT8ARRAY:
            case PG_TYPE_FLOAT4ARRAY:
            case PG_TYPE_FLOAT8ARRAY:
            case PG_TYPE_NUMERICARRAY:
            case PG_TYPE_DATEARRAY:
            case PG_TYPE_TIMEARRAY:
            case PG_TYPE_TIMESTAMP_NO_TMZONEARRAY:
                {
                Array obj = rs.getArray(column);
                if (obj != null) {
                    writer.append("{");
                    boolean first = true;
                    Object array = obj.getArray();
                    int length = java.lang.reflect.Array.getLength(array);
                    for (int i = 0; i < length; i++) {
                        if (!first) {
                            writer.append(",");
                        }
                        else {
                            first = false;
                        }
                        Object o = java.lang.reflect.Array.get(array, i);
                        if (o != null) {
                            if (col.type == PG_TYPE_TEXTARRAY) {
                                escapeQuote(writer, o.toString());
                            }
                            else {
                                writer.append(o.toString());
                            }
                        }
                    }
                    writer.append("}");
                }
                }
                break;
            case PG_TYPE_INT2VECTOR:
            case PG_TYPE_OIDVECTOR:
                {
                ArrayImpl obj = (ArrayImpl)rs.getObject(column);
                if (obj != null) {
                    boolean first = true;
                    for (Object o:obj.getValues()) {
                        if (!first) {
                            writer.append(" ");
                        }
                        else {
                            first = false;
                        }
                        if (o != null) {
                            writer.append(o.toString());
                        } else {
                            writer.append('0');
                        }
                    }
                }
                }
                break;

            default:
                Object obj = rs.getObject(column);
                if (obj != null) {
                    throw new TeiidSQLException("unknown datatype "+ col.type+ " failed to convert");
                }
                break;
        }
    }

    public static void escapeQuote(Writer sb, String s) throws IOException {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }

            sb.append(c);
        }
        sb.append('"');
    }

    @Override
    public void sendSslResponse() {
        SSLEngine engine = null;
        try {
            if (config != null) {
                engine = config.getServerSSLEngine();
            }
        } catch (IOException e) {
            LogManager.logError(LogConstants.CTX_ODBC, e, RuntimePlugin.Util.gs(secureData()?RuntimePlugin.Event.TEIID40122:RuntimePlugin.Event.TEIID40016));
        } catch (GeneralSecurityException e) {
            LogManager.logError(LogConstants.CTX_ODBC, e, RuntimePlugin.Util.gs(secureData()?RuntimePlugin.Event.TEIID40122:RuntimePlugin.Event.TEIID40016));
        }
        ByteBuf buffer = Unpooled.buffer(1);
        ChannelPromise promise = this.ctx.newPromise();
        if (engine == null) {
            if (secureData()) {
                sendErrorResponse(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40124));
                return;
            }
            buffer.writeByte('N');
        } else {
            promise.addListener(new SSLEnabler(engine));
            buffer.writeByte('S');
        }
        this.ctx.writeAndFlush(buffer, promise);
    }

    private void sendErrorResponse(Throwable t) {
        if (t instanceof SQLException) {
            //we are just re-logging an exception raised by the engine
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODBC, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_ODBC, t, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40020)); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } else {
            //should be in the odbc layer
            LogManager.logError(LogConstants.CTX_ODBC, t, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40015));
        }
        SQLException e = TeiidSQLException.create(t);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState());
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }

    boolean isBinary(int oid) {
        switch (oid) {
        case PG_TYPE_INT2:
        case PG_TYPE_INT4:
        case PG_TYPE_INT8:
        case PG_TYPE_FLOAT4:
        case PG_TYPE_FLOAT8:
        case PG_TYPE_BYTEA:
        case PG_TYPE_DATE:
        case PG_TYPE_TIME:
        case PG_TYPE_TIMESTAMP_NO_TMZONE:
            return true;
        }
        return false;
    }

    private void sendRowDescription(List<PgColInfo> cols, short[] resultColumnFormat) {
        if (cols == null) {
            //send NoData
            startMessage('n');
            sendMessage();
            return;
        }
        startMessage('T');
        writeShort(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            PgColInfo info = cols.get(i);
            writeString(info.name);
            // rel ID
            writeInt(info.reloid);
            // attribute number of the column
            writeShort(info.attnum);
            // data type
            writeInt(info.type);
            // pg_type.typlen
            writeShort(getTypeSize(info.type, info.precision));
            // pg_attribute.atttypmod
            writeInt(info.mod);
            if (!isBinary(info.type) || resultColumnFormat == null) {
                //text
                writeShort(0);
            } else {
                writeShort(resultColumnFormat[resultColumnFormat.length == 1?0:i]);
            }
        }
        sendMessage();
    }

    private int getTypeSize(int pgType, int precision) {
        switch (pgType) {
        case PG_TYPE_VARCHAR:
            return Math.max(255, precision + 10);
        default:
            return precision + 4;
        }
    }

    private void sendErrorResponse(String message) {
        LogManager.logWarning(LogConstants.CTX_ODBC, message); //$NON-NLS-1$
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        write(0);
        sendMessage();
    }

    private void sendParseComplete() {
        startMessage('1');
        sendMessage();
    }

    private void sendBindComplete() {
        startMessage('2');
        sendMessage();
    }

    @Override
    public void sendPortalSuspended() {
        startMessage('s');
        sendMessage();
    }

    private void sendAuthenticationCleartextPassword() {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    private void sendAuthenticationGSS() {
        startMessage('R');
        writeInt(7);
        sendMessage();
    }

    private void sendAuthenticationGSSContinue(byte[] serviceToken)  {
        startMessage('R');
        writeInt(8);
        write(serviceToken);
        sendMessage();
    }

    private void sendAuthenticationOk() {
        startMessage('R');
        writeInt(0);
        sendMessage();
    }

    private void sendReadyForQuery(boolean inTransaction, boolean failedTransaction) {
        startMessage('Z');
        char c;
        if (failedTransaction) {
            // failed transaction block
            c = 'E';
        }
        else {
            if (inTransaction) {
                // in a transaction block
                c = 'T';
            } else {
                // idle
                c = 'I';
            }
        }
        write((byte) c);
        sendMessage();
    }

    private void sendBackendKeyData(int processId, int screctKey) {
        startMessage('K');
        writeInt(processId);
        writeInt(screctKey);
        sendMessage();
    }

    @Override
    public void sendParameterStatus(String param, String value) {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }

    @Override
    public void functionCallResponse(Object data, boolean binary) {
        startMessage('V', -1);
        int lengthIndex = this.dataOut.writerIndex() - 4;
        if (data == null) {
            if (binary) {
                writeInt(-1);
            } else {
                writeString("-1");
            }
        } else {
            int dataBytesIndex = this.dataOut.writerIndex();
            //write data - see the getContent and getBinaryContent methods
            //more than likely we'll want to change this method to take a resultset as well
            throw new AssertionError("not implemented");
            //int bytes = this.dataOut.writerIndex() - dataBytesIndex - 4;
            //this.dataOut.setInt(dataBytesIndex, bytes);
        }
        this.dataOut.setInt(lengthIndex, this.dataOut.writerIndex() - lengthIndex);
        sendMessage();
    }

    private void writeString(String s) {
        write(s.getBytes(this.encoding));
        write(0);
    }

    private void writeInt(int i) {
        dataOut.writeInt(i);
    }

    private void writeShort(int i) {
        dataOut.writeShort(i);
    }

    private void write(byte[] data) {
        dataOut.writeBytes(data);
    }

    private void write(int b) {
        dataOut.writeByte(b);
    }

    private void startMessage(char newMessageType) {
        startMessage(newMessageType, 32);
    }

    private void startMessage(char newMessageType, int estimatedLength) {
        if (estimatedLength > -1) {
            initBuffer(estimatedLength);
        }
        this.dataOut.writeByte((byte)newMessageType);
        int nextByte = this.dataOut.writerIndex() + 4;
        this.dataOut.ensureWritable(nextByte);
        this.dataOut.writerIndex(nextByte);
    }

    private void initBuffer(int estimatedLength) {
        this.dataOut = Unpooled.buffer(estimatedLength).order(ByteOrder.BIG_ENDIAN);
        ByteBufOutputStream cbos = new ByteBufOutputStream(this.dataOut);
        this.writer = new OutputStreamWriter(cbos, this.encoding);
    }

    private void sendMessage() {
        int pos = this.dataOut.writerIndex();
        this.dataOut.setInt(1, pos - 1);
        sendContents();
    }

    private void sendContents() {
        ByteBuf cb = this.dataOut;
        this.dataOut = null;
        this.writer = null;
        this.ctx.writeAndFlush(cb);
    }

    private static void trace(String... msg) {
        LogManager.logTrace(LogConstants.CTX_ODBC, (Object[])msg);
    }

    public boolean secureData() {
        return requireSecure && config != null && config.isSslEnabled();
    }

}
