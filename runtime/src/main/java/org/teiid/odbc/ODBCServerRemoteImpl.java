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

import static org.teiid.odbc.PGUtil.PG_TYPE_BPCHAR;
import static org.teiid.odbc.PGUtil.PG_TYPE_NUMERIC;
import static org.teiid.odbc.PGUtil.PG_TYPE_VARCHAR;
import static org.teiid.odbc.PGUtil.convertType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLEngine;

import org.ietf.jgss.GSSCredential;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.LogonException;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.SqlUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.deployers.PgCatalogMetadataStore;
import org.teiid.dqp.service.SessionService;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.LocalProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.ResultSetImpl;
import org.teiid.jdbc.StatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.odbc.ODBCClientRemote.CursorDirection;
import org.teiid.odbc.PGUtil.PgColInfo;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.GSSResult;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.ODBCClientInstance;
import org.teiid.transport.PgBackendProtocol;
import org.teiid.transport.PgFrontendProtocol.NullTerminatedStringDataInputStream;
import org.teiid.transport.pg.TimestampUtils;

/**
 */
public class ODBCServerRemoteImpl implements ODBCServerRemote {

    private static final boolean HONOR_DECLARE_FETCH_TXN = PropertiesUtils.getHierarchicalProperty("org.teiid.honorDeclareFetchTxn", false, Boolean.class); //$NON-NLS-1$

    public static final String CONNECTION_PROPERTY_PREFIX = "connection."; //$NON-NLS-1$
    private static final String UNNAMED = ""; //$NON-NLS-1$
    private static Pattern setPattern = Pattern.compile("set\\s+(\\w+)\\s+to\\s+((?:'[^']*')+)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE);//$NON-NLS-1$

    private static Pattern columnMetadataPattern = Pattern.compile("select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, " //$NON-NLS-1$
            + "c.relhasrules, c.relkind, c.oid, pg_get_expr\\(d.adbin, d.adrelid\\), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids " //$NON-NLS-1$
            + "from \\(\\(\\(pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.oid = (\\d+)\\) inner join pg_catalog.pg_attribute a " //$NON-NLS-1$
            + "on \\(not a.attisdropped\\) and a.attnum > 0 and a.attrelid = c.oid\\) inner join pg_catalog.pg_type t on t.oid = a.atttypid\\) left outer join pg_attrdef d " //$NON-NLS-1$
            + "on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum order by n.nspname, c.relname, attnum"); //$NON-NLS-1$

    private static Pattern pkPattern = Pattern.compile("select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname " +//$NON-NLS-1$
            "from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, " +//$NON-NLS-1$
            "pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = (E?(?:'[^']*')+) AND n.nspname = (E?(?:'[^']*')+).*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE);//$NON-NLS-1$

    private static Pattern pkKeyPattern = Pattern.compile("select ta.attname, ia.attnum, ic.relname, n.nspname, NULL from " + //$NON-NLS-1$
            "pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class ic, pg_catalog.pg_index i, " + //$NON-NLS-1$
            "pg_catalog.pg_namespace n where ic.relname = (E?(?:'[^']*')+) AND n.nspname = (E?(?:'[^']*')+) .*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    private Pattern fkPattern = Pattern.compile("select\\s+((?:'[^']*')+)::name as PKTABLE_CAT," + //$NON-NLS-1$
            "\\s+n2.nspname as PKTABLE_SCHEM," +  //$NON-NLS-1$
            "\\s+c2.relname as PKTABLE_NAME," +  //$NON-NLS-1$
            "\\s+a2.attname as PKCOLUMN_NAME," +  //$NON-NLS-1$
            "\\s+((?:'[^']*')+)::name as FKTABLE_CAT," +  //$NON-NLS-1$
            "\\s+n1.nspname as FKTABLE_SCHEM," +  //$NON-NLS-1$
            "\\s+c1.relname as FKTABLE_NAME," +  //$NON-NLS-1$
            "\\s+a1.attname as FKCOLUMN_NAME," +  //$NON-NLS-1$
            "\\s+i::int2 as KEY_SEQ," +  //$NON-NLS-1$
            "\\s+case ref.confupdtype" +  //$NON-NLS-1$
            "\\s+when 'c' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+when 'n' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+when 'd' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+when 'r' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+else 3::int2" +  //$NON-NLS-1$
            "\\s+end as UPDATE_RULE," +  //$NON-NLS-1$
            "\\s+case ref.confdeltype" +  //$NON-NLS-1$
            "\\s+when 'c' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+when 'n' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+when 'd' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+when 'r' then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+else 3::int2" +  //$NON-NLS-1$
            "\\s+end as DELETE_RULE," +                             //$NON-NLS-1$
            "\\s+ref.conname as FK_NAME," +  //$NON-NLS-1$
            "\\s+cn.conname as PK_NAME," +  //$NON-NLS-1$
            "\\s+case" +  //$NON-NLS-1$
            "\\s+when ref.condeferrable then" +  //$NON-NLS-1$
            "\\s+case" +  //$NON-NLS-1$
            "\\s+when ref.condeferred then (\\d)::int2" +  //$NON-NLS-1$
            "\\s+else (\\d)::int2" +  //$NON-NLS-1$
            "\\s+end" +  //$NON-NLS-1$
            "\\s+else (\\d)::int2" +  //$NON-NLS-1$
            "\\s+end as DEFERRABLITY" +  //$NON-NLS-1$
            "\\s+from" +  //$NON-NLS-1$
            "\\s+\\(\\(\\(\\(\\(\\(\\( \\(select cn.oid, conrelid, conkey, confrelid, confkey," +  //$NON-NLS-1$
            "\\s+generate_series\\(array_lower\\(conkey, 1\\), array_upper\\(conkey, 1\\)\\) as i," +  //$NON-NLS-1$
            "\\s+confupdtype, confdeltype, conname," +  //$NON-NLS-1$
            "\\s+condeferrable, condeferred" +  //$NON-NLS-1$
            "\\s+from pg_catalog.pg_constraint cn," +  //$NON-NLS-1$
            "\\s+pg_catalog.pg_class c," +  //$NON-NLS-1$
            "\\s+pg_catalog.pg_namespace n" +  //$NON-NLS-1$
            "\\s+where contype = 'f'" +  //$NON-NLS-1$
            "\\s+and\\s+con(f?)relid = c.oid" +  //$NON-NLS-1$
            "\\s+and\\s+relname = (E?(?:'[^']*')+)" +  //$NON-NLS-1$
            "\\s+and\\s+n.oid = c.relnamespace" +  //$NON-NLS-1$
            "\\s+and\\s+n.nspname = (E?(?:'[^']*')+)" +  //$NON-NLS-1$
            "\\s+\\) ref" +  //$NON-NLS-1$
            "\\s+inner join pg_catalog.pg_class c1" +  //$NON-NLS-1$
            "\\s+on c1.oid = ref.conrelid\\)" +  //$NON-NLS-1$
            "\\s+inner join pg_catalog.pg_namespace n1" +  //$NON-NLS-1$
            "\\s+on\\s+n1.oid = c1.relnamespace\\)" +  //$NON-NLS-1$
            "\\s+inner join pg_catalog.pg_attribute a1" +  //$NON-NLS-1$
            "\\s+on\\s+a1.attrelid = c1.oid" +  //$NON-NLS-1$
            "\\s+and\\s+a1.attnum = conkey\\[i\\]\\)" +  //$NON-NLS-1$
            "\\s+inner join pg_catalog.pg_class c2" +  //$NON-NLS-1$
            "\\s+on\\s+c2.oid = ref.confrelid\\)" +  //$NON-NLS-1$
            "\\s+inner join pg_catalog.pg_namespace n2" +  //$NON-NLS-1$
            "\\s+on\\s+n2.oid = c2.relnamespace\\)" +  //$NON-NLS-1$
            "\\s+inner join pg_catalog.pg_attribute a2" +  //$NON-NLS-1$
            "\\s+on\\s+a2.attrelid = c2.oid" +  //$NON-NLS-1$
            "\\s+and\\s+a2.attnum = confkey\\[i\\]\\)" +  //$NON-NLS-1$
            "\\s+left outer join pg_catalog.pg_constraint cn" +  //$NON-NLS-1$
            "\\s+on cn.conrelid = ref.confrelid" +  //$NON-NLS-1$
            "\\s+and cn.contype = 'p'\\)" +  //$NON-NLS-1$
            "\\s+order by ref.oid, ref.i", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    public static final String TYPE_QUERY = "SELECT typinput='array_in'::regproc, typtype   FROM pg_catalog.pg_type   LEFT   JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           " //$NON-NLS-1$
            + "join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     " //$NON-NLS-1$
            + "ON sp.nspoid = typnamespace  WHERE typname = $1  ORDER BY sp.r, pg_type.oid DESC LIMIT 1"; //$NON-NLS-1$

    //added for pg jdbc 42.2.13
    public static final String TYPE_QUERY_2 = "SELECT typinput='array_in'::regproc as is_array, typtype, typname   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           " //$NON-NLS-1$
            + "join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     " //$NON-NLS-1$
            + "ON sp.nspoid = typnamespace  WHERE typname = $1  ORDER BY sp.r, pg_type.oid DESC"; //$NON-NLS-1$

    private static final String PK_QUERY = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (i.keys).n AS KEY_SEQ, ci.relname AS PK_NAME " //$NON-NLS-1$
            + "FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary, " //$NON-NLS-1$
            + "             information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i" //$NON-NLS-1$
            + "     ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true"; //$NON-NLS-1$

    //added for pg jdbc 42.2.13
    private static final Pattern PK_QUERY_PATTERN = Pattern.compile(Pattern.quote("SELECT        result.TABLE_CAT,        result.TABLE_SCHEM,        result.TABLE_NAME,        result.COLUMN_NAME,        result.KEY_SEQ,        result.PK_NAME " //$NON-NLS-1$
            + "FROM      (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND ct.relname = ") //$NON-NLS-1$
            + "((?:'[^']*')+) " //$NON-NLS-1$
            + Pattern.quote("AND i.indisprimary  ) result where  result.A_ATTNUM = (result.KEYS).x  ORDER BY result.table_name, result.pk_name, result.key_seq"), Pattern.DOTALL); //$NON-NLS-1$

    private static final String PK_REPLACEMENT_QUERY = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, a.attnum AS KEY_SEQ, ci.relname AS PK_NAME\n" + //$NON-NLS-1$
            "FROM pg_catalog.pg_namespace n, pg_catalog.pg_class ct, pg_catalog.pg_class ci, pg_catalog.pg_attribute a, pg_catalog.pg_index i \n" + //$NON-NLS-1$
            "WHERE ct.oid=i.indrelid AND ci.oid=i.indexrelid AND a.attrelid=ci.oid AND ct.relnamespace = n.oid "; //$NON-NLS-1$

    private static Pattern cursorSelectPattern = Pattern.compile("DECLARE\\s+(\\S+)(\\s+BINARY)?(?:\\s+INSENSITIVE)?(\\s+(NO\\s+)?SCROLL)?\\s+CURSOR\\s+(?:WITH(?:OUT)? HOLD\\s+)?FOR\\s+(.*)", Pattern.CASE_INSENSITIVE|Pattern.DOTALL); //$NON-NLS-1$
    private static Pattern fetchPattern = Pattern.compile("FETCH(?:(?:\\s+(FORWARD|ABSOLUTE|RELATIVE))?\\s+(\\d+)\\s+(?:IN|FROM))?\\s+(\\S+)\\s*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern fetchFirstLastPattern = Pattern.compile("FETCH\\s+(FIRST|LAST)\\s+(?:IN|FROM)\\s+(\\S+)\\s*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern movePattern = Pattern.compile("MOVE(?:\\s+(FORWARD|BACKWARD))?\\s+(\\d+)\\s+(?:IN|FROM)\\s+(\\S+)\\s*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern closePattern = Pattern.compile("CLOSE (\\S+)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    private static Pattern deallocatePattern = Pattern.compile("DEALLOCATE(?:\\s+PREPARE)?\\s+(.*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern releasePattern = Pattern.compile("RELEASE\\s+(\\w+\\d?_*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern savepointPattern = Pattern.compile("SAVEPOINT\\s+(\\w+\\d?_*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern rollbackPattern = Pattern.compile("ROLLBACK(\\s+to)?\\s+(\\w+\\d+_*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    private static Pattern txnPattern = Pattern.compile("(BEGIN(?:\\s+READ\\s+ONLY)?|COMMIT|ROLLBACK)(\\s+(WORK|TRANSACTION))?", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    private TeiidDriver driver;
    private ODBCClientRemote client;
    private Properties props;
    private ConnectionImpl connection;
    private volatile boolean executing;
    private boolean errorOccurred;

    private volatile ResultsFuture<Boolean> executionFuture;

    // TODO: this is unbounded map; need to define some boundaries as to how many stmts each session can have
    private Map<String, Prepared> preparedMap = Collections.synchronizedMap(new HashMap<String, Prepared>());
    private Map<String, Portal> portalMap = Collections.synchronizedMap(new HashMap<String, Portal>());
    private Map<String, Cursor> cursorMap = Collections.synchronizedMap(new HashMap<String, Cursor>());
    private    LogonImpl logon;

    //state needed to implement cancel

    private static final long BIT_MASK = (1L << 32) -1;
    private static ConcurrentHashMap<Long, ODBCServerRemoteImpl> remotes = new ConcurrentHashMap<>();
    //TODO: there are ways to lookup pid, but nothing built-in. instead we'll increase the "security"
    //of cancellation with 63 random bits - the high bit needs to be 0 as pid must be positive
    private long secretKey = (long)(Math.random()*Long.MAX_VALUE);
    private volatile String executingStatement;

    public ODBCServerRemoteImpl(ODBCClientInstance client, TeiidDriver driver, LogonImpl logon) {
        this.driver = driver;
        this.client = client.getClient();
        this.logon = logon;
    }

    @Override
    public void initialize(Properties props, SocketAddress remoteAddress,
            SSLEngine sslEngine) {
        this.props = props;
        this.client.initialized(this.props);

        String user = props.getProperty("user"); //$NON-NLS-1$
        String database = props.getProperty("database"); //$NON-NLS-1$

        AuthenticationType authType = null;
        try {
            authType = getAuthenticationType(user, database);
        } catch (LogonException e) {
            errorOccurred(e);
            terminate();
            return;
        }

        switch (authType) {
        case USERPASSWORD:
            this.client.useClearTextAuthentication();
            break;
        case GSS:
            this.client.useAuthenticationGSS();
            break;
        case SSL:
            java.util.Properties info = new java.util.Properties();
            if (sslEngine != null) {
                info.put(LocalProfile.SSL_SESSION, sslEngine.getSession());
            }
            try {
                info.put(TeiidURL.CONNECTION.USER_NAME, user);
                //ssl auth should "logon" without sending an additional challenge
                logon(database, remoteAddress, info, null);
            } catch (SQLException e) {
                errorOccurred(e);
                terminate();
            }
            break;
        default:
            throw new AssertionError("Unsupported Authentication Type"); //$NON-NLS-1$
        }
    }

    private AuthenticationType getAuthenticationType(String user,
            String database) throws LogonException {
        SessionService ss = this.logon.getSessionService();
        if (ss == null) {
            return AuthenticationType.USERPASSWORD;
        }
        return ss.getAuthenticationType(database, null, user);
    }

    @Override
    public void logon(String databaseName, String user,
            NullTerminatedStringDataInputStream data,
            SocketAddress remoteAddress) {
        try {
            java.util.Properties info = new java.util.Properties();
            info.put(TeiidURL.CONNECTION.USER_NAME, user);

            AuthenticationType authType = getAuthenticationType(user, databaseName);

            String password = null;
            if (authType.equals(AuthenticationType.USERPASSWORD)) {
                password = data.readString();
            }
            else if (authType.equals(AuthenticationType.GSS)) {
                byte[] serviceToken = data.readServiceToken();
                GSSResult result = this.logon.neogitiateGssLogin(serviceToken, databaseName, null, user);
                serviceToken = result.getServiceToken();
                if (result.isAuthenticated()) {
                    info.put(ILogon.KRB5TOKEN, serviceToken);
                    if (!result.isNullContinuationToken()) {
                        this.client.authenticationGSSContinue(serviceToken);
                    }
                    // if delegation is in progress, participate in it.
                    if (result.getDelegationCredential() != null) {
                        info.put(GSSCredential.class.getName(), result.getDelegationCredential());
                    }
                }
                else {
                    this.client.authenticationGSSContinue(serviceToken);
                    return;
                }
            } else {
                throw new AssertionError("Unsupported Authentication Type"); //$NON-NLS-1$
            }

            logon(databaseName, remoteAddress, info, password);
        } catch (SQLException|LogonException|IOException e) {
            errorOccurred(e);
            terminate();
        }
    }

    private void logon(String databaseName, SocketAddress remoteAddress,
            java.util.Properties info, String password) throws SQLException {
        // this is local connection
        String url = "jdbc:teiid:"+databaseName; //$NON-NLS-1$

        if (password != null) {
            info.put(TeiidURL.CONNECTION.PASSWORD, password);
        }
        //since we can't mark this as local ahead of time, we must not allow
        //passthrough with no subject
        info.put(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "false"); //$NON-NLS-1$
        String applicationName = this.props.getProperty(PgBackendProtocol.APPLICATION_NAME);
        if (applicationName == null) {
            applicationName = PgBackendProtocol.DEFAULT_APPLICATION_NAME;
            this.props.put(PgBackendProtocol.APPLICATION_NAME, applicationName);
        }
        info.put(TeiidURL.CONNECTION.APP_NAME, applicationName);

        if (remoteAddress instanceof InetSocketAddress) {
            //we currently don't pass a hostname resolver as
            //this is typically the hostname of the load balancer
            SocketServerConnection.updateConnectionProperties(info, ((InetSocketAddress)remoteAddress).getAddress(), false, null);
        }

        this.connection =  driver.connect(url, info);
        //Propagate so that we can use in pg methods
        SessionMetadata sm = ((LocalServerConnection)this.connection.getServerConnection()).getWorkContext().getSession();
        sm.addAttachment(ODBCServerRemoteImpl.class, this);
        setConnectionProperties(this.connection);
        Enumeration<?> keys = this.props.propertyNames();
        while (keys.hasMoreElements()) {
            String key = (String)keys.nextElement();
            this.connection.setExecutionProperty(key, this.props.getProperty(key));
        }
        StatementImpl s = this.connection.createStatement();
        try {
            s.execute("select teiid_session_set('resolve_groupby_positional', true), teiid_session_set('pg_column_names', true)"); //$NON-NLS-1$
        } finally {
            s.close();
        }

        this.client.authenticationSucess((int)((secretKey>>32)&BIT_MASK), (int)(secretKey&BIT_MASK));
        ready();
        remotes.put(secretKey, this);
    }

    @Override
    public void cancel(int pid, int key) {
        long keyToCancel = pid;
        keyToCancel <<= 32;
        keyToCancel |= (key&BIT_MASK);
        ODBCServerRemoteImpl remote = remotes.get(keyToCancel);
        if (remote != null) {
            String current = remote.executingStatement;
            if (current != null) {
                try {
                    remote.connection.cancelRequest(current);
                } catch (TeiidProcessingException | TeiidComponentException e) {
                    LogManager.logDetail(LogConstants.CTX_ODBC, e, "Error cancelling statement"); //$NON-NLS-1$
                }
            }
        }
        terminate();
    }

    public static void setConnectionProperties(ConnectionImpl conn)
            throws SQLException {
        SessionMetadata sm = ((LocalServerConnection)conn.getServerConnection()).getWorkContext().getSession();
        VDB vdb = sm.getVdb();
        Properties p = vdb.getProperties();
        setConnectionProperties(conn, p);
    }

    public static void setConnectionProperties(ConnectionImpl conn,
            Properties p) {
        for (Map.Entry<Object, Object> entry : p.entrySet()) {
            String key = (String)entry.getKey();

            if (key.startsWith(CONNECTION_PROPERTY_PREFIX)) {
                conn.setExecutionProperty(key.substring(CONNECTION_PROPERTY_PREFIX.length()), (String) entry.getValue());
            }
        }
    }

    private void cursorExecute(String cursorName, final String sql, final ResultsFuture<Integer> completion, boolean scroll, final boolean binary) {
        try {
            // close if the name is already used or the unnamed prepare; otherwise
            // stmt is alive until session ends.
            this.preparedMap.remove(UNNAMED);
            Portal p = this.portalMap.remove(UNNAMED);
            if (p != null) {
                closePortal(p);
            }
            if (cursorName == null || cursorName.length() == 0) {
                cursorName = UNNAMED;
            }
            //implicit binding - although there's no requirement or expectation that this should be prepared
            final PreparedStatementImpl stmt = this.connection.prepareStatement(sql, scroll?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            internalCursorExecute(completion, cursorName, null, stmt, sql, binary?new short[] {1}:null, null);
        } catch (SQLException e) {
            completion.getResultsReceiver().exceptionOccurred(e);
        }
    }

    private void internalCursorExecute(final ResultsFuture<Integer> completion, final String cursorName, final String portalName,
            PreparedStatementImpl stmt, String sql, short[] resultColumnFormat, List<PgColInfo> cols) throws SQLException {
        Cursor cursor = cursorMap.get(cursorName);
        if (cursor != null) {
            errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40111, cursorName));
            return;
        }

        this.executionFuture = stmt.submitExecute(ResultsMode.RESULTSET, null);
        this.executingStatement = stmt.getRequestIdentifier();
        this.executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
            @Override
            public void onCompletion(ResultsFuture<Boolean> future) {
                executionFuture = null;
                try {
                    if (future.get()) {
                        cursorMap.put(cursorName,
                                new Cursor(cursorName, sql, stmt, stmt.getResultSet(), cols==null?getPgColInfo(stmt.getResultSet().getMetaData()):cols, resultColumnFormat));
                        if (portalName != null) {
                            //we've upgraded the portal into a cursor, and need to remove it from the former
                            //to prevent closure
                            portalMap.remove(portalName);
                        }
                        client.sendCommandComplete("DECLARE CURSOR"); //$NON-NLS-1$
                        completion.getResultsReceiver().receiveResults(0);
                    }
                } catch (Throwable e) {
                    completion.getResultsReceiver().exceptionOccurred(e);
                }
            }
        });
    }

    private void cursorFetch(String cursorName, CursorDirection direction, int rows, final ResultsFuture<Integer> completion) throws SQLException {
        Cursor cursor = this.cursorMap.get(cursorName);
        if (cursor == null) {
            throw new SQLException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40078, cursorName));
        }
        if (rows < 1) {
            throw new SQLException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40112, cursorName, rows));
        }
        this.client.sendResults("FETCH", cursor.rs, cursor.prepared.columnMetadata, completion, direction, rows, true, cursor.resultColumnFormat); //$NON-NLS-1$
    }

    private void cursorMove(String prepareName, String direction, final int rows, final ResultsFuture<Integer> completion) throws SQLException {

        // win odbc driver sending a move after close; and error is ending up in failure; since the below
        // is not harmful it is ok to send empty move.
        if (rows == 0) {
            client.sendCommandComplete("MOVE", 0); //$NON-NLS-1$
            completion.getResultsReceiver().receiveResults(0);
            return;
        }

        final Cursor cursor = this.cursorMap.get(prepareName);
        if (cursor == null) {
            throw new SQLException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40078, prepareName));
        }

        final boolean forward = direction == null || direction.equalsIgnoreCase("forward"); //$NON-NLS-1$
        Runnable r = new Runnable() {
            public void run() {
                run(null, 0);
            }
            public void run(ResultsFuture<Boolean> next, int i) {
                for (; i < rows; i++) {
                    try {
                        if (next == null) {
                            if (forward) {
                                next = cursor.rs.submitNext();
                            } else {
                                //TODO: we know that we are scrollable in this case, we should just
                                //use an absolute positioning
                                //as of now previous is non-blocking
                                next = StatementImpl.booleanFuture(cursor.rs.previous());
                            }
                        }
                        if (!next.isDone()) {
                            final int current = i;
                            next.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
                                @Override
                                public void onCompletion(
                                        ResultsFuture<Boolean> future) {
                                    run(future, current); //restart later
                                }
                            });
                            return;
                        }
                        if (!next.get()) {
                            break; //no next row
                        }
                        next = null;
                    } catch (Throwable e) {
                        completion.getResultsReceiver().exceptionOccurred(e);
                        return;
                    }
                }
                if (!completion.isDone()) {
                    client.sendCommandComplete("MOVE", i); //$NON-NLS-1$
                    completion.getResultsReceiver().receiveResults(i);
                }
            }
        };
        r.run();
    }

    private void cursorClose(String prepareName) throws SQLException {
        Cursor cursor = this.cursorMap.remove(prepareName);
        if (cursor != null) {
            closePortal(cursor);
            this.client.sendCommandComplete("CLOSE CURSOR"); //$NON-NLS-1$
        }
    }

    private void sqlExecute(final String sql, final ResultsFuture<Integer> completion) throws SQLException {
        String modfiedSQL = fixSQL(sql);
        final boolean autoCommit = connection.getAutoCommit();
        final StatementImpl stmt = connection.createStatement();
        executionFuture = stmt.submitExecute(modfiedSQL, null);
        this.executingStatement = stmt.getRequestIdentifier();
        completion.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
            public void onCompletion(ResultsFuture<Integer> future) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    LogManager.logDetail(LogConstants.CTX_ODBC, e, "Error closing statement"); //$NON-NLS-1$
                }
            }
        });
        executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
            @Override
            public void onCompletion(ResultsFuture<Boolean> future) {
                executionFuture = null;
                try {
                    if (future.get()) {
                        List<PgColInfo> cols = getPgColInfo(stmt.getResultSet().getMetaData());
                        String tag = PgBackendProtocol.getCompletionTag(sql);
                        client.sendResults(sql, stmt.getResultSet(), cols, completion, CursorDirection.FORWARD, -1, tag.equals("SELECT") || tag.equals("SHOW"), null); //$NON-NLS-1$ //$NON-NLS-2$
                    } else {
                        if (autoCommit ^ connection.getAutoCommit()) {
                            //toggled autocommit, any portal is now invalid
                            closePortals();
                        }
                        sendUpdateCount(sql, stmt);
                        updateSessionProperties();
                        completion.getResultsReceiver().receiveResults(1);
                    }
                } catch (Throwable e) {
                    if (!completion.isDone()) {
                        completion.getResultsReceiver().exceptionOccurred(e);
                    }
                }
            }
        });
    }

    private void sendUpdateCount(final String sql,
            final StatementImpl stmt) throws SQLException {
        String keyword = SqlUtil.getKeyword(sql);
        if (keyword.equalsIgnoreCase("INSERT")) { //$NON-NLS-1$
            client.sendCommandComplete(keyword, 0, stmt.getUpdateCount());
        } else {
            client.sendCommandComplete(sql, stmt.getUpdateCount());
        }
    }

    @Override
    public void prepare(String prepareName, String sql, int[] paramType) {
        if (prepareName == null || prepareName.length() == 0) {
            prepareName  = UNNAMED;
        }

        if (sql != null) {
            PreparedStatementImpl stmt = null;
            try {
                // close if the name is already used or the unnamed prepare; otherwise
                // stmt is alive until session ends.
                if (prepareName.equals(UNNAMED)) {
                    this.preparedMap.remove(prepareName);
                } else {
                    Prepared previous = this.preparedMap.get(prepareName);
                    if (previous != null) {
                        errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40110, prepareName));
                        return;
                    }
                }
                //just pull the initial information - leave statement formation until binding
                String modfiedSQL = fixSQL(sql);
                Matcher m = null;
                String cursorName = null;
                boolean scroll = false;
                if ((m = cursorSelectPattern.matcher(modfiedSQL)).matches()){
                    //per docs binary is irrelevant as that should be specified by bind
                    modfiedSQL = fixSQL(m.group(5));
                    cursorName = normalizeName(m.group(1));
                    scroll = m.group(3) != null && m.group(4) == null;
                }
                if (!modfiedSQL.trim().isEmpty()) {
                    stmt = this.connection.prepareStatement(modfiedSQL, scroll?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                }
                Prepared prepared = new Prepared(prepareName, sql, modfiedSQL, paramType, stmt==null?null:getPgColInfo(stmt.getMetaData()), cursorName);
                this.preparedMap.put(prepareName, prepared);
                this.client.prepareCompleted(prepareName);
            } catch (SQLException e) {
                if (e.getCause() instanceof TeiidProcessingException) {
                    LogManager.logWarning(LogConstants.CTX_ODBC, e.getCause(), RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40020));
                }
                errorOccurred(e);
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                }
            }
        }
    }

    private long readLong(byte[] bytes, int length) {
        long val = 0;
        for (int k = 0; k < length; k++) {
            val += ((long)(bytes[k] & 255) << ((length - k - 1)*8));
        }
        return val;
    }

    @Override
    public void bindParameters(String bindName, String prepareName, Object[] params, int resultCodeCount, short[] resultColumnFormat, Charset encoding) {
        // An unnamed portal is destroyed at the end of the transaction, or as soon as
        // the next Bind statement specifying the unnamed portal as destination is issued.
        if (bindName == null || bindName.length() == 0) {
            Portal p = this.portalMap.remove(UNNAMED);
            if (p != null) {
                closePortal(p);
            }
            bindName  = UNNAMED;
        } else if (this.portalMap.get(bindName) != null || this.cursorMap.get(bindName) != null) {
            errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40111, bindName));
            return;
        }

        if (prepareName == null || prepareName.length() == 0) {
            prepareName  = UNNAMED;
        }

        Prepared prepared = this.preparedMap.get(prepareName);
        if (prepared == null) {
            errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40077, prepareName));
            return;
        }
        PreparedStatementImpl stmt = null;
        try {
            if (!prepared.modifiedSql.isEmpty()) {
                stmt = this.connection.prepareStatement(prepared.modifiedSql);
            }
            for (int i = 0; i < params.length; i++) {
                if (stmt == null) {
                    errorOccurred("cannot bind parameters on an empty statement"); //$NON-NLS-1$
                    return;
                }
                Object param = params[i];
                if (param instanceof byte[] && prepared.paramType.length > i) {
                    int oid = prepared.paramType[i];
                    switch (oid) {
                    case PGUtil.PG_TYPE_UNSPECIFIED:
                        //TODO: should infer type from the parameter metadata from the parse message
                        break;
                    case PGUtil.PG_TYPE_BYTEA:
                        break;
                    case PGUtil.PG_TYPE_INT2:
                        param = (short)readLong((byte[])param, 2);
                        break;
                    case PGUtil.PG_TYPE_INT4:
                        param = (int)readLong((byte[])param, 4);
                        break;
                    case PGUtil.PG_TYPE_INT8:
                        param = readLong((byte[])param, 8);
                        break;
                    case PGUtil.PG_TYPE_FLOAT4:
                        param = Float.intBitsToFloat((int)readLong((byte[])param, 4));
                        break;
                    case PGUtil.PG_TYPE_FLOAT8:
                        param = Double.longBitsToDouble(readLong((byte[])param, 8));
                        break;
                    case PGUtil.PG_TYPE_TIME:
                        //micro to millis
                        param = TimestampUtils.convertToTime(readLong((byte[])param, 8)/1000, TimestampWithTimezone.getCalendar().getTimeZone());
                        break;
                    case PGUtil.PG_TYPE_DATE:
                        param = TimestampUtils.toDate(TimestampWithTimezone.getCalendar().getTimeZone(), (int)readLong((byte[])param, 4));
                        break;
                    case PGUtil.PG_TYPE_TIMESTAMP_NO_TMZONE:
                        param = TimestampUtils.toTimestamp(readLong((byte[])param, 8), TimestampWithTimezone.getCalendar().getTimeZone());
                        break;
                    default:
                        //start with the string conversion
                        param = new String((byte[])param, encoding);
                        break;
                    }
                }
                stmt.setObject(i+1, param);
            }
            this.portalMap.put(bindName, new Portal(bindName, prepared, resultColumnFormat, stmt));
            this.client.bindComplete();
            stmt = null;
        } catch (SQLException e) {
            errorOccurred(e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public void unsupportedOperation(String msg) {
        errorOccurred(msg);
    }

    @Override
    public void execute(String bindName, int maxRows) {
        if (beginExecution()) {
            errorOccurred("Awaiting asynch result"); //$NON-NLS-1$
            return;
        }
        if (bindName == null || bindName.length() == 0) {
            bindName  = UNNAMED;
        }
        if (maxRows == 0) {
            maxRows = -1;
        }
        // special case cursor execution through portal
        final Cursor cursor = this.cursorMap.get(bindName);
        if (cursor != null) {
            sendCursorResults(cursor, maxRows);
            return;
        }

        final Portal query = this.portalMap.get(bindName);
        if (query == null) {
            errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40078, bindName));
            return;
        }

        if (query.stmt == null) {
            this.client.emptyQueryReceived();
            this.doneExecuting();
            return;
        }

        sendPortalResults(maxRows, query);
    }

    private void sendPortalResults(final int maxRows, final Portal query) {
        if (query.rs != null) {
            //this is a suspended portal
            sendCursorResults(query, maxRows);
            return;
        }
        final PreparedStatementImpl stmt = query.stmt;
        try {
            //prepared cursor case
            if (query.prepared.cursorName != null) {
                ResultsFuture<Integer> results = new ResultsFuture<Integer>();
                results.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
                    public void onCompletion(ResultsFuture<Integer> future) {
                        try {
                            future.get();
                            doneExecuting();
                        } catch (ExecutionException e) {
                            Throwable cause = e;
                            while (cause instanceof ExecutionException && cause.getCause() != null && cause != cause.getCause()) {
                                cause = cause.getCause();
                            }
                            errorOccurred(cause);
                        } catch (Throwable e) {
                            errorOccurred(e);
                        }
                    };
                });
                //TODO: pass in the full Portal/Prepared - likely requires getting rid of the Cursor class
                internalCursorExecute(results, query.prepared.cursorName, query.name, stmt, query.prepared.sql, query.resultColumnFormat, query.prepared.columnMetadata);
                return;
            }
            this.executionFuture = stmt.submitExecute(ResultsMode.EITHER, null);
            this.executingStatement = stmt.getRequestIdentifier();
            executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
                @Override
                public void onCompletion(ResultsFuture<Boolean> future) {
                    executionFuture = null;
                    try {
                        if (future.get()) {
                            query.rs = stmt.getResultSet();
                            sendCursorResults(query, maxRows);
                        } else {
                            sendUpdateCount(query.prepared.sql, stmt);
                            updateSessionProperties();
                            doneExecuting();
                        }
                    } catch (ExecutionException e) {
                        if (e.getCause() != null) {
                            errorOccurred(e.getCause());
                        } else {
                            errorOccurred(e);
                        }
                    } catch (Throwable e) {
                        errorOccurred(e);
                    }
                }
            });
        } catch (SQLException e) {
            errorOccurred(e);
        }
    }

    private void sendCursorResults(final Portal cursor, final int fetchSize) {
        ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        this.client.sendResults(null, cursor.rs, cursor.prepared.columnMetadata, result, CursorDirection.FORWARD, fetchSize, false, cursor.resultColumnFormat);
        result.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
            public void onCompletion(ResultsFuture<Integer> future) {
                try {
                    int rowsSent = future.get();
                    if (rowsSent < fetchSize || fetchSize <= 0) {
                        client.sendCommandComplete(cursor.prepared.sql, rowsSent);
                    }
                    else {
                        client.sendPortalSuspended();
                    }
                    doneExecuting();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    errorOccurred(e.getCause());
                }
            };
        });
    }

    private String fixSQL(String sql) {
        String modified = modifySQL(sql);
        if (modified != null && !modified.equals(sql)) {
            LogManager.logDetail(LogConstants.CTX_ODBC, "Modified Query:", modified); //$NON-NLS-1$
        }
        return modified;
    }

    private String modifySQL(String sql) {
        String modified = sql;
        if (sql == null) {
            return null;
        }
        Matcher m = null;
        // selects are coming with "select\t" so using a space after "select" does not always work
        if (StringUtil.startsWithIgnoreCase(sql, "select")) { //$NON-NLS-1$
            if ((m = pkPattern.matcher(modified)).matches()) {
                return new StringBuffer("SELECT k.Name AS attname, convert(Position, short) AS attnum, TableName AS relname, SchemaName AS nspname, TableName AS relname") //$NON-NLS-1$
                      .append(" FROM SYS.KeyColumns k") //$NON-NLS-1$
                      .append(" WHERE ") //$NON-NLS-1$
                      .append(" UCASE(SchemaName)").append(" LIKE UCASE(").append(m.group(2)).append(")")//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                      .append(" AND UCASE(TableName)") .append(" LIKE UCASE(").append(m.group(1)).append(")")//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                      .append(" AND KeyType LIKE 'Primary'") //$NON-NLS-1$
                      .append(" ORDER BY attnum").toString(); //$NON-NLS-1$
            }
            else if ((m = pkKeyPattern.matcher(modified)).matches()) {
                String tableName = m.group(1);
                if (tableName.endsWith("_pkey'")) { //$NON-NLS-1$
                    tableName = tableName.substring(0, tableName.length()-6) + '\'';
                    return "select ia.attname, ia.attnum, ic.relname, n.nspname, NULL "+ //$NON-NLS-1$
                        "from pg_catalog.pg_attribute ia, pg_catalog.pg_class ic, pg_catalog.pg_namespace n, Sys.KeyColumns kc "+ //$NON-NLS-1$
                        "where ic.relname = "+tableName+" AND n.nspname = "+m.group(2)+" AND "+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        "n.oid = ic.relnamespace AND ia.attrelid = ic.oid AND kc.SchemaName = n.nspname " +//$NON-NLS-1$
                        "AND kc.TableName = ic.relname AND kc.KeyType = 'Primary' AND kc.Name = ia.attname order by ia.attnum";//$NON-NLS-1$
                }
                return "SELECT NULL, NULL, NULL, NULL, NULL FROM (SELECT 1) as X WHERE 0=1"; //$NON-NLS-1$
            }
            else if ((m = fkPattern.matcher(modified)).matches()){
                String baseQuery = "SELECT PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM, "+//$NON-NLS-1$
                        "FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY "+//$NON-NLS-1$
                        "FROM SYS.ReferenceKeyColumns WHERE  "; //$NON-NLS-1$
                if ("f".equals(m.group(14))) { //$NON-NLS-1$
                    //exported keys
                    return baseQuery + "PKTABLE_NAME = " + m.group(15)+" and PKTABLE_SCHEM = "+m.group(16);//$NON-NLS-1$ //$NON-NLS-2$
                }
                //imported keys
                return baseQuery + "FKTABLE_NAME = " + m.group(15)+" and FKTABLE_SCHEM = "+m.group(16);//$NON-NLS-1$ //$NON-NLS-2$
            }
            else if (modified.startsWith("SELECT name FROM master..sysdatabases")) { //$NON-NLS-1$
                return "SELECT 'Teiid'"; //$NON-NLS-1$
            }
            else if (modified.equalsIgnoreCase("select db_name() dbname")) { //$NON-NLS-1$
                return "SELECT current_database()"; //$NON-NLS-1$
            }
            else if (sql.equalsIgnoreCase("select current_schema()")) { //$NON-NLS-1$
                // since teiid can work with multiple schemas at a given time
                // this call resolution is ambiguous
                return "SELECT ''";  //$NON-NLS-1$
            }
            else if (sql.equals("SELECT typinput='array_in'::regproc, typtype FROM pg_catalog.pg_type WHERE typname = $1")) { //$NON-NLS-1$
                return "SELECT substring(typname,1,1) = '_', typtype FROM pg_catalog.pg_type WHERE typname = ?"; //$NON-NLS-1$
            }
            if ((m = columnMetadataPattern.matcher(modified)).matches()) {
                return "select t1.schemaname as nspname, c.relname, t1.name as attname, t.oid as attypid, t.typname, convert(t1.Position, short) as attnum, t.typlen as attlen," //$NON-NLS-1$
                        + PgCatalogMetadataStore.TYPMOD + " as atttypmod, "  //$NON-NLS-1$
                        + "CASE WHEN (t1.NullType = 'No Nulls') THEN true ELSE false END as attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(case when t1.IsAutoIncremented then 'nextval(' else t1.DefaultValue end, c.oid), " //$NON-NLS-1$
                        + " case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids from sys.columns as t1, pg_catalog.matpg_datatype as t, pg_catalog.pg_class c where c.relnspname=t1.schemaname and c.relname=t1.tablename and t1.DataType = t.Name and c.oid = " //$NON-NLS-1$
                        + m.group(1)
                        + " order by nspname, relname, attnum"; //$NON-NLS-1$
            }
            //we don't support generate_series or the natural join syntax
            if (modified.equals(TYPE_QUERY)) {
                return "select typname like '\\_%' escape '\\', typname from pg_catalog.pg_type where typname = $1"; //$NON-NLS-1$
            } else if (modified.equals(TYPE_QUERY_2)) {
                return "select typname like '\\_%' escape '\\' is_array, typtype, typname from pg_catalog.pg_type where typname = $1"; //$NON-NLS-1$
            }
            //we don't support _pg_expandarray and referencing elements by name
            if (modified.startsWith(PK_QUERY)) {
                return PK_REPLACEMENT_QUERY + modified.substring(PK_QUERY.length());
            }
            Matcher matcher = PK_QUERY_PATTERN.matcher(modified);
            if (matcher.matches()) {
                return PK_REPLACEMENT_QUERY + " and ct.relname = " + matcher.group(1) + " ORDER BY table_name, pk_name, key_seq"; //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        else if (sql.equalsIgnoreCase("show max_identifier_length")){ //$NON-NLS-1$
            return "select 63"; //$NON-NLS-1$
        }
        else if ((m = setPattern.matcher(sql)).matches()) {
            return "SET " + m.group(1) + " " + m.group(2); //$NON-NLS-1$ //$NON-NLS-2$
        }
        else if ((m = txnPattern.matcher(sql)).matches()) {
            if (StringUtil.startsWithIgnoreCase(m.group(1), "BEGIN")) { //$NON-NLS-1$
                return "START TRANSACTION"; //$NON-NLS-1$
            }
            return m.group(1);
        }
        else if ((m = rollbackPattern.matcher(modified)).matches()) {
            return "set \"dummy-update-pg-odbc\" 0"; //$NON-NLS-1$
        }
        else if ((m = savepointPattern.matcher(sql)).matches()) {
            return "set \"dummy-update-pg-odbc\" 0"; //$NON-NLS-1$
        }
        else if ((m = releasePattern.matcher(sql)).matches()) {
            return "set \"dummy-update-pg-odbc\" 0"; //$NON-NLS-1$
        }
        //quickly check if rewrite is needed by the scriptreader
        for (int i = 0; i < modified.length(); i++) {
            switch (modified.charAt(i)) {
            case ':':
            case '~':
            case '(':
            case '$':
                ScriptReader reader = new ScriptReader(modified);
                reader.setRewrite(true);
                try {
                    return reader.readStatement();
                } catch (IOException e) {
                    //can't happen
                }
            }
        }
        return modified;
    }

    @Override
    public void executeQuery(String query) {
        if (beginExecution()) {
            errorOccurred("Awaiting asynch result"); //$NON-NLS-1$
            ready();
            return;
        }
        //46.2.3 Note that a simple Query message also destroys the unnamed portal.
        Portal p = this.portalMap.remove(UNNAMED);
        if (p != null) {
            closePortal(p);
        }
        this.preparedMap.remove(UNNAMED);
        query = query.trim();
        if (query.length() == 0) {
            client.emptyQueryReceived();
            ready();
        }

        QueryWorkItem r = new QueryWorkItem(query);
        r.run();
    }

    private boolean beginExecution() {
        if (this.executionFuture != null) {
            return true;
        }
        this.executing = true;
        return false;
    }

    public boolean isExecuting() {
        return executing;
    }

    public boolean isErrorOccurred() {
        return errorOccurred;
    }

    @Override
    public void getParameterDescription(String prepareName) {
        if (prepareName == null || prepareName.length() == 0) {
            prepareName  = UNNAMED;
        }
        Prepared query = this.preparedMap.get(prepareName);
        if (query == null) {
            errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40079, prepareName));
            return;
        }
        // The response is a ParameterDescription message describing the parameters needed by the statement,
        this.client.sendParameterDescription(query.paramType);

        // followed by a RowDescription message describing the rows that will be returned when the statement
        // is eventually executed (or a NoData message if the statement will not return rows).
        this.client.sendResultSetDescription(query.columnMetadata, null);
    }

    private void errorOccurred(String error) {
        this.client.errorOccurred(error);
        synchronized (this) {
            this.errorOccurred = true;
            doneExecuting();
        }
    }

    public void errorOccurred(Throwable error) {
        this.client.errorOccurred(error);
        synchronized (this) {
            this.errorOccurred = true;
            doneExecuting();
        }
    }

    @Override
    public void getResultSetMetaDataDescription(String bindName) {
        if (bindName == null || bindName.length() == 0) {
            bindName  = UNNAMED;
        }
        Portal query = this.portalMap.get(bindName);
        if (query == null) {
            errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40078, bindName));
        }
        else {
            if (query.prepared.cursorName != null) {
                //there are no columns for a prepared declare
                this.client.sendResultSetDescription(null, query.resultColumnFormat);
            } else {
                this.client.sendResultSetDescription(query.prepared.columnMetadata, query.resultColumnFormat);
            }
        }
    }

    @Override
    public void sync() {
        ready();
    }

    protected void doneExecuting() {
        executing = false;
    }

    private void ready() {
        boolean inTxn = false;
        boolean failedTxn = false;
        try {
            if (!this.connection.getAutoCommit()) {
                inTxn = true;
            }
        } catch (SQLException e) {
            failedTxn = true;
        }
        synchronized (this) {
            this.errorOccurred = false;
        }
        this.executingStatement = null;
        this.client.ready(inTxn, failedTxn);
    }

    @Override
    public void closeBoundStatement(String bindName) {
        if (bindName == null || bindName.length() == 0) {
            bindName  = UNNAMED;
        }
        Portal query = this.portalMap.remove(bindName);
        if (query != null) {
            closePortal(query);
        }
        this.client.statementClosed();
    }

    private void closePortal(Portal query) {
        ResultSet rs = query.rs;
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                LogManager.logDetail(LogConstants.CTX_ODBC, e, "Did not successfully close portal", query.name); //$NON-NLS-1$
            }
            query.rs = null;
        }
        if (query.stmt != null) {
            try {
                query.stmt.close();
            } catch (SQLException e) {
                LogManager.logDetail(LogConstants.CTX_ODBC, e, "Did not successfully close portal", query.name); //$NON-NLS-1$
            }
        }
    }

    @Override
    public void closePreparedStatement(String preparedName) {
        if (preparedName == null || preparedName.length() == 0) {
            preparedName  = UNNAMED;
        }
        Prepared query = this.preparedMap.remove(preparedName);
        if (query != null) {
            synchronized (this.portalMap) {
                for (Iterator<Portal> iter = this.portalMap.values().iterator(); iter.hasNext();) {
                    Portal p = iter.next();
                    if (p.prepared.name.equals(preparedName)) {
                        iter.remove();
                    }
                    closePortal(p);
                }
            }
        }
        this.client.statementClosed();
    }

    @Override
    public void terminate() {
        remotes.remove(this.secretKey);
        closePortals();

        this.preparedMap.clear();
        try {
            if (this.connection != null) {
                if (!this.connection.getAutoCommit()) {
                    this.connection.rollback(false);
                }
                this.connection.close();
            }
        } catch (SQLException e) {
            //ignore
        }
        this.client.terminated();
    }

    private void closePortals() {
        for (Portal p: this.portalMap.values()) {
            closePortal(p);
        }
        for (Cursor p: this.cursorMap.values()) {
            closePortal(p);
        }
        this.portalMap.clear();
        this.cursorMap.clear();
    }

    @Override
    public void flush() {
        this.client.flush();
    }

    @Override
    public void functionCall(int oid, Object[] params, short resultFormat) {
        //try (PreparedStatement s = this.connection.prepareStatement("select proname from pg_proc where oid = ?")) { //$NON-NLS-1$
        errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40081));
        this.ready();
    }

    @Override
    public void sslRequest() {
        this.client.sendSslResponse();
    }

    private void updateSessionProperties() {
        String encoding = getEncoding();
        if (encoding != null) {
            //this may be unnecessary
            this.client.setEncoding(encoding, false);
        }
        String appName = this.connection.getExecutionProperty(PgBackendProtocol.APPLICATION_NAME);
        if (appName != null) {
            String existing = props.getProperty(PgBackendProtocol.APPLICATION_NAME);
            if (!EquivalenceUtil.areEqual(appName, existing)) {
                try {
                    SessionMetadata sm = ((LocalServerConnection)connection.getServerConnection()).getWorkContext().getSession();
                    sm.setApplicationName(appName);
                } catch (SQLException e) {
                    //connection invalid
                }
                this.client.sendParameterStatus(PgBackendProtocol.APPLICATION_NAME, appName);
                this.props.put(PgBackendProtocol.APPLICATION_NAME, appName);
            }
        }
    }

    public String getEncoding() {
        return this.connection.getExecutionProperty(PgBackendProtocol.CLIENT_ENCODING);
    }

    static String normalizeName(String name) {
        if (name.length() > 1 && name.startsWith("\"") && name.endsWith("\"")) {
            return StringUtil.replaceAll(name.substring(1, name.length() - 1), "\"", "\"\"");
        }
        //--we are not consistently dealing with identifier naming/casing
        //return name.toUpperCase();
        return name;
    }

    private final class QueryWorkItem implements Runnable {
        private final ScriptReader reader;
        String sql;
        private String next;

        private QueryWorkItem(String query) {
            this.reader = new ScriptReader(query);
        }

        private void done(Throwable error) {
            if (error != null) {
                errorOccurred(error);
            } else {
                doneExecuting();
            }
            ready();
        }

        @Override
        public void run() {
            try {
                if (sql == null) {
                    sql = reader.readStatement();
                }
                while (sql != null) {
                    try {

                        ResultsFuture<Integer> results = new ResultsFuture<Integer>();
                        results.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
                            public void onCompletion(ResultsFuture<Integer> future) {
                                try {
                                    future.get();
                                    if (next != null) {
                                        sql = next;
                                        next = null;
                                    } else {
                                        sql = reader.readStatement();
                                    }
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                } catch (IOException e) {
                                    done(e);
                                    return;
                                } catch (ExecutionException e) {
                                    Throwable cause = e;
                                    while (cause instanceof ExecutionException && cause.getCause() != null && cause != cause.getCause()) {
                                        cause = cause.getCause();
                                    }
                                    done(cause);
                                    return;
                                }
                                QueryWorkItem.this.run(); //continue processing
                            };
                        });

                        if (isErrorOccurred()) {
                            if (!connection.getAutoCommit()) {
                                connection.rollback(false);
                            }
                            break;
                        }
                        if (!HONOR_DECLARE_FETCH_TXN && sql.equalsIgnoreCase("BEGIN") && connection.getAutoCommit()) { //$NON-NLS-1$
                            next = reader.readStatement();
                            if (next != null && (cursorSelectPattern.matcher(next)).matches()) {
                                sql = next;
                                next = null;
                                LogManager.logDetail(LogConstants.CTX_ODBC, "not honoring the transaction for declare/fetch"); //$NON-NLS-1$
                            }
                        }
                        Matcher m = null;
                        if ((m = cursorSelectPattern.matcher(sql)).matches()){
                            boolean scroll = false;
                            if (m.group(3) != null && m.group(4) == null ) {
                                scroll = true;
                            }
                            cursorExecute(normalizeName(m.group(1)), fixSQL(m.group(5)), results, scroll, m.group(2) != null);
                        }
                        else if ((m = fetchPattern.matcher(sql)).matches()){
                            int rowCount = 1;
                            String direction = m.group(1);
                            CursorDirection cursorDirection = CursorDirection.FORWARD;
                            if (direction != null) {
                                cursorDirection = CursorDirection.valueOf(direction.toUpperCase());
                            }
                            String rows = m.group(2);
                            if (rows != null) {
                                rowCount = Integer.parseInt(rows);
                            }
                            cursorFetch(normalizeName(m.group(3)), cursorDirection, rowCount, results);
                        }
                        else if ((m = fetchFirstLastPattern.matcher(sql)).matches()){
                            int rowCount = 1;
                            String direction = m.group(1);
                            CursorDirection cursorDirection = CursorDirection.valueOf(direction.toUpperCase());
                            cursorFetch(normalizeName(m.group(2)), cursorDirection, rowCount, results);
                        }
                        else if ((m = movePattern.matcher(sql)).matches()){
                            cursorMove(normalizeName(m.group(3)), m.group(1), Integer.parseInt(m.group(2)), results);
                        }
                        else if ((m = closePattern.matcher(sql)).matches()){
                            cursorClose(normalizeName(m.group(1)));
                            results.getResultsReceiver().receiveResults(1);
                        }
                        else if ((m = deallocatePattern.matcher(sql)).matches()) {
                            String plan_name = m.group(1);
                            plan_name = normalizeName(plan_name);
                            closePreparedStatement(plan_name);
                            client.sendCommandComplete("DEALLOCATE"); //$NON-NLS-1$
                            results.getResultsReceiver().receiveResults(1);
                        }
                        else {
                            sqlExecute(sql, results);
                        }
                        return; //wait for the execution to finish
                    } catch (SQLException e) {
                        done(e);
                        return;
                    }
                }
            } catch (NumberFormatException e) {
                //create a sqlexception so that the logic doesn't over log this
                done(TeiidSQLException.create(e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40147, e.getMessage())));
                return;
            } catch(Exception e) {
                done(e);
                return;
            }
            done(null);
        }
    }

    /**
     * @see PgCatalogMetadataStore add_pg_attribute for mod calculation
     */
    private List<PgColInfo> getPgColInfo(ResultSetMetaData meta)
            throws SQLException {
        if (meta == null) {
            return null;
        }
        int columns = meta.getColumnCount();
        final ArrayList<PgColInfo> result = new ArrayList<PgColInfo>(columns);
        for (int i = 1; i <= columns; i++) {
            final PgColInfo info = new PgColInfo();
            info.name = meta.getColumnLabel(i);
            info.type = meta.getColumnType(i);
            String typeName = meta.getColumnTypeName(i);
            info.type = convertType(info.type, typeName);
            info.precision = meta.getColumnDisplaySize(i);
            if (info.type == PG_TYPE_NUMERIC) {
                info.mod = 4+ 65536*Math.min(32767, meta.getPrecision(i))+Math.min(32767, meta.getScale(i));
            } else if (info.type == PG_TYPE_BPCHAR || info.type == PG_TYPE_VARCHAR){
                info.mod = (int) Math.min(Integer.MAX_VALUE, 4+(long)meta.getColumnDisplaySize(i));
            } else {
                info.mod = -1;
            }
            String name = meta.getColumnName(i);
            String table = meta.getTableName(i);
            String schema = meta.getSchemaName(i);
            if (schema != null) {
                final PreparedStatementImpl ps = this.connection.prepareStatement("select " //$NON-NLS-1$
                        + "pg_catalog.getOid(SYS.Columns.TableUID), " //$NON-NLS-1$
                        + "cast(SYS.Columns.Position as short), " //$NON-NLS-1$
                        + "cast((select p.value from SYS.Properties p where p.name = 'pg_type:oid' and p.uid = SYS.Columns.uid) as integer) " //$NON-NLS-1$
                        + "from SYS.Columns where Name = ? and TableName = ? and SchemaName = ?"); //$NON-NLS-1$
                try {
                    ps.setString(1, name);
                    ps.setString(2, table);
                    ps.setString(3, schema);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        info.reloid = rs.getInt(1);
                        info.attnum = rs.getShort(2);
                        int specificType = rs.getInt(3);
                        if (!rs.wasNull()) {
                            info.type = specificType;
                        }
                    }
                } finally {
                    ps.close();
                }
            }
            result.add(info);
        }
        return result;
    }

    /**
     * Represents a PostgreSQL Prepared object.  The actual plan preparation is performed lazily.
     */
    static class Prepared {

        public Prepared (String name, String sql, String modifiedSql, int[] paramType, List<PgColInfo> columnMetadata,
                String cursorName) {
            this.name = name;
            this.sql = sql;
            this.modifiedSql = modifiedSql;
            this.paramType = paramType;
            this.columnMetadata = columnMetadata;
            this.cursorName = cursorName;
        }

        /**
         * The object name.
         */
        final String name;

        /**
         * The original SQL statement.
         */
        final String sql;

        final String modifiedSql;

        /**
         * The list of pg parameter types (if set).
         */
        final int[] paramType;

        /**
         * calculated column metadata
         */
        final List<PgColInfo> columnMetadata;

        /**
         * The cursor name if this is a prepared cursor
         */
        final String cursorName;
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    static class Portal {

        public Portal(String name, Prepared prepared,short[] resultColumnformat, PreparedStatementImpl stmt) {
            this.name = name;
            this.prepared = prepared;
            this.resultColumnFormat = resultColumnformat;
            this.stmt = stmt;
        }
        /**
         * The portal name.
         */
        final String name;

        /**
         * The format used in the result set columns (if set).
         */
        final short[] resultColumnFormat;

        final Prepared prepared;

        volatile ResultSetImpl rs;

        /**
         * The prepared statement.
         */
        final PreparedStatementImpl stmt;
    }

    static class Cursor extends Portal {

        public Cursor (String name, String sql, PreparedStatementImpl stmt, ResultSetImpl rs, List<PgColInfo> colMetadata, short[] resultColumnFormat) {
            super(name, new Prepared(UNNAMED, sql, sql, null, colMetadata, null), resultColumnFormat, stmt);
            this.rs = rs;
        }
    }

}
