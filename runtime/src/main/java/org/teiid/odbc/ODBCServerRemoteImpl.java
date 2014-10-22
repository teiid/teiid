/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.odbc;

import static org.teiid.odbc.PGUtil.PG_TYPE_FLOAT4;
import static org.teiid.odbc.PGUtil.PG_TYPE_FLOAT8;
import static org.teiid.odbc.PGUtil.PG_TYPE_NUMERIC;
import static org.teiid.odbc.PGUtil.convertType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ietf.jgss.GSSCredential;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.PgCatalogMetadataStore;
import org.teiid.dqp.service.SessionService;
import org.teiid.jdbc.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.odbc.PGUtil.PgColInfo;
import org.teiid.query.parser.SQLParserUtil;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.*;
import org.teiid.transport.PgFrontendProtocol.NullTerminatedStringDataInputStream;

/**
 * While executing the multiple prepared statements I see this bug currently
 * http://pgfoundry.org/tracker/?func=detail&atid=538&aid=1007690&group_id=1000125
 */
public class ODBCServerRemoteImpl implements ODBCServerRemote {

	private static final String UNNAMED = ""; //$NON-NLS-1$
	private static Pattern setPattern = Pattern.compile("set\\s+(\\w+)\\s+to\\s+((?:'[^']*')+)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE);//$NON-NLS-1$
	
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
			"\\s+end as DELETE_RULE," +							 //$NON-NLS-1$
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
			"\\s+where contype = 'f' " +  //$NON-NLS-1$
			"\\s+and  conrelid = c.oid" +  //$NON-NLS-1$
			"\\s+and  relname = (E?(?:'[^']*')+)" +  //$NON-NLS-1$
			"\\s+and  n.oid = c.relnamespace" +  //$NON-NLS-1$
			"\\s+and  n.nspname = (E?(?:'[^']*')+)" +  //$NON-NLS-1$
			"\\s+\\) ref" +  //$NON-NLS-1$
			"\\s+inner join pg_catalog.pg_class c1" +  //$NON-NLS-1$
			"\\s+on c1.oid = ref.conrelid\\)" +  //$NON-NLS-1$
			"\\s+inner join pg_catalog.pg_namespace n1" +  //$NON-NLS-1$
			"\\s+on  n1.oid = c1.relnamespace\\)" +  //$NON-NLS-1$
			"\\s+inner join pg_catalog.pg_attribute a1" +  //$NON-NLS-1$
			"\\s+on  a1.attrelid = c1.oid" +  //$NON-NLS-1$
			"\\s+and  a1.attnum = conkey\\[i\\]\\)" +  //$NON-NLS-1$
			"\\s+inner join pg_catalog.pg_class c2" +  //$NON-NLS-1$
			"\\s+on  c2.oid = ref.confrelid\\)" +  //$NON-NLS-1$
			"\\s+inner join pg_catalog.pg_namespace n2" +  //$NON-NLS-1$
			"\\s+on  n2.oid = c2.relnamespace\\)" +  //$NON-NLS-1$
			"\\s+inner join pg_catalog.pg_attribute a2" +  //$NON-NLS-1$
			"\\s+on  a2.attrelid = c2.oid" +  //$NON-NLS-1$
			"\\s+and  a2.attnum = confkey\\[i\\]\\)" +  //$NON-NLS-1$
			"\\s+left outer join pg_catalog.pg_constraint cn" +  //$NON-NLS-1$
			"\\s+on cn.conrelid = ref.confrelid" +  //$NON-NLS-1$
			"\\s+and cn.contype = 'p'\\)" +  //$NON-NLS-1$
			"\\s+order by ref.oid, ref.i", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
		
	private static Pattern cursorSelectPattern = Pattern.compile("DECLARE\\s+\"(\\w+)\"(?:\\s+INSENSITIVE)?(\\s+(NO\\s+)?SCROLL)?\\s+CURSOR\\s+(?:WITH(?:OUT)? HOLD\\s+)?FOR\\s+(.*)", Pattern.CASE_INSENSITIVE|Pattern.DOTALL); //$NON-NLS-1$
	private static Pattern fetchPattern = Pattern.compile("FETCH (\\d+) IN \"(\\w+)\".*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern movePattern = Pattern.compile("MOVE (\\d+) IN \"(\\w+)\".*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern closePattern = Pattern.compile("CLOSE \"(\\w+)\"", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	private static Pattern deallocatePattern = Pattern.compile("DEALLOCATE(?:\\s+PREPARE)?\\s+(.*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern releasePattern = Pattern.compile("RELEASE (\\w+\\d?_*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern savepointPattern = Pattern.compile("SAVEPOINT (\\w+\\d?_*)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern rollbackPattern = Pattern.compile("ROLLBACK\\s*(to)*\\s*(\\w+\\d+_*)*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	private TeiidDriver driver;
	private ODBCClientRemote client;
	private Properties props;
	private ConnectionImpl connection;
	private boolean executing;
	private boolean errorOccurred;
	
	private volatile ResultsFuture<Boolean> executionFuture;
	
	// TODO: this is unbounded map; need to define some boundaries as to how many stmts each session can have
	private Map<String, Prepared> preparedMap = Collections.synchronizedMap(new HashMap<String, Prepared>());
	private Map<String, Portal> portalMap = Collections.synchronizedMap(new HashMap<String, Portal>());
	private Map<String, Cursor> cursorMap = Collections.synchronizedMap(new HashMap<String, Cursor>());
	private	LogonImpl logon;
	
	public ODBCServerRemoteImpl(ODBCClientInstance client, TeiidDriver driver, LogonImpl logon) {
		this.driver = driver;
		this.client = client.getClient();
		this.logon = logon;
	}
	
	@Override
	public void initialize(Properties props) {
		this.props = props;		
		this.client.initialized(this.props);

		String user = props.getProperty("user"); //$NON-NLS-1$
		String database = props.getProperty("database");
		
		AuthenticationType authType = null;
		try {
			authType = getAuthenticationType(user, database);
		} catch (LogonException e) {
			errorOccurred(e);
			terminate();
		}
		
		if (authType.equals(AuthenticationType.USERPASSWORD)) {
			this.client.useClearTextAuthentication();
		}
		else if (authType.equals(AuthenticationType.GSS)) {
			this.client.useAuthenticationGSS();
		} else {
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
	public void logon(String databaseName, String user, NullTerminatedStringDataInputStream data, SocketAddress remoteAddress) {
		try {
			java.util.Properties info = new java.util.Properties();
			info.put("user", user); //$NON-NLS-1$
			
			AuthenticationType authType = getAuthenticationType(user, databaseName);
			
			String password = null; 
			if (authType.equals(AuthenticationType.USERPASSWORD)) {
				password = data.readString();
			}
			else if (authType.equals(AuthenticationType.GSS)) {
				byte[] serviceToken = data.readServiceToken();
            	LogonResult result = this.logon.neogitiateGssLogin(this.props, serviceToken, false);
            	serviceToken = (byte[])result.getProperty(ILogon.KRB5TOKEN);
            	if (Boolean.TRUE.equals(result.getProperty(ILogon.KRB5_ESTABLISHED))) {
                	info.put(ILogon.KRB5TOKEN, serviceToken);
                	// if delegation is in progress, participate in it.
                	if (result.getProperty(GSSCredential.class.getName()) != null) {
                		info.put(GSSCredential.class.getName(), result.getProperty(GSSCredential.class.getName()));
                	}
            	}
            	else {
	            	this.client.authenticationGSSContinue(serviceToken);
	            	return;            		
            	}
			} else {
				throw new AssertionError("Unsupported Authentication Type"); //$NON-NLS-1$
			}
			
			// this is local connection
			String url = "jdbc:teiid:"+databaseName+";ApplicationName=ODBC"; //$NON-NLS-1$ //$NON-NLS-2$

			if (password != null) {
				info.put("password", password); //$NON-NLS-1$
			}
			
			if (remoteAddress instanceof InetSocketAddress) {
				SocketServerConnection.updateConnectionProperties(info, ((InetSocketAddress)remoteAddress).getAddress(), false);
			}
			
			this.connection =  driver.connect(url, info);
			//Propagate so that we can use in pg methods
			((LocalServerConnection)this.connection.getServerConnection()).getWorkContext().getSession().addAttchment(ODBCServerRemoteImpl.class, this);
			int hash = this.connection.getConnectionId().hashCode();
			Enumeration<?> keys = this.props.propertyNames();
			while (keys.hasMoreElements()) {
				String key = (String)keys.nextElement();
				this.connection.setExecutionProperty(key, this.props.getProperty(key));
			}
			this.client.authenticationSucess(hash, hash);
			ready();
		} catch (SQLException e) {
			errorOccurred(e);
			terminate();
		} catch(LogonException e) {
			errorOccurred(e);
			terminate();
		} catch (IOException e) {
			errorOccurred(e);
			terminate();			
		}
	}	
	
	private void cursorExecute(String cursorName, final String sql, final ResultsFuture<Integer> completion, boolean scroll) {
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
			Cursor cursor = cursorMap.get(cursorName);
			if (cursor != null) {
				errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40111, cursorName));
				return;
			}
			
			final PreparedStatementImpl stmt = this.connection.prepareStatement(sql, scroll?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            this.executionFuture = stmt.submitExecute(ResultsMode.RESULTSET, null);
            final String name = cursorName;
            this.executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
        		@Override
        		public void onCompletion(ResultsFuture<Boolean> future) {
        			executionFuture = null;
                    try {
                        if (future.get()) {
		                	List<PgColInfo> cols = getPgColInfo(stmt.getResultSet().getMetaData());
	                        cursorMap.put(name, new Cursor(name, sql, stmt, stmt.getResultSet(), cols));
    						client.sendCommandComplete("DECLARE CURSOR", null); //$NON-NLS-1$		                            
							completion.getResultsReceiver().receiveResults(0);
						}
					} catch (Throwable e) {
						completion.getResultsReceiver().exceptionOccurred(e);
					}
        		}
			});					
		} catch (SQLException e) {
			completion.getResultsReceiver().exceptionOccurred(e);
		} 
	}
	
	private void cursorFetch(String cursorName, int rows, final ResultsFuture<Integer> completion) throws SQLException {
		Cursor cursor = this.cursorMap.get(cursorName);
		if (cursor == null) {
			throw new SQLException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40078, cursorName));
		}
		if (rows < 1) {
			throw new SQLException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40112, cursorName, rows));
		}
		this.client.sendResults("FETCH", cursor.rs, cursor.prepared.columnMetadata, completion, rows, true); //$NON-NLS-1$
	}
	
	private void cursorMove(String prepareName, final int rows, final ResultsFuture<Integer> completion) throws SQLException {
		
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
		Runnable r = new Runnable() {
			public void run() {
				run(null, 0);
			}
			public void run(ResultsFuture<Boolean> next, int i) {
				for (; i < rows; i++) {
					try {
						if (next == null) {
							next = cursor.rs.submitNext();
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
			this.client.sendCommandComplete("CLOSE CURSOR", null); //$NON-NLS-1$
		}
	}	
	
    private void sqlExecute(final String sql, final ResultsFuture<Integer> completion) throws SQLException {
    	String modfiedSQL = fixSQL(sql); 
    	final StatementImpl stmt = connection.createStatement();
        executionFuture = stmt.submitExecute(modfiedSQL, null);
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
                        client.sendResults(sql, stmt.getResultSet(), cols, completion, -1, true);
	                } else {
	                	client.sendUpdateCount(sql, stmt.getUpdateCount());
	                	setEncoding();
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
				stmt = this.connection.prepareStatement(modfiedSQL);
				if (paramType.length > 0) {
					ParameterMetaData pmd = stmt.getParameterMetaData();
					for (int i = 0; i < paramType.length; i++) {
						if (paramType[i] == 0) {
							paramType[i] = convertType(pmd.getParameterType(i + 1));
						}
					}
				}
				Prepared prepared = new Prepared(prepareName, sql, modfiedSQL, paramType, getPgColInfo(stmt.getMetaData()));
				this.preparedMap.put(prepareName, prepared);
				this.client.prepareCompleted(prepareName);
			} catch (SQLException e) {
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
	
	@Override
	public void bindParameters(String bindName, String prepareName, Object[] params, int resultCodeCount, int[] resultColumnFormat) {
		// An unnamed portal is destroyed at the end of the transaction, or as soon as 
		// the next Bind statement specifying the unnamed portal as destination is issued. 
		if (bindName == null || bindName.length() == 0) {
			this.portalMap.remove(UNNAMED);
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
			stmt = this.connection.prepareStatement(prepared.modifiedSql);
			for (int i = 0; i < params.length; i++) {
				stmt.setObject(i+1, params[i]);
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
		
		if (query.prepared.sql.trim().isEmpty()) {
			this.client.emptyQueryReceived();
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
            this.executionFuture = stmt.submitExecute(ResultsMode.EITHER, null);
            executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
        		@Override
        		public void onCompletion(ResultsFuture<Boolean> future) {
        			executionFuture = null;
                    try {
		                if (future.get()) {
		                	query.rs = stmt.getResultSet();
		                	sendCursorResults(query, maxRows);
		                } else {
		                	client.sendUpdateCount(query.prepared.sql, stmt.getUpdateCount());
		                	setEncoding();
		                	doneExecuting();
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
		this.client.sendResults(null, cursor.rs, cursor.prepared.columnMetadata, result, fetchSize, false);
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
				return "SELECT PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM, "+//$NON-NLS-1$
							"FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY "+//$NON-NLS-1$
							"FROM SYS.ReferenceKeyColumns WHERE PKTABLE_NAME LIKE "+m.group(14)+" and PKTABLE_SCHEM LIKE "+m.group(15);//$NON-NLS-1$ //$NON-NLS-2$ 
			}
			else if (modified.equalsIgnoreCase("select version()")) { //$NON-NLS-1$
				return "SELECT 'Teiid "+ApplicationInfo.getInstance().getReleaseNumber()+"'"; //$NON-NLS-1$ //$NON-NLS-2$
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
		}
		else if (sql.equalsIgnoreCase("show max_identifier_length")){ //$NON-NLS-1$
			return "select 63"; //$NON-NLS-1$
		}
		else if ((m = setPattern.matcher(sql)).matches()) {
			return "SET " + m.group(1) + " " + m.group(2); //$NON-NLS-1$ //$NON-NLS-2$
		}
		else if (modified.equalsIgnoreCase("BEGIN")) { //$NON-NLS-1$
			return "START TRANSACTION"; //$NON-NLS-1$
		}
		else if ((m = rollbackPattern.matcher(modified)).matches()) {
			return "ROLLBACK"; //$NON-NLS-1$
		}					
		else if ((m = savepointPattern.matcher(sql)).matches()) {
			return "SELECT 0"; //$NON-NLS-1$
		}
		else if ((m = releasePattern.matcher(sql)).matches()) {
			return "SELECT 0"; //$NON-NLS-1$
		} 
		for (int i = 0; i < modified.length(); i++) {
			switch (modified.charAt(i)) {
			case ':':
			case '~':
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
		this.portalMap.remove(UNNAMED);
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
		synchronized (this) {
			this.executing = true;			
		}
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
		this.client.sendResultSetDescription(query.columnMetadata);
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
			this.client.sendResultSetDescription(query.prepared.columnMetadata);
		}
	}

	@Override
	public void sync() {
		ready();
	}
	
	protected synchronized void doneExecuting() {
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
		this.client.ready(inTxn, failedTxn);
	}
	
	@Override
	public void cancel() {
		// TODO Auto-generated method stub
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
		try {
			query.stmt.close();
		} catch (SQLException e) {
			LogManager.logDetail(LogConstants.CTX_ODBC, e, "Did not successfully close portal", query.name); //$NON-NLS-1$
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
		for (Portal p: this.portalMap.values()) {
			closePortal(p);
		}
		this.portalMap.clear();
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
	
	@Override
	public void flush() {
		this.client.flush();
	}
	
	@Override
	public void functionCall(int oid) {
		errorOccurred(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40081));
	}
	
	@Override
	public void sslRequest() {
		this.client.sendSslResponse();
	}
	
	private void setEncoding() {
		String encoding = getEncoding();
		if (encoding != null) {
			//this may be unnecessary
			this.client.setEncoding(encoding, false);
		}
	}
	
	public String getEncoding() {
		return this.connection.getExecutionProperty(PgBackendProtocol.CLIENT_ENCODING);
	}
	
    private final class QueryWorkItem implements Runnable {
		private final ScriptReader reader;
		String sql;

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
		    		                sql = reader.readStatement();
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
		    			
		            	Matcher m = null;
		    	        if ((m = cursorSelectPattern.matcher(sql)).matches()){
		    	        	boolean scroll = false;
		    	        	if (m.group(2) != null && m.group(3) == null ) {
	    	        			scroll = true;
		    	        	}
		    				cursorExecute(m.group(1), fixSQL(m.group(4)), results, scroll);
		    			}
		    			else if ((m = fetchPattern.matcher(sql)).matches()){
		    				cursorFetch(m.group(2), Integer.parseInt(m.group(1)), results);
		    			}
		    			else if ((m = movePattern.matcher(sql)).matches()){
		    				cursorMove(m.group(2), Integer.parseInt(m.group(1)), results);
		    			}
		    			else if ((m = closePattern.matcher(sql)).matches()){
		    				cursorClose(m.group(1));
		    				results.getResultsReceiver().receiveResults(1);
		    			}
		    			else if ((m = deallocatePattern.matcher(sql)).matches()) { 
		    				String plan_name = m.group(1);
		    				plan_name = SQLParserUtil.normalizeId(plan_name);
		    				closePreparedStatement(plan_name);
		    				client.sendCommandComplete("DEALLOCATE", null); //$NON-NLS-1$
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
			} catch(IOException e) {
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
			info.type = convertType(info.type);
			info.precision = meta.getColumnDisplaySize(i);
			if (info.type == PG_TYPE_NUMERIC || info.type == PG_TYPE_FLOAT4 || info.type == PG_TYPE_FLOAT8) {
				info.mod = (int) Math.min(Integer.MAX_VALUE, (4+(65536*(long)meta.getPrecision(i))+meta.getScale(i)));
			} else {
				info.mod = (int) Math.min(Integer.MAX_VALUE, 4+(long)meta.getColumnDisplaySize(i));
			}
			String name = meta.getColumnName(i);
			String table = meta.getTableName(i);
			String schema = meta.getSchemaName(i);
			if (schema != null) {
				final PreparedStatementImpl ps = this.connection.prepareStatement("select attrelid, attnum, typoid from matpg_relatt where attname = ? and relname = ? and nspname = ?"); //$NON-NLS-1$
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
			}
			result.add(info);
		}
		return result;
	}  
    
	/**
     * Represents a PostgreSQL Prepared object.  The actual plan preparation is performed lazily.
     */
    static class Prepared {

    	public Prepared (String name, String sql, String modifiedSql, int[] paramType, List<PgColInfo> columnMetadata) {
    		this.name = name;
    		this.sql = sql;
    		this.modifiedSql = modifiedSql;
    		this.paramType = paramType;
    		this.columnMetadata = columnMetadata;
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
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    static class Portal {

    	public Portal(String name, Prepared prepared, int[] resultColumnformat, PreparedStatementImpl stmt) {
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
        final int[] resultColumnFormat;

        final Prepared prepared;
        
        volatile ResultSetImpl rs;
        
        /**
         * The prepared statement.
         */
        final PreparedStatementImpl stmt;
    }
    
    static class Cursor extends Portal {
    	
    	public Cursor (String name, String sql, PreparedStatementImpl stmt, ResultSetImpl rs, List<PgColInfo> colMetadata) {
    		super(name, new Prepared(UNNAMED, sql, sql, null, colMetadata), null, stmt);
    		this.rs = rs;
    	}
    }    

}
