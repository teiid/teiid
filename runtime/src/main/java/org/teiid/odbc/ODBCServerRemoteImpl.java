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

import static org.teiid.odbc.PGUtil.*;

import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.StringUtil;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.ResultSetImpl;
import org.teiid.jdbc.StatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.TeiidURL.CONNECTION.AuthenticationType;
import org.teiid.odbc.PGUtil.PgColInfo;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ODBCClientInstance;
import org.teiid.transport.PgFrontendProtocol.NullTerminatedStringDataInputStream;

/**
 * While executing the multiple prepared statements I see this bug currently
 * http://pgfoundry.org/tracker/?func=detail&atid=538&aid=1007690&group_id=1000125
 */
public class ODBCServerRemoteImpl implements ODBCServerRemote {

	private static final String UNNAMED = "UNNAMED"; //$NON-NLS-1$
	private static Pattern setPattern = Pattern.compile("(SET|set)\\s+(\\w+)\\s+(TO|to)\\s+'(\\w+\\d*)'");//$NON-NLS-1$
	
	private static Pattern pkPattern = Pattern.compile("select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname " +//$NON-NLS-1$
			"from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, " +//$NON-NLS-1$
			"pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = (E?(?:'[^']*')+) AND n.nspname = (E?(?:'[^']*')+).*" );//$NON-NLS-1$
	
	private static Pattern pkKeyPattern = Pattern.compile("select ta.attname, ia.attnum, ic.relname, n.nspname, NULL from " + //$NON-NLS-1$
			"pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class ic, pg_catalog.pg_index i, " + //$NON-NLS-1$
			"pg_catalog.pg_namespace n where ic.relname = (E?(?:'[^']*')+) AND n.nspname = (E?(?:'[^']*')+) .*"); //$NON-NLS-1$
	
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
			"\\s+order by ref.oid, ref.i"); //$NON-NLS-1$
		
	private static Pattern preparedAutoIncrement = Pattern.compile("select 1 \\s*from pg_catalog.pg_attrdef \\s*where adrelid = \\$1 AND adnum = \\$2 " + //$NON-NLS-1$
			"\\s*and pg_catalog.pg_get_expr\\(adbin, adrelid\\) \\s*like '%nextval\\(%'", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	private static Pattern cursorSelectPattern = Pattern.compile("DECLARE \"(\\w+)\" CURSOR(\\s(WITH HOLD|SCROLL))? FOR (.*)", Pattern.CASE_INSENSITIVE|Pattern.DOTALL); //$NON-NLS-1$
	private static Pattern fetchPattern = Pattern.compile("FETCH (\\d+) IN \"(\\w+)\".*", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern movePattern = Pattern.compile("MOVE (\\d+) IN \"(\\w+)\".*", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern closePattern = Pattern.compile("CLOSE \"(\\w+)\"", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	private static Pattern deallocatePattern = Pattern.compile("DEALLOCATE \"(\\w+\\d+_*)\"", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern releasePattern = Pattern.compile("RELEASE (\\w+\\d?_*)", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern savepointPattern = Pattern.compile("SAVEPOINT (\\w+\\d?_*)", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	private static Pattern rollbackPattern = Pattern.compile("ROLLBACK\\s*(to)*\\s*(\\w+\\d+_*)*", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	
	private TeiidDriver driver;
	private ODBCClientRemote client;
	private Properties props;
	private AuthenticationType authType;
	private ConnectionImpl connection;
	private boolean executing;
	private boolean errorOccurred;
	
	private volatile ResultsFuture<Boolean> executionFuture;
	
	// TODO: this is unbounded map; need to define some boundaries as to how many stmts each session can have
	private Map<String, Prepared> preparedMap = Collections.synchronizedMap(new HashMap<String, Prepared>());
	private Map<String, Portal> portalMap = Collections.synchronizedMap(new HashMap<String, Portal>());
	private Map<String, Cursor> cursorMap = Collections.synchronizedMap(new HashMap<String, Cursor>());
	private ILogon logon;
	
	public ODBCServerRemoteImpl(ODBCClientInstance client, AuthenticationType authType, TeiidDriver driver, ILogon logon) {
		this.driver = driver;
		this.client = client.getClient();
		this.authType = authType;
		this.logon = logon;
	}
	
	@Override
	public void initialize(Properties props) {
		this.props = props;

		this.client.initialized(this.props);
		
		if (this.authType.equals(AuthenticationType.CLEARTEXT)) {
			this.client.useClearTextAuthentication();
		}
		else if (this.authType.equals(AuthenticationType.KRB5)) {
			this.client.useAuthenticationGSS();
		}
	}
	
	@Override
	public void logon(String databaseName, String user, NullTerminatedStringDataInputStream data) {
		try {
			java.util.Properties info = new java.util.Properties();
			info.put("user", user); //$NON-NLS-1$
			
			String password = null; 
			String passthroughAuthentication = ""; //$NON-NLS-1$
			if (authType.equals(AuthenticationType.CLEARTEXT)) {
				password = data.readString();
			}
			else if (authType.equals(AuthenticationType.KRB5)) {
				byte[] serviceToken = data.readServiceToken();
            	LogonResult result = this.logon.neogitiateGssLogin(this.props, serviceToken, false);
            	if (!Boolean.TRUE.equals(result.getProperty(ILogon.KRB5_ESTABLISHED))) {
	            	serviceToken = (byte[])result.getProperty(ILogon.KRB5TOKEN);
	            	this.client.authenticationGSSContinue(serviceToken);
	            	return;
            	}
            	passthroughAuthentication = ";PassthroughAuthentication=true"; //$NON-NLS-1$
			}
			
			String url = "jdbc:teiid:"+databaseName+";ApplicationName=ODBC"+passthroughAuthentication; //$NON-NLS-1$ //$NON-NLS-2$

			if (password != null) {
				info.put("password", password); //$NON-NLS-1$
			}
			
			this.connection =  (ConnectionImpl)driver.connect(url, info);
			int hash = this.connection.getConnectionId().hashCode();
			Enumeration keys = this.props.propertyNames();
			while (keys.hasMoreElements()) {
				String key = (String)keys.nextElement();
				Statement stmt = this.connection.createStatement();
				stmt.execute("SET " + key + " '" + this.props.getProperty(key) + "'"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				stmt.close();
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
	
	private void cursorExecute(final String cursorName, final String sql, final ResultsFuture<Integer> completion) {
		if (sql != null) {
			try {
				// close if the name is already used or the unnamed prepare; otherwise
				// stmt is alive until session ends.
				Prepared previous = this.preparedMap.remove(cursorName);
				if (previous != null) {
					previous.stmt.close();
				}
				
				final PreparedStatementImpl stmt = this.connection.prepareStatement(sql);
                this.executionFuture = stmt.submitExecute(ResultsMode.RESULTSET);
                this.executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
	        		@Override
	        		public void onCompletion(ResultsFuture<Boolean> future) {
	        			executionFuture = null;
                        try {
		                	List<PgColInfo> cols = getPgColInfo(stmt.getResultSet().getMetaData());
                            cursorMap.put(cursorName, new Cursor(cursorName, sql, stmt, null, stmt.getResultSet(), cols));
        					client.sendCommandComplete("DECLARE CURSOR", 0); //$NON-NLS-1$		                            
    						completion.getResultsReceiver().receiveResults(0);
    					} catch (Throwable e) {
    						completion.getResultsReceiver().exceptionOccurred(e);
    					}
	        		}
				});					
			} catch (SQLException e) {
				completion.getResultsReceiver().exceptionOccurred(e);
			} 
		}
	}
	
	private void cursorFetch(String cursorName, int rows, final ResultsFuture<Integer> completion) throws SQLException {
		Cursor cursor = this.cursorMap.get(cursorName);
		if (cursor == null) {
			throw new SQLException(RuntimePlugin.Util.getString("not_bound", cursorName)); //$NON-NLS-1$
		}
		cursor.fetchSize = rows;
		ResultsFuture<Integer> result = new ResultsFuture<Integer>();
		this.client.sendCursorResults(cursor.rs, cursor.columnMetadata, result, rows);
		result.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
        	public void onCompletion(ResultsFuture<Integer> future) {
        		try {
					int rowsSent = future.get();
					client.sendCommandComplete("FETCH", rowsSent); //$NON-NLS-1$						
					completion.getResultsReceiver().receiveResults(rowsSent);
				} catch (Throwable e) {
					completion.getResultsReceiver().exceptionOccurred(e);
				}
        	};
		});
	}
	
	private void cursorMove(String prepareName, int rows, final ResultsFuture<Integer> completion) throws SQLException {
		
		// win odbc driver sending a move after close; and error is ending up in failure; since the below
		// is not harmful it is ok to send empty move.
		if (rows == 0) {
			client.sendCommandComplete("MOVE", 0); //$NON-NLS-1$
			completion.getResultsReceiver().receiveResults(0);
			return;			
		}
		
		Cursor cursor = this.cursorMap.get(prepareName);
		if (cursor == null) {
			throw new SQLException(RuntimePlugin.Util.getString("not_bound", prepareName)); //$NON-NLS-1$
		}
		ResultsFuture<Integer> result = new ResultsFuture<Integer>();
		this.client.sendMoveCursor(cursor.rs, rows, result);
		result.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
        	public void onCompletion(ResultsFuture<Integer> future) {
        		try {
					int rowsMoved = future.get();
					client.sendCommandComplete("MOVE", rowsMoved); //$NON-NLS-1$						
					completion.getResultsReceiver().receiveResults(rowsMoved);
				} catch (Throwable e) {
					completion.getResultsReceiver().exceptionOccurred(e);
				}
        	};
		});			
	}	
	
	private void cursorClose(String prepareName) throws SQLException {
		Cursor cursor = this.cursorMap.remove(prepareName);
		if (cursor != null) {
			cursor.rs.close();
			cursor.stmt.close();
			this.client.sendCommandComplete("CLOSE CURSOR", 0); //$NON-NLS-1$
		}
	}	
	
    private void sqlExecute(final String sql, final ResultsFuture<Integer> completion) throws SQLException {
    	String modfiedSQL = fixSQL(sql); 
    	final StatementImpl stmt = connection.createStatement();
        executionFuture = stmt.submitExecute(modfiedSQL);
        executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
    		@Override
    		public void onCompletion(ResultsFuture<Boolean> future) {
    			executionFuture = null;
    			try {
    				ResultsFuture<Integer> result = new ResultsFuture<Integer>();
	                if (future.get()) {
                		List<PgColInfo> cols = getPgColInfo(stmt.getResultSet().getMetaData());
                        client.sendResults(sql, stmt.getResultSet(), cols, result, true);
	                } else {
	                	client.sendUpdateCount(sql, stmt.getUpdateCount());
	                	setEncoding();
	                	result.getResultsReceiver().receiveResults(1);
	                }
	                result.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
	                	public void onCompletion(ResultsFuture<Integer> future) {
							try {
		                		try {
									stmt.close();
								} catch (SQLException e) {
									LogManager.logDetail(LogConstants.CTX_ODBC, e, "Error closing statement"); //$NON-NLS-1$
								}
								future.get();
								completion.getResultsReceiver().receiveResults(1);
							} catch (Throwable e) {
								completion.getResultsReceiver().exceptionOccurred(e);
							}
	                	}
	                });
    			} catch (Throwable e) {
    				completion.getResultsReceiver().exceptionOccurred(e);
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
			String modfiedSQL = fixSQL(sql);
			try {
				// close if the name is already used or the unnamed prepare; otherwise
				// stmt is alive until session ends.
				Prepared previous = this.preparedMap.remove(prepareName);
				if (previous != null) {
					previous.stmt.close();
				}
				
				PreparedStatementImpl stmt = this.connection.prepareStatement(modfiedSQL);
				this.preparedMap.put(prepareName, new Prepared(prepareName, sql, stmt, paramType));
				this.client.prepareCompleted(prepareName);
			} catch (SQLException e) {
				errorOccurred(e);
			}
		}
	}	
	
	@Override
	public void bindParameters(String bindName, String prepareName, int paramCount, Object[] params, int resultCodeCount, int[] resultColumnFormat) {
		// An unnamed portal is destroyed at the end of the transaction, or as soon as 
		// the next Bind statement specifying the unnamed portal as destination is issued. 
		this.portalMap.remove(UNNAMED);
		
		if (prepareName == null || prepareName.length() == 0) {
			prepareName  = UNNAMED;
		}
		
		Prepared previous = this.preparedMap.get(prepareName);
		if (previous == null) {
			errorOccurred(RuntimePlugin.Util.getString("bad_binding", prepareName)); //$NON-NLS-1$
			return;
		}		
		
		if (bindName == null || bindName.length() == 0) {
			bindName  = UNNAMED;
		}	
		
		try {
			for (int i = 0; i < paramCount; i++) {
				previous.stmt.setObject(i+1, params[i]);
			}
		} catch (SQLException e) {
			errorOccurred(e);
		}
		
		this.portalMap.put(bindName, new Portal(bindName, prepareName, previous.sql, previous.stmt, resultColumnFormat));
		this.client.bindComplete();
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
		
		// special case cursor execution through portal
		final Cursor cursor = this.cursorMap.get(bindName);
		if (cursor != null) {
			sendCursorResults(cursor);
			return;
		}		
		
		final Portal query = this.portalMap.get(bindName);
		if (query == null) {
			errorOccurred(RuntimePlugin.Util.getString("not_bound", bindName)); //$NON-NLS-1$
			return;
		}	
		
		if (query.sql.trim().isEmpty()) {
			this.client.emptyQueryReceived();
			return;
		}
		
        sendPortalResults(maxRows, query);			
	}

	private void sendPortalResults(int maxRows, final Portal query) {
		final PreparedStatementImpl stmt = query.stmt;
        try {
        	// maxRows = 0, means unlimited.
        	if (maxRows != 0) {
        		stmt.setMaxRows(maxRows);
        	}
        	
            this.executionFuture = stmt.submitExecute(ResultsMode.EITHER);
            executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
        		@Override
        		public void onCompletion(ResultsFuture<Boolean> future) {
        			executionFuture = null;
                    try {
                    	ResultsFuture<Integer> result = new ResultsFuture<Integer>();
		                if (future.get()) {
		                	List<PgColInfo> cols = getPgColInfo(stmt.getResultSet().getMetaData());
                            client.sendResults(query.sql, stmt.getResultSet(), cols, result, true);
		                } else {
		                	client.sendUpdateCount(query.sql, stmt.getUpdateCount());
		                	setEncoding();
		                	result.getResultsReceiver().receiveResults(1);
		                }
	        			result.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
                        	public void onCompletion(ResultsFuture<Integer> future) {
                        		try {
									future.get();
                            		doneExecuting();
								} catch (InterruptedException e) {
									throw new AssertionError(e);
								} catch (ExecutionException e) {
									errorOccurred(e.getCause());
								}
                        	};
						});
                    } catch (Throwable e) {
                        errorOccurred(e);
                    }
        		}
			});
        } catch (SQLException e) {
        	errorOccurred(e);
        }
	}

	private void sendCursorResults(final Cursor cursor) {
		ResultsFuture<Integer> result = new ResultsFuture<Integer>();
		this.client.sendPortalResults(cursor.sql, cursor.rs, cursor.columnMetadata, result, cursor.fetchSize, true);
		result.addCompletionListener(new ResultsFuture.CompletionListener<Integer>() {
			public void onCompletion(ResultsFuture<Integer> future) {
				try {
					int rowsSent = future.get();
					if (rowsSent < cursor.fetchSize) {
						client.sendCommandComplete(cursor.sql, 0);
					}
					else {
						client.sendPortalSuspended();
					}
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
		// select current_schema()
		// set client_encoding to 'WIN1252'
		if (sql == null) {
			return null;
		}
		// selects are coming with "select\t" so using a space after "select" does not always work
		if (StringUtil.startsWithIgnoreCase(sql, "select")) { //$NON-NLS-1$
			modified = sql.replace('\n', ' ');
										
			Matcher m = null;
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
			else if (preparedAutoIncrement.matcher(modified).matches()) {
				return "SELECT 1 from matpg_relatt where attrelid = ? and attnum = ? and autoinc = true"; //$NON-NLS-1$
			}
			else {
				// since teiid can work with multiple schemas at a given time
				// this call resolution is ambiguous
				if (sql.equalsIgnoreCase("select current_schema()")) { //$NON-NLS-1$
					return "SELECT ''";  //$NON-NLS-1$
				}							
			}
			
		}
		else if (sql.equalsIgnoreCase("show max_identifier_length")){ //$NON-NLS-1$
			return "select 63"; //$NON-NLS-1$
		}
		else {
			Matcher m = setPattern.matcher(sql);
			if (m.matches()) {
				return "SET " + m.group(2) + " " + m.group(4); //$NON-NLS-1$ //$NON-NLS-2$
			}
			else if (modified.equalsIgnoreCase("BEGIN")) { //$NON-NLS-1$
				return "START TRANSACTION"; //$NON-NLS-1$
			}
			else if ((m = rollbackPattern.matcher(modified)).matches()) {
				return "ROLLBACK"; //$NON-NLS-1$
			}					
		}
		modified = sql;
		//these are somewhat dangerous
		modified =  modified.replaceAll("::[A-Za-z0-9]*", " "); //$NON-NLS-1$ //$NON-NLS-2$
		modified =  modified.replaceAll("'pg_toast'", "'SYS'"); //$NON-NLS-1$ //$NON-NLS-2$
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
			errorOccurred(RuntimePlugin.Util.getString("no_stmt_found", prepareName)); //$NON-NLS-1$
		}
		else {
			try {
				// The response is a ParameterDescription message describing the parameters needed by the statement,
				this.client.sendParameterDescription(query.stmt.getParameterMetaData(), query.paramType);
				
				// followed by a RowDescription message describing the rows that will be returned when the statement  
				// is eventually executed (or a NoData message if the statement will not return rows).
				List<PgColInfo> cols = getPgColInfo(query.stmt.getMetaData());
				this.client.sendResultSetDescription(cols);
			} catch (SQLException e) {
				errorOccurred(e);
			}
		}
	}
	
	private void errorOccurred(String error) {
		this.client.errorOccurred(error);
		synchronized (this) {
			this.errorOccurred = true;
			doneExecuting();
		}
	}
	
	private void errorOccurred(Throwable error) {
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
			errorOccurred(RuntimePlugin.Util.getString("not_bound", bindName)); //$NON-NLS-1$
		}
		else {
			try {
				List<PgColInfo> cols = getPgColInfo(query.stmt.getMetaData());
				this.client.sendResultSetDescription(cols);
			} catch (SQLException e) {
				errorOccurred(e);
			}
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
		if (query == null) {
			errorOccurred(RuntimePlugin.Util.getString("not_bound", bindName)); //$NON-NLS-1$
		}
		else {
			try {
				if (this.connection.getAutoCommit()) {
					// After checking the pg's client code I do not see it send a 
					// close of the Prepare stmt as per the wire protocol, it only sends
					// bound close. Since it also have issue with 
					// http://pgfoundry.org/tracker/?func=detail&atid=538&aid=1007690&group_id=1000125
					// treating the prepare and bound as same for now.
					closePreparedStatement(bindName);
				}
			} catch (SQLException e) {
				closePreparedStatement(bindName);
			}
		}
	}

	@Override
	public void closePreparedStatement(String preparedName) {
		if (preparedName == null || preparedName.length() == 0) {
			preparedName  = UNNAMED;
		}		
		Prepared query = this.preparedMap.remove(preparedName);
		if (query == null) {
			errorOccurred(RuntimePlugin.Util.getString("no_stmt_found", preparedName)); //$NON-NLS-1$
		}
		else {
			// Close all the bound messages off of this prepared
			// TODO: can there be more than one?
			this.portalMap.remove(preparedName);
			
			try {
				query.stmt.close();
				this.client.statementClosed();
			} catch (SQLException e) {
				errorOccurred(RuntimePlugin.Util.getString("error_closing_stmt", preparedName)); //$NON-NLS-1$
			}			
		}	
	}

	@Override
	public void terminate() {
		
		for (Portal p: this.portalMap.values()) {
			try {
				p.stmt.close();
			} catch (SQLException e) {
				//ignore
			}
		}
		
		for (Prepared p:this.preparedMap.values()) {
			try {
				p.stmt.close();
			} catch (SQLException e) {
				//ignore
			}
		}
			
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
		errorOccurred(RuntimePlugin.Util.getString("lo_not_supported")); //$NON-NLS-1$
	}
	
	@Override
	public void sslRequest() {
		this.client.sendSslResponse();
	}
	
	private void setEncoding() {
		try {
			StatementImpl t = connection.createStatement();
			ResultSet rs = t.executeQuery("show client_encoding"); //$NON-NLS-1$
			if (rs.next()) {
				String encoding = rs.getString(1);
				if (encoding != null) {
					//this may be unnecessary
					this.client.setEncoding(encoding);
				}
			}
		} catch (Exception e) {
			//don't care
		}
	}
	
    private final class QueryWorkItem implements Runnable {
		private final ScriptReader reader;
		String sql;

		private QueryWorkItem(String query) {
			this.reader = new ScriptReader(new StringReader(query));		
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
		    				cursorExecute(m.group(1), fixSQL(m.group(4)), results);
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
		    			else if ((m = savepointPattern.matcher(sql)).matches()) {
		    				client.sendCommandComplete("SAVEPOINT", 0); //$NON-NLS-1$
		    				results.getResultsReceiver().receiveResults(1);
		    			}
		    			else if ((m = releasePattern.matcher(sql)).matches()) {
		    				client.sendCommandComplete("RELEASE", 0); //$NON-NLS-1$
		    				results.getResultsReceiver().receiveResults(1);
		    			}		
		    			else if ((m = deallocatePattern.matcher(sql)).matches()) { 
		    				closePreparedStatement(m.group(1));
		    				client.sendCommandComplete("DEALLOCATE", 0); //$NON-NLS-1$
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
    
	private List<PgColInfo> getPgColInfo(ResultSetMetaData meta)
			throws SQLException {
		if (meta == null) {
			return null;
		}
		int columns = meta.getColumnCount();
		final ArrayList<PgColInfo> result = new ArrayList<PgColInfo>(columns);
		for (int i = 1; i < columns + 1; i++) {
			final PgColInfo info = new PgColInfo();
			info.name = meta.getColumnLabel(i).toLowerCase();
			info.type = meta.getColumnType(i);
			info.type = convertType(info.type);
			info.precision = meta.getColumnDisplaySize(i);
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
     * Represents a PostgreSQL Prepared object.
     */
    static class Prepared {

    	public Prepared (String name, String sql, PreparedStatementImpl stmt, int[] paramType) {
    		this.name = name;
    		this.sql = sql;
    		this.stmt = stmt;
    		this.paramType = paramType;
    	}
    	
        /**
         * The object name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The prepared statement.
         */
        PreparedStatementImpl stmt;

        /**
         * The list of parameter types (if set).
         */
        int[] paramType;        
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    static class Portal {

    	public Portal(String name, String preparedName, String sql, PreparedStatementImpl stmt, int[] resultColumnformat) {
    		this.name = name;
    		this.preparedName = preparedName;
    		this.sql = sql;
    		this.stmt = stmt;
    		this.resultColumnFormat = resultColumnformat;
    	}
        /**
         * The portal name.
         */
        String name;

        
        String preparedName;
        
        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The format used in the result set columns (if set).
         */
        int[] resultColumnFormat;

        /**
         * The prepared statement.
         */
        PreparedStatementImpl stmt;        
    }
    
    static class Cursor extends Prepared {
    	ResultSetImpl rs;
    	int fetchSize = 1000;
        /**
         * calculated column metadata
         */
        List<PgColInfo> columnMetadata;	
    	
    	public Cursor (String name, String sql, PreparedStatementImpl stmt, int[] paramType, ResultSetImpl rs, List<PgColInfo> colMetadata) {
    		super(name, sql, stmt, paramType);
    		this.rs = rs;
    		this.columnMetadata = colMetadata;
    	}
    }    

}
