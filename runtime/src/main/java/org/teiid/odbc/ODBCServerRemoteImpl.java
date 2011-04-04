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

import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.StringUtil;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.StatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ODBCClientInstance;

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
	
	private static Pattern procParametersPattern = Pattern.compile("select proname, proretset, prorettype, pronargs, proargtypes, " + //$NON-NLS-1$
			"nspname, p.oid, atttypid, attname, proargnames, proargmodes, proallargtypes from ((pg_catalog.pg_namespace n inner join " + //$NON-NLS-1$
			"pg_catalog.pg_proc p on p.pronamespace = n.oid) inner join pg_type t on t.oid = p.prorettype) left outer join " + //$NON-NLS-1$
			"pg_attribute a on a.attrelid = t.typrelid  and attnum > 0 and not attisdropped " + //$NON-NLS-1$
			"where has_function_privilege(p.oid, 'EXECUTE') and nspname like (E?(?:'[^']*')+) " + //$NON-NLS-1$
			"and proname like (E?(?:'[^']*')+) " + //$NON-NLS-1$
			"order by nspname, proname, p.oid, attnum"); //$NON-NLS-1$
	
	private static Pattern preparedAutoIncrement = Pattern.compile("select 1 \\s*from pg_catalog.pg_attrdef \\s*where adrelid = \\$1 AND adnum = \\$2 " + //$NON-NLS-1$
			"\\s*and pg_catalog.pg_get_expr\\(adbin, adrelid\\) \\s*like '%nextval\\(%'", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	private static Pattern deallocatePattern = Pattern.compile("DEALLOCATE \"(\\w+\\d+_*)\""); //$NON-NLS-1$
	private static Pattern releasePattern = Pattern.compile("RELEASE (\\w+\\d+_*)"); //$NON-NLS-1$
	private static Pattern savepointPattern = Pattern.compile("SAVEPOINT (\\w+\\d+_*)"); //$NON-NLS-1$
	private static Pattern rollbackPattern = Pattern.compile("ROLLBACK\\s*(to)*\\s*(\\w+\\d+_*)*"); //$NON-NLS-1$
	
	private TeiidDriver driver;
	private ODBCClientInstance clientInstance;
	private ODBCClientRemote client;
	private Properties props;
	private AuthenticationType authType;
	private ConnectionImpl connection;
	
	private volatile ResultsFuture<Boolean> executionFuture;
	
	// TODO: this is unbounded map; need to define some boundaries as to how many stmts each session can have
	private Map<String, Prepared> preparedMap = Collections.synchronizedMap(new HashMap<String, Prepared>());
	private Map<String, Portal> portalMap = Collections.synchronizedMap(new HashMap<String, Portal>());
	
	public ODBCServerRemoteImpl(ODBCClientInstance client, AuthenticationType authType, TeiidDriver driver) {
		this.driver = driver;
		this.client = client.getClient();
		this.clientInstance = client;
		this.authType = authType;
	}
	
	@Override
	public void initialize(Properties props) {
		this.props = props;

		this.client.initialized(this.props);
		
		if (this.authType.equals(AuthenticationType.CLEARTEXT)) {
			this.client.useClearTextAuthentication();
		}
		else if (this.authType.equals(AuthenticationType.MD5)) {
			// TODO: implement MD5 auth type
		}
	}
	
	@Override
	public void logon(String databaseName, String user, String password) {
		try {
			 java.util.Properties info = new java.util.Properties();
			String url = "jdbc:teiid:"+databaseName+";ApplicationName=ODBC"; //$NON-NLS-1$ //$NON-NLS-2$
			info.put("user", user); //$NON-NLS-1$
			info.put("password", password); //$NON-NLS-1$
			this.connection =  (ConnectionImpl)driver.connect(url, info);
			int hash = this.connection.getConnectionId().hashCode();
			this.client.authenticationSucess(hash, hash);
			sync();
		} catch (SQLException e) {
			this.client.errorOccurred(e);
			terminate();
		} 
	}	
	
	@Override
	public void prepare(String prepareName, String sql, int[] paramType) {
		if (this.connection != null) {
			
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
					this.client.errorOccurred(e);
				}
			}
		}
		else {
			this.client.errorOccurred(RuntimePlugin.Util.getString("no_active_connection")); //$NON-NLS-1$
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
			this.client.errorOccurred(RuntimePlugin.Util.getString("bad_binding", prepareName)); //$NON-NLS-1$
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
			this.client.errorOccurred(e);
		}
		
		this.portalMap.put(bindName, new Portal(bindName, prepareName, previous.sql, previous.stmt, resultColumnFormat));
		this.client.bindComplete();
	}

	@Override
	public void unsupportedOperation(String msg) {
		this.client.errorOccurred(msg);
		sync();
	}

	@Override
	public void execute(String bindName, int maxRows) {
		if (isAwaitingAsynch()) {
			return;
		}
		if (bindName == null || bindName.length() == 0) {
			bindName  = UNNAMED;
		}		
		
		final Portal query = this.portalMap.get(bindName);
		if (query == null) {
			this.client.errorOccurred(RuntimePlugin.Util.getString("not_bound", bindName)); //$NON-NLS-1$
			sync();
		}				
		else {
			if (query.sql.trim().isEmpty()) {
				this.client.emptyQueryReceived();
				return;
			}
			
            final PreparedStatementImpl stmt = query.stmt;
            try {
            	// maxRows = 0, means unlimited.
            	if (maxRows != 0) {
            		stmt.setMaxRows(maxRows);
            	}
            	
                this.executionFuture = stmt.submitExecute();
                executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
	        		@Override
	        		public void onCompletion(ResultsFuture<Boolean> future) {
	        			executionFuture = null;
                        try {
		        			if (future.get()) {
	                            client.sendResults(query.sql, stmt.getResultSet(), true);
		                    } else {
		                    	client.sendUpdateCount(query.sql, stmt.getUpdateCount());
		                    	setEncoding();
		                    }
                        } catch (Throwable e) {
                            client.errorOccurred(e);
                        }
                        if (!clientInstance.hasPending()) {
                        	sync();
                        }
	        		}
				});
            } catch (SQLException e) {
            	this.client.errorOccurred(e);
            }			
		}
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
			else if ((m = savepointPattern.matcher(modified)).matches()) {
				return "SELECT 'SAVEPOINT'"; //$NON-NLS-1$
			}
			else if ((m = releasePattern.matcher(modified)).matches()) {
				return "SELECT 'RELEASE'"; //$NON-NLS-1$
			}		
			else if ((m = deallocatePattern.matcher(modified)).matches()) {
				closePreparedStatement(m.group(1));
				return "SELECT 'DEALLOCATE'"; //$NON-NLS-1$
			}					
		}
		modified = sql;
		//these are somewhat dangerous
		modified =  modified.replaceAll("::[A-Za-z0-9]*", " "); //$NON-NLS-1$ //$NON-NLS-2$
		modified =  modified.replaceAll("'pg_toast'", "'SYS'"); //$NON-NLS-1$ //$NON-NLS-2$
		return modified;
	}

	@Override
	public void executeQuery(final String query) {
		if (isAwaitingAsynch()) {
			return;
		}
		//46.2.3 Note that a simple Query message also destroys the unnamed portal.
		this.portalMap.remove(UNNAMED);
		this.preparedMap.remove(UNNAMED);
		
		if (query.trim().length() == 0) {
    		this.client.emptyQueryReceived();
    		sync();
    		return;
    	}
        QueryWorkItem r = new QueryWorkItem(query);
		r.run();
	}

	/**
	 * Just a sanity check.  Should never happen
	 */
	private boolean isAwaitingAsynch() {
		if (this.executionFuture != null) {
			this.client.errorOccurred("Awaiting asynch result"); //$NON-NLS-1$
			sync();
			return true;
		}
		return false;
	}

	@Override
	public void getParameterDescription(String prepareName) {
		if (prepareName == null || prepareName.length() == 0) {
			prepareName  = UNNAMED;
		}		
		Prepared query = this.preparedMap.get(prepareName);
		if (query == null) {
			this.client.errorOccurred(RuntimePlugin.Util.getString("no_stmt_found", prepareName)); //$NON-NLS-1$
			sync();
		}
		else {
			try {
				this.client.sendParameterDescription(query.stmt.getParameterMetaData(), query.paramType);
			} catch (SQLException e) {
				this.client.errorOccurred(e);
			}
		}
	}

	@Override
	public void getResultSetMetaDataDescription(String bindName) {
		if (bindName == null || bindName.length() == 0) {
			bindName  = UNNAMED;
		}		
		Portal query = this.portalMap.get(bindName);
		if (query == null) {
			this.client.errorOccurred(RuntimePlugin.Util.getString("not_bound", bindName)); //$NON-NLS-1$
		}
		else {
			try {
				this.client.sendResultSetDescription(query.stmt.getMetaData(), query.stmt);
			} catch (SQLException e) {
				this.client.errorOccurred(e);
			}
		}
	}

	@Override
	public void sync() {
		if (this.executionFuture != null) {
			return;
		}
		boolean inTxn = false;
		boolean failedTxn = false;
		try {
			if (!this.connection.getAutoCommit()) {
				inTxn = true;
			}
		} catch (SQLException e) {
			failedTxn = true;
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
			this.client.errorOccurred(RuntimePlugin.Util.getString("not_bound", bindName)); //$NON-NLS-1$
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
			this.client.errorOccurred(RuntimePlugin.Util.getString("no_stmt_found", preparedName)); //$NON-NLS-1$
		}
		else {
			// Close all the bound messages off of this prepared
			// TODO: can there be more than one?
			this.portalMap.remove(preparedName);
			
			try {
				query.stmt.close();
				this.client.statementClosed();
			} catch (SQLException e) {
				this.client.errorOccurred(RuntimePlugin.Util.getString("error_closing_stmt", preparedName)); //$NON-NLS-1$
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
		this.client.errorOccurred(RuntimePlugin.Util.getString("lo_not_supported")); //$NON-NLS-1$
		sync();
	}
	
	@Override
	public void sslRequest() {
		this.client.sslDenied();
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
		String modfiedSQL;
		String sql;

		private QueryWorkItem(String query) {
			this.reader = new ScriptReader(new StringReader(query));
		}

		@Override
		public void run() {
			try {
				if (modfiedSQL == null) {
					sql = reader.readStatement();
			        modfiedSQL = fixSQL(sql);
				}
		        while (modfiedSQL != null) {
		            try {
		            	final StatementImpl stmt = connection.createStatement();
		                executionFuture = stmt.submitExecute(modfiedSQL);
		                executionFuture.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
			        		@Override
			        		public void onCompletion(ResultsFuture<Boolean> future) {
			        			executionFuture = null;
			        			try {
					                if (future.get()) {
					                	client.sendResults(sql, stmt.getResultSet(), true);
					                } else {
					                	client.sendUpdateCount(sql, stmt.getUpdateCount());
					                	setEncoding();
					                }
					                sql = reader.readStatement();
					                modfiedSQL = fixSQL(sql);
			        			} catch (Throwable e) {
			        				client.errorOccurred(e);
			        				sync();
			        				return;
			        			} finally {
			        				try {
										stmt.close();
									} catch (SQLException e) {
										LogManager.logDetail(LogConstants.CTX_ODBC, e, "Error closing statement"); //$NON-NLS-1$
									}
			        			}
			        			QueryWorkItem.this.run(); //continue processing
			        		}
						});
		                return; //wait for the execution to finish
		            } catch (SQLException e) {
		                client.errorOccurred(e);
		                break;
		            } 
		        }
			} catch(IOException e) {
				client.errorOccurred(e);
			}
			sync();
		}
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


}
