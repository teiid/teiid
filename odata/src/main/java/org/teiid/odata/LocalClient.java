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
package org.teiid.odata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.odata4j.core.OCollection;
import org.odata4j.core.OCollections;
import org.odata4j.core.OComplexObject;
import org.odata4j.core.OComplexObjects;
import org.odata4j.core.OEntity;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.core.OSimpleObject;
import org.odata4j.edm.EdmCollectionType;
import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.ServerErrorException;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.Responses;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.VDBRepository;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.net.TeiidURL;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.odata.ODataTypeManager;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
	private MetadataStore metadataStore;
	private String vdbName;
	private int vdbVersion;
	private AtomicInteger tempTableCounter = new AtomicInteger(0);
	private SessionCache sessions;
	private ReferenceQueue<String> refQueue = new ReferenceQueue<String>();
	
	public LocalClient(String vdbName, int vdbVersion) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.sessions = new SessionCache(100);
	}
	
	public TimerTask getTimerTask() {
		return new CleanUpTask();
	}
	
	@Override
	public String getVDBName() {
		return this.vdbName;
	}

	@Override
	public int getVDBVersion() {
		return this.vdbVersion;
	}
	
	private ConnectionImpl getConnection() throws SQLException {
		cleanup();
		return getConnection(this.vdbName, this.vdbVersion, null);
	}
	
	private ConnectionImpl getConnection(String vdbName, int version, String sessionId) throws SQLException {
		TeiidDriver driver = new TeiidDriver();
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:teiid:").append(vdbName).append(".").append(version).append(";");
		if (sessionId != null) {
			sb.append(TeiidURL.CONNECTION.SESSION_ID).append("=").append(sessionId).append(";");
		}
		return driver.connect(sb.toString(), null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}	
	
	private void cleanup() {
		Reference<? extends String> ref;
		while ((ref = this.refQueue.poll()) != null) {
			String sessionId = ref.get();
			closeSession(sessionId);
		}
	}

	private void closeSession(String sessionId) {
		try {
			// by making the connection and closing; it should close any temp tables associated
			// with that session.
			ConnectionImpl conn = getConnection(this.vdbName, this.vdbVersion, sessionId);
			conn.close();
		} catch (SQLException e) {
			// ignore
		}
	}

	@Override
	public BaseResponse sqlExecuteCall(String sql, Map<String, OFunctionParameter> parameters, EdmType returnType) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection();
			final CallableStatementImpl stmt = connection.prepareCall(sql);
			
			int i = 1;
			if (returnType != null && returnType.isSimple()) {
				stmt.registerOutParameter(i++, JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(returnType.getFullyQualifiedTypeName())));
			}
			
			if (!parameters.isEmpty()) {
				for (String key:parameters.keySet()) {
					OFunctionParameter param = parameters.get(key);
					stmt.setObject(i++, ((OSimpleObject)param.getValue()).getValue());
				}
			}
			
			boolean results = stmt.execute();
			if (results) {
				final ResultSet rs = stmt.getResultSet();
                OCollection.Builder resultRows = OCollections.newBuilder(returnType);
                while (rs.next()) {
                	int idx = 1;
                	List<OProperty<?>> row = new ArrayList<OProperty<?>>();
                	Iterator<EdmProperty> props = ((EdmComplexType)((EdmCollectionType)returnType).getItemType()).getProperties().iterator();
                	while (props.hasNext()) {
                		EdmProperty prop = props.next();
                		row.add(OProperties.simple(prop.getName(), rs.getObject(idx++)));
                	}
                	OComplexObject erow = OComplexObjects.create((EdmComplexType)((EdmCollectionType)returnType).getItemType(), row);
                	resultRows.add(erow);
                }
                String collectionName = returnType.getFullyQualifiedTypeName();
                collectionName = collectionName.replace("(", "_");
                collectionName = collectionName.replace(")", "_");
				return Responses.collection(resultRows.build(), null, null, null, collectionName);				
			}
			
            if (returnType != null && returnType.isSimple()) {
            	Object result = stmt.getObject(1);
            	if (result == null) {
            		result = org.odata4j.expression.Expression.null_();
            	}
            	return Responses.simple((EdmSimpleType)returnType, "return", result);
            }			
			return Responses.simple(EdmSimpleType.INT32, 1);
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
	}	

	@Override
	public MetadataStore getMetadataStore() {
		if (this.metadataStore == null) {
			try {
				InitialContext ic = new InitialContext();
				ClientServiceRegistry csr = (ClientServiceRegistry)ic.lookup(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
				VDBRepository repo = csr.getClientService(VDBRepository.class);
				VDBMetaData vdb = repo.getVDB(this.vdbName, this.vdbVersion);
				if (vdb == null) {
					throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16001, this.vdbName, this.vdbVersion));
				}
				this.metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
			} catch (NamingException e) {
				 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
			} catch (ComponentNotFoundException e) {
				throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
			}
		}
		return this.metadataStore;
	}

	@Override
	public List<OEntity> sqlExecute(Query query, List<SQLParam> parameters, EdmEntitySet entitySet, Map<String, Boolean> projectedColumns) {
		return executeSQL(null, true, query, parameters, entitySet, projectedColumns);
	}	
	
	public List<OEntity> executeSQL(final String sessionId, final boolean closeConnection, Query query, List<SQLParam> parameters, EdmEntitySet entitySet, Map<String, Boolean> projectedColumns) {
		try {
			cleanup();
			
			String sql = query.toString();
					
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			
			final ConnectionImpl connection = getConnection(this.vdbName, this.vdbVersion, sessionId);;
			final PreparedStatementImpl stmt = connection.prepareCall(sql);
			if (parameters!= null && !parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			
			ResultsFuture<Boolean> executionFuture = stmt.submitExecute(ResultsMode.RESULTSET, null);
			if (executionFuture.get()) {
				final ResultSet rs = stmt.getResultSet();
				ResultsFuture<Boolean> result = new ResultsFuture<Boolean>();
                result.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
                	public void onCompletion(ResultsFuture<Boolean> future) {
						try {
	                		try {
								stmt.close();
								if (closeConnection) {
									connection.close();
								}
							} catch (SQLException e) {
								LogManager.logDetail(LogConstants.CTX_ODATA, e, "Error closing statement"); //$NON-NLS-1$
							}
							future.get();
						} catch (Throwable e) {
							//ignore
						}
                	}
                });				
				return new EntityList(projectedColumns, entitySet, rs, result);
			}
			return null;
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		}
	}
	
	@Override
	public CountResponse sqlExecuteCount(Query query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareStatement(sql);
			if (!parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			ResultSet rs = stmt.executeQuery();
			rs.next();
			int count = rs.getInt(1);
			rs.close();
			stmt.close();
			return Responses.count(count);
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}	
	
	private void execute(Command command, String sessionId, boolean closeConnection) {
		cleanup();
		ConnectionImpl connection = null;
		try {
			String sql = command.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection(this.vdbName, this.vdbVersion, sessionId);
			final Statement stmt = connection.createStatement();
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				if (closeConnection) {
					connection.close();
				}
			} catch (SQLException e) {
			}
		}
	}
	
	@Override
	public int sqlExecuteUpdate(Command query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareStatement(sql);
			if (!parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			int count = stmt.executeUpdate();
			stmt.close();
			return count;
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}

	@Override
	public EdmDataServices getMetadata() {
		return ODataEntitySchemaBuilder.buildMetadata(getMetadataStore());
	}

	@Override
	public Cursor createCursor(Query query, List<SQLParam> parameters, EdmEntitySet entitySet) {

		String tempTable = "#TEMP"+tempTableCounter.getAndIncrement();
        Insert insert = new Insert();
        GroupSymbol groupSymbol = new GroupSymbol(tempTable);
        insert.setGroup(groupSymbol);
        insert.setQueryExpression(query); 
		
        String sql = insert.toString();
		
		LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
		
		try {
			// this connection deliberately not closed, to keep the cursor results
			final ConnectionImpl connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareCall(sql);
			if (!parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			int count = stmt.executeUpdate();
			stmt.close();
					
			// create a session and place the session id and temptable name
			WeakReference<String> key = new WeakReference<String>(connection.getConnectionId(), this.refQueue);
			this.sessions.put(key, tempTable);
			
			return new LocalCursor(connection.getConnectionId(), tempTable, count, 0);
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		}
	}

	private boolean valid(String session, String table) {
		String tempTable = this.sessions.get(session);
		return(tempTable != null && tempTable.equals(table));
	}
	
	@Override
	public List<OEntity> fetchCursor(Cursor cursor, EdmEntitySet entitySet) {
		if (!valid(cursor.session(), cursor.name())) {
			throw new ServerErrorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16009));
		}
		
		Query query = new Query();
		From from = new From();
		from.addClause(new UnaryFromClause(new GroupSymbol(cursor.name())));
		MultipleElementSymbol all = new MultipleElementSymbol();
		Select select = new Select();
		select.addSymbol(all);
		query.setSelect(select);
		query.setFrom(from);
		query.setLimit(new Limit(new Constant(cursor.offset()), new Constant(cursor.batchSize())));
		
		return executeSQL(cursor.session(), false, query, null, entitySet, null);
	}	
	
	@Override
	public void closeCursor(Cursor cursor) {
		if (!valid(cursor.session(), cursor.name())) {
			throw new ServerErrorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16009));
		}		
		this.sessions.remove(cursor.session());
		Drop drop = new Drop();
        drop.setTable(new GroupSymbol(cursor.name())); //$NON-NLS-1$
        execute(drop, cursor.session(), true);
	}		
	
	static class LocalCursor implements Cursor {
		private String name;
		private int rowCount;
		private int offset;
		private int batch = 10;
		private String session;
		
		public LocalCursor(String sessionid, String name, int rowcount, int offset) {
			this.session = sessionid;
			this.name = name;
			this.rowCount = rowcount;
			this.offset = offset;
		}
		
		@Override
		public String session() {
			return this.session;
		}
		
		@Override
		public String name() {
			return name;
		}

		@Override
		public int rowCount() {
			return this.rowCount;
		}

		@Override
		public int offset() {
			return this.offset;
		}
		
		@Override
		public int batchSize() {
			return this.batch;
		}
		
		public void setBatchSize(int size) {
			this.batch = size;
		}
		
		@Override
		public String nextToken() {
			if ((this.offset + this.batch) < this.rowCount) {
				StringBuilder sb = new StringBuilder();
				sb.append(this.session).append(",").append(this.name).append(",").append(this.rowCount).append(",").append(this.offset+this.batch);
				return sb.toString();
			}
			return null;
		}
		
		public static Cursor parse(String str) {
			StringTokenizer st = new StringTokenizer(str, ",");
			String session = st.nextToken();
			String name = st.nextToken();
			int rowcount = Integer.parseInt(st.nextToken());
			int offset = Integer.parseInt(st.nextToken());
			return new LocalCursor(session, name, rowcount, offset);
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.session).append(",").append(this.name).append(",").append(this.rowCount).append(",").append(this.offset);
			return sb.toString();
		}
	}

	class CleanUpTask extends TimerTask {
		public void run() {
			cleanup();
		}
	}
	
	@SuppressWarnings("serial")
	class SessionCache extends LRUCache<WeakReference<String>, String>{
		public SessionCache(int maxSize) {
			super(maxSize);
		}
		
		@Override
		protected boolean removeEldestEntry(Entry<WeakReference<String>, String> eldest) {
			if (size() > maxSize) {
				closeSession(eldest.getKey().get());
				return true;
			}
			return false;
		}
	};
}
