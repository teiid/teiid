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

import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.odata4j.core.*;
import org.odata4j.core.OCollection.Builder;
import org.odata4j.edm.*;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.ServerErrorException;
import org.odata4j.producer.*;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.*;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.net.TeiidURL;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.odata.ODataEntitySchemaBuilder;
import org.teiid.translator.odata.ODataTypeManager;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
	private static final String BATCH_SIZE = "batch-size"; //$NON-NLS-1$
	private static final String SKIPTOKEN_TIME = "skiptoken-cache-time"; //$NON-NLS-1$
	static final String INVALID_CHARACTER_REPLACEMENT = "invalid-xml10-character-replacement"; //$NON-NLS-1$

	private volatile VDBMetaData vdb;
	private String vdbName;
	private int vdbVersion;
	private int batchSize;
	private long cacheTime;
	private String transportName;
	private String connectionString;
	private Properties connectionProperties = new Properties();
	private EdmDataServices edmMetaData;
	private TeiidDriver driver = TeiidDriver.getInstance();
	private String invalidCharacterReplacement;

	public LocalClient(String vdbName, int vdbVersion, Properties props) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.batchSize = PropertiesUtils.getIntProperty(props, BATCH_SIZE, BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE);
		this.cacheTime = PropertiesUtils.getLongProperty(props, SKIPTOKEN_TIME, 300000L);
		this.transportName = props.getProperty(EmbeddedProfile.TRANSPORT_NAME, "odata"); //$NON-NLS-1$
		this.invalidCharacterReplacement = props.getProperty(INVALID_CHARACTER_REPLACEMENT);
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:teiid:").append(this.vdbName).append(".").append(this.vdbVersion).append(";"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		connectionProperties.put(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "true"); //$NON-NLS-1$
		connectionProperties.put(EmbeddedProfile.TRANSPORT_NAME, transportName); 
		connectionProperties.put(EmbeddedProfile.WAIT_FOR_LOAD, "0"); //$NON-NLS-1$
		this.connectionString = sb.toString();
	}

	@Override
	public VDBMetaData getVDB() {
        ConnectionImpl connection = null;
        if (this.vdb == null) {
            try {
                connection = getConnection();
                LocalServerConnection lsc = (LocalServerConnection)connection.getServerConnection();
                VDBMetaData vdb = lsc.getWorkContext().getVDB();
                if (vdb == null) {
                    throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16001, this.vdbName, this.vdbVersion));
                }
                this.vdb = vdb;
            } catch (SQLException e) {
                throw new ServerErrorException(e.getMessage(),e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        
                    }
                }
            }
        }
        return this.vdb;
	}
	
	public void setDriver(TeiidDriver driver) {
		this.driver = driver;
	}

	ConnectionImpl getConnection() throws SQLException {
		return driver.connect(this.connectionString, connectionProperties);
	}

	@Override
	public BaseResponse executeCall(String sql, List<SQLParam> parameters, EdmType returnType) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			connection = getConnection();
			final CallableStatementImpl stmt = connection.prepareCall(sql);

			int i = 1;
			if (returnType != null && returnType.isSimple()) {
				stmt.registerOutParameter(i++, JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(returnType.getFullyQualifiedTypeName())));
			}

			if (!parameters.isEmpty()) {
				for (SQLParam param:parameters) {
					stmt.setObject(i++, param.value, param.sqlType);
				}
			}

			boolean results = stmt.execute();
			if (results) {
				final ResultSet rs = stmt.getResultSet();
                OCollection.Builder resultRows = OCollections.newBuilder((EdmComplexType)((EdmCollectionType)returnType).getItemType());
                while (rs.next()) {
                	int idx = 1;
                	List<OProperty<?>> row = new ArrayList<OProperty<?>>();
                	Iterator<EdmProperty> props = ((EdmComplexType)((EdmCollectionType)returnType).getItemType()).getProperties().iterator();
                	while (props.hasNext()) {
                		EdmProperty prop = props.next();
                		row.add(buildPropery(prop.getName(), prop.getType(), rs.getObject(idx++), invalidCharacterReplacement));
                	}
                	OComplexObject erow = OComplexObjects.create((EdmComplexType)((EdmCollectionType)returnType).getItemType(), row);
                	resultRows.add(erow);
                }
                String collectionName = returnType.getFullyQualifiedTypeName();
                collectionName = collectionName.replace("(", "_"); //$NON-NLS-1$ //$NON-NLS-2$
                collectionName = collectionName.replace(")", "_"); //$NON-NLS-1$ //$NON-NLS-2$
				return Responses.collection(resultRows.build(), null, null, null, collectionName);
			}

            if (returnType != null) {
            	Object result = stmt.getObject(1);
            	OProperty prop = buildPropery("return", returnType, result, invalidCharacterReplacement); //$NON-NLS-1$
            	return Responses.property(prop); 
            }
			return Responses.simple(EdmSimpleType.INT32, 1);
		} catch (Exception e) {
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
		return getVDB().getAttachment(TransformationMetadata.class).getMetadataStore();
	}

	@Override
	public EntityList executeSQL(Query query, List<SQLParam> parameters, EdmEntitySet entitySet, LinkedHashMap<String, Boolean> projectedColumns, QueryInfo queryInfo) {
		Connection connection = null;
		try {
			boolean cache = queryInfo != null && this.batchSize > 0; 
			if (cache) {
				CacheHint hint = new CacheHint();
				hint.setTtl(this.cacheTime);
				hint.setScope(CacheDirective.Scope.USER);
				query.setCacheHint(hint);
			}
			
			boolean getCount = false; 
			if (queryInfo != null) {
				getCount = queryInfo.inlineCount == InlineCount.ALLPAGES;
				if (!getCount && (queryInfo.top != null || queryInfo.skip != null)) {
					if (queryInfo.top != null && queryInfo.skip != null) {
						query.setLimit(new Limit(new Constant(queryInfo.skip), new Constant(queryInfo.top)));
					}
					else if (queryInfo.top != null) {
						query.setLimit(new Limit(new Constant(0), new Constant(queryInfo.top)));
					}
				}
			}

			String sql = query.toString();

			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$

			connection = getConnection();
			final PreparedStatement stmt = connection.prepareStatement(sql, cache?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if (parameters!= null && !parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}

			final ResultSet rs = stmt.executeQuery();
			
			if (projectedColumns == null) {
				projectedColumns = new LinkedHashMap<String, Boolean>();
				for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
					projectedColumns.put(rs.getMetaData().getColumnLabel(i+1), Boolean.TRUE);
				}
			}
			
			EntityList result = new EntityList(invalidCharacterReplacement);
			
			HashMap<String, EdmProperty> propertyTypes = new HashMap<String, EdmProperty>();
			
			EdmEntityType entityType = entitySet.getType();
			Iterator<EdmProperty> propIter = entityType.getProperties().iterator();
			while(propIter.hasNext()) {
				EdmProperty prop = propIter.next();
				propertyTypes.put(prop.getName(), prop);
			}
			
			//skip to the initial position
			int count = 0;
			int skipSize = 0;
			//skip based upon the skip value
			if (getCount && queryInfo.skip != null) {
				skipSize = queryInfo.skip;
			}
			//skip based upon the skipToken
			if (queryInfo != null && queryInfo.skipToken != null) {
				skipSize += Integer.parseInt(queryInfo.skipToken);
			}
			if (skipSize > 0) {
				count += skip(cache, rs, skipSize);
			}

			//determine the number of records to return
			int size = batchSize;
			int top = Integer.MAX_VALUE;
			if (getCount && queryInfo.top != null) {
				top = queryInfo.top;
				size = top; 
				if (batchSize > 0) {
					size = Math.min(batchSize, size);
				}
			} else if (size < 1) {
				size = Integer.MAX_VALUE;
			}
			
			//build the results
			for (int i = 0; i < size; i++) {
				count++;
				if (!rs.next()) {
					break;
				}
				result.addEntity(rs, propertyTypes, projectedColumns, entitySet);
			}
			
			//set the count
			if (getCount) {
				if (!cache) {
					while (rs.next()) {
						count++;
					}
				} else {
					rs.last();
					count = rs.getRow();
				}
			}
			result.setCount(count);
			
			//set the skipToken if needed
			if (cache && result.size() == this.batchSize) {
				int end = skipSize + result.size();
				if (getCount) {
					if (end < Math.min(top, count)) {
						result.setSkipToken(String.valueOf(end));
					}
				} else if (rs.next()) {
					result.setSkipToken(String.valueOf(end));
					//will force the entry to cache or is effectively a no-op when already cached
					rs.last();	
				}
			}
			return result;
		} catch (Exception e) {
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

	private int skip(boolean cache, final ResultSet rs, int skipSize)
			throws SQLException {
		int skipped = 0;
		if (!cache) {
			for (int i = 0; i < skipSize; i++) {
				skipped++;
				if (!rs.next()) {
					break;
				}
			}
		} else {
			rs.absolute(skipSize);
		}
		return skipped;
	}

	@Override
	public CountResponse executeCount(Query query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
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
		} catch (Exception e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
			}
		}
	}

	@Override
	public UpdateResponse executeUpdate(Command query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT, Statement.RETURN_GENERATED_KEYS);
			if (!parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			final int count = stmt.executeUpdate();
			final Map<String, Object> keys = getGeneratedKeys(stmt.getGeneratedKeys());
			stmt.close();
			return new UpdateResponse() {
				@Override
				public Map<String, Object> getGeneratedKeys() {
					return keys;
				}
				@Override
				public int getUpdateCount() {
					return count;
				}
			};
		} catch (Exception e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
			}
		}
	}
	
	private Map<String, Object> getGeneratedKeys(ResultSet result) throws SQLException{
		if (result == null) {
			return null;
		}
		
		HashMap<String, Object> keys = new HashMap<String, Object>();
		ResultSetMetaData metadata = result.getMetaData();
		// now read the values
		while(result.next()) {
			for (int i = 0; i < metadata.getColumnCount(); i++) {
				String label = metadata.getColumnLabel(i+1);
				keys.put(label, result.getObject(i+1));
			}
		}
		return keys;
	}

	@Override
	public EdmDataServices getMetadata() {
		if (this.edmMetaData == null) {
			this.edmMetaData = buildMetadata(getVDB(), getMetadataStore());
		}
		return this.edmMetaData;
	}
	
    public static EdmDataServices buildMetadata(VDBMetaData vdb, MetadataStore metadataStore) {
        try {
            List<EdmSchema.Builder> edmSchemas = new ArrayList<EdmSchema.Builder>();
            for (Schema schema:metadataStore.getSchemaList()) {
                if (isVisible(vdb, schema)) {
                    ODataEntitySchemaBuilder.buildEntityTypes(schema, edmSchemas);
                }
            }
            for (Schema schema:metadataStore.getSchemaList()) {
                if (isVisible(vdb, schema)) {
                    ODataEntitySchemaBuilder.buildFunctionImports(schema, edmSchemas);
                }
            }
            for (Schema schema:metadataStore.getSchemaList()) {
                if (isVisible(vdb, schema)) {
                    ODataEntitySchemaBuilder.buildAssosiationSets(schema, edmSchemas);
                }
            }
            return EdmDataServices.newBuilder().addSchemas(edmSchemas).build();
        } catch (Exception e) {
            throw new TeiidRuntimeException(e);
        }
    }	
    
    private static boolean isVisible(VDBMetaData vdb, Schema schema) {
        String schemaName = schema.getName();
        Model model = vdb.getModel(schemaName);
        if (model == null) {
            return true;
        }
        return model.isVisible();
    }
	
	static OProperty<?> buildPropery(String propName, EdmType type, Object value, String invalidCharacterReplacement) throws TransformationException, SQLException, IOException {
		if (!(type instanceof EdmSimpleType)) {
			if (type instanceof EdmCollectionType) {
				EdmCollectionType collectionType = (EdmCollectionType)type;
				EdmType componentType = collectionType.getItemType();
				Builder<OObject> b = OCollections.newBuilder(componentType);
				if (value instanceof Array) {
					value = ((Array)value).getArray();
				}
				int length = java.lang.reflect.Array.getLength(value);
				for (int i = 0; i < length; i++) {
					Object o = java.lang.reflect.Array.get(value, i);
					OProperty p = buildPropery("x", componentType, o, invalidCharacterReplacement);
					if (componentType instanceof EdmSimpleType) {
						b.add(OSimpleObjects.create((EdmSimpleType) componentType, p.getValue()));
					} else {
						throw new AssertionError("Multi-dimensional arrays are not yet supported.");
						//b.add((OCollection)p.getValue());
					}
				}
				return OProperties.collection(propName, collectionType, b.build());
			}
			throw new AssertionError("non-simple types are not yet supported");
		}
		EdmSimpleType expectedType = (EdmSimpleType)type;
		if (value == null) {
			return OProperties.null_(propName, expectedType);
		}
		Class<?> sourceType = DataTypeManager.getRuntimeType(value.getClass());
		Class<?> targetType = DataTypeManager.getDataTypeClass(ODataTypeManager.teiidType(expectedType.getFullyQualifiedTypeName()));
		if (sourceType != targetType) {
			Transform t = DataTypeManager.getTransform(sourceType,targetType);
			if (t == null && BlobType.class == targetType) {
				if (sourceType == ClobType.class) {
					return OProperties.binary(propName, ClobType.getString((Clob)value).getBytes());
				}
				if (sourceType == SQLXML.class) {
					return OProperties.binary(propName, ((SQLXML)value).getString().getBytes());
				}
			}
			value = t!=null?t.transform(value, targetType):value;
			value = replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
			return OProperties.simple(propName, expectedType, value);
		}
		value = replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
		return OProperties.simple(propName, expectedType,value);
	}

	static Object replaceInvalidCharacters(EdmSimpleType expectedType, Object value, String invalidCharacterReplacement) {
		if (expectedType != EdmSimpleType.STRING || invalidCharacterReplacement == null) {
			return value;
		}
		if (value instanceof Character) {
			value = value.toString();
		}
		String s = (String)value;
		StringBuilder result = null;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c <= 0x0020 && c != ' ' && c != '\n' && c != '\t' && c != '\r') {
				if (result == null) {
					result = new StringBuilder();
					result.append(s.substring(0, i));
				}
				result.append(invalidCharacterReplacement);
			} else if (result != null) {
				result.append(c);
			}
		}
		if (result == null) {
			return value;
		}
		return result.toString();
	}
}
