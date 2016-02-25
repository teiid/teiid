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
import java.sql.Array;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.odata4j.core.OCollection;
import org.odata4j.core.OCollection.Builder;
import org.odata4j.core.OCollections;
import org.odata4j.core.OObject;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.core.OSimpleObjects;
import org.odata4j.edm.EdmCollectionType;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.ServerErrorException;
import org.odata4j.internal.EdmDataServicesDecorator;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.InlineCount;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.Responses;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.LocalProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.net.TeiidURL;
import org.teiid.odbc.ODBCServerRemoteImpl;
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
import org.teiid.vdb.runtime.VDBKey;

public class LocalClient implements Client {
	private static final String BATCH_SIZE = "batch-size"; //$NON-NLS-1$
	private static final String SKIPTOKEN_TIME = "skiptoken-cache-time"; //$NON-NLS-1$
	static final String INVALID_CHARACTER_REPLACEMENT = "invalid-xml10-character-replacement"; //$NON-NLS-1$
	static final String DELIMITER = "--" ; //$NON-NLS-1$
	
	private volatile VDBMetaData vdb;
	private String vdbName;
	private Integer vdbVersion;
	private int batchSize;
	private long cacheTime;
	private String connectionString;
	private Properties initProperties;
	private EdmDataServices edmMetaData;
	private TeiidDriver driver = TeiidDriver.getInstance();
	private String invalidCharacterReplacement;

	public LocalClient(String vdbName, Integer vdbVersion, Properties props) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.batchSize = PropertiesUtils.getIntProperty(props, BATCH_SIZE, BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE);
		this.cacheTime = PropertiesUtils.getLongProperty(props, SKIPTOKEN_TIME, 300000L);
		this.invalidCharacterReplacement = props.getProperty(INVALID_CHARACTER_REPLACEMENT);
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:teiid:").append(this.vdbName); //$NON-NLS-1$
		if (vdbVersion != null) {
			sb.append(".").append(this.vdbVersion); //$NON-NLS-1$
		}
		sb.append(";"); //$NON-NLS-1$ 
		this.initProperties = props;
		if (this.initProperties.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION) == null) {
		    this.initProperties.put(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "true"); //$NON-NLS-1$    
		}
		if (this.initProperties.getProperty(LocalProfile.TRANSPORT_NAME) == null) {
		    this.initProperties.setProperty(LocalProfile.TRANSPORT_NAME, "odata");    
		}		 
		if (this.initProperties.getProperty(LocalProfile.WAIT_FOR_LOAD) == null) {
		    this.initProperties.put(LocalProfile.WAIT_FOR_LOAD, "0"); //$NON-NLS-1$
		}
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
		if (this.vdb == null) {
			//validate the name
			if (vdbName == null || !vdbName.matches("[\\w-\\.]+")) { //$NON-NLS-1$
				throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16008));			
			}
			VDBKey key = new VDBKey(vdbName, vdbVersion==null?0:vdbVersion);
			if (key.isSemantic() && (!key.isFullySpecified() || key.isAtMost() || key.getVersion() != 1)) {
				throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16021, key));
			}
		}
		
		ConnectionImpl connection = driver.connect(this.connectionString, this.initProperties);
		if (connection == null) {
			throw new TeiidRuntimeException("URL not valid " + this.connectionString); //$NON-NLS-1$
		}
		ODBCServerRemoteImpl.setConnectionProperties(connection);
		ODBCServerRemoteImpl.setConnectionProperties(connection, this.initProperties);
		return connection;
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
                stmt.registerOutParameter(i++, JDBCSQLTypeInfo
                        .getSQLType(ODataTypeManager.teiidType(returnType.getFullyQualifiedTypeName())));
			}

			if (!parameters.isEmpty()) {
				for (SQLParam param:parameters) {
					stmt.setObject(i++, param.value, param.sqlType);
				}
			}

			boolean results = stmt.execute();
			if (returnType != null && !results) {
            	Object result = stmt.getObject(1);
            	OProperty<?> prop = buildPropery("return", returnType, result, invalidCharacterReplacement); //$NON-NLS-1$
            	return Responses.property(prop); 
            }
			return null;
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
    public BaseResponse executeSQL(Query query, List<SQLParam> parameters,
            QueryInfo queryInfo, EntityCollector<?> collector) {
	    ConnectionImpl connection = null;
		try {
			boolean cache = queryInfo != null && this.batchSize > 0; 
			if (cache) {
				CacheHint hint = new CacheHint();
				hint.setTtl(this.cacheTime);
				hint.setScope(CacheDirective.Scope.USER);
				hint.setMinRows(Long.valueOf(this.batchSize));
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

			connection = getConnection();
			String sessionId = connection.getServerConnection().getLogonResult().getSessionID();
			
			String skipToken = null;			
			if (queryInfo != null && queryInfo.skipToken != null) {
			    skipToken = queryInfo.skipToken;
			    if (cache) {
    			    int idx = queryInfo.skipToken.indexOf(DELIMITER);
    			    sessionId = queryInfo.skipToken.substring(0, idx);
    			    skipToken = queryInfo.skipToken.substring(idx+2);
			    }
			}
			String sql = query.toString();
			if (cache) {
			    sql += " /* "+ sessionId +" */"; //$NON-NLS-1$ //$NON-NLS-2$
			}
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			
            final PreparedStatement stmt = connection.prepareStatement(sql,
                    cache ? ResultSet.TYPE_SCROLL_INSENSITIVE
                            : ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
			if (parameters!= null && !parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}

			final ResultSet rs = stmt.executeQuery();
			
			//skip to the initial position
			int skipcount = 0;
			int skipSize = 0;
			//skip based upon the skip value
			if (getCount && queryInfo.skip != null) {
				skipSize = queryInfo.skip;
			}
			//skip based upon the skipToken
			if (skipToken != null) {
				skipSize += Integer.parseInt(skipToken);
			}
			if (skipSize > 0) {
				skipcount += skip(cache, rs, skipSize);
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
			Object previous = null;
			for (int i = 0; i < size; i++) {
			    if (!rs.next()) {
			        break;
			    }
				if (previous != null) {
				    boolean sameRow = true;
				    // this needs to be done because, we do not want break the expand nodes
				    // in between
				    while(sameRow) {				        
				        Object current = collector.addRow(previous, rs, this.invalidCharacterReplacement);
				        sameRow = collector.isSameRow(previous, current);
				        previous = current;
				        if (sameRow) {
				            // advance cursor
			                if (!rs.next()) {
			                    break;
			                }
			                skipcount++;
			                i++; // keep the total count or rows
				        } else {
				            break;
				        }
				    };
				} else {
				    previous = collector.addRow(previous, rs, this.invalidCharacterReplacement);
				    skipcount++;
				}
			}
			collector.lastRow(previous);
			
			//set the count
			if (getCount) {
				if (!cache) {
					while (rs.next()) {
						skipcount++;
					}
				} else {
					rs.last();
					skipcount = rs.getRow();
				}
			}
			collector.setInlineCount(skipcount);
			
			//set the skipToken if needed
			if (cache && collector.size() == this.batchSize) {
				int end = skipSize + collector.size();
				if (getCount) {
					if (end < Math.min(top, skipcount)) {
					    collector.setSkipToken(nextToken(cache, sessionId, end));
					}
				} else if (rs.next()) {
				    collector.setSkipToken(nextToken(cache, sessionId, end));
					//will force the entry to cache or is effectively a no-op when already cached
					rs.last();	
				}
			}
			return collector;
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
	
	private String nextToken(boolean cache, String sessionid, int skip) {
	    if (cache) {
	        return sessionid+DELIMITER+String.valueOf(skip);
	    }
	    return String.valueOf(skip);
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
                    ODataEntitySchemaBuilder.buildEntityTypes(schema, edmSchemas, false);
                }
            }
            for (Schema schema:metadataStore.getSchemaList()) {
                if (isVisible(vdb, schema)) {
                    ODataEntitySchemaBuilder.buildFunctionImports(schema, edmSchemas, false);
                }
            }
            for (Schema schema:metadataStore.getSchemaList()) {
                if (isVisible(vdb, schema)) {
                    ODataEntitySchemaBuilder.buildAssosiationSets(schema, edmSchemas, false);
                }
            }
            final EdmDataServices edmDataServices = EdmDataServices.newBuilder().addSchemas(edmSchemas).build();
            
            EdmDataServicesDecorator decorator = new EdmDataServicesDecorator() {
				
				@Override
				protected EdmDataServices getDelegate() {
					return edmDataServices;
				}
				
				public EdmEntitySet findEdmEntitySet(String entitySetName) {
					int idx = entitySetName.indexOf('.');
				    if (idx != -1) {
				      EdmEntitySet ees = super.findEdmEntitySet(entitySetName);
				      if (ees != null) {
				    	  return ees;
				      }
				    }
				    EdmEntitySet result = null;
				    for (EdmSchema schema : this.getSchemas()) {
				      for (EdmEntityContainer eec : schema.getEntityContainers()) {
				        for (EdmEntitySet ees : eec.getEntitySets()) {
				          if (ees.getName().equals(entitySetName)) {
				        	  if (result != null) {
				        		  throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16017, entitySetName));
				        	  }
				        	  result = ees;
				          }
				        }
				      }
				    }
				    return result;
				}
				
				public EdmFunctionImport findEdmFunctionImport(String functionImportName) {
					int idx = functionImportName.indexOf('.');
				    if (idx != -1) {
				      EdmFunctionImport efi = super.findEdmFunctionImport(functionImportName);
				      if (efi != null) {
				        return efi;
				      }
				    }    
				    EdmFunctionImport result = null;
				    for (EdmSchema schema : this.getSchemas()) {
				      for (EdmEntityContainer eec : schema.getEntityContainers()) {
				        for (EdmFunctionImport efi : eec.getFunctionImports()) {
				          if (efi.getName().equals(functionImportName)) {
				        	  if (result != null) {
				        		  throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16017, functionImportName));
				        	  }
				              result = efi;
				          }
				        }
				      }
				    }
				    return result;
				}
			};

            return decorator;
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
	
    static OProperty<?> buildPropery(String propName, EdmType type,
            Object value, String invalidCharacterReplacement)
            throws TransformationException, SQLException, IOException {
        
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
					OProperty<?> p = buildPropery("x", componentType, o, invalidCharacterReplacement);
					if (componentType instanceof EdmSimpleType) {
						b.add(OSimpleObjects.create((EdmSimpleType<?>) componentType, p.getValue()));
					} else if (componentType instanceof EdmCollectionType) {
					    //Builder<OObject> cb = OCollections.newBuilder(componentType);
					    b.add((OCollection<?>)p.getValue());
					    //b.add(cb.build());
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
			value = DataTypeManager.convertToRuntimeType(value, true);
			value = t!=null?t.transform(value, targetType):value;
			value = replaceInvalidCharacters(expectedType, value, invalidCharacterReplacement);
			if (value instanceof BinaryType) {
				value = ((BinaryType)value).getBytesDirect();
			}
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
