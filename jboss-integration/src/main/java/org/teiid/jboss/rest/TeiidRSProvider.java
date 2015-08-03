/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.teiid.core.types.*;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.StringUtil;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public abstract class TeiidRSProvider {

    public StreamingOutput execute(final String vdbName, final int version, final String procedureName, final LinkedHashMap<String, String> parameters,
    		final String charSet, final boolean passthroughAuth, final boolean usingReturn) throws SQLException {
    	return new StreamingOutput() {
			
			@Override
			public void write(OutputStream output) throws IOException,
					WebApplicationException {
				Connection conn = null;
				try {
					conn = getConnection(vdbName, version, passthroughAuth);
			        LinkedHashMap<String, Object> updatedParameters = convertParameters(conn, vdbName, procedureName, parameters);
			        InputStream is = executeProc(conn, procedureName, updatedParameters, charSet, usingReturn);
			        ObjectConverterUtil.write(output, is, -1);
				} catch (SQLException e) {
					throw new WebApplicationException(e);
				} finally {
		            if (conn != null) {
		                try {
		                    conn.close();
		                } catch (SQLException e) {
		                }
		            }
		        }
			}
		};
    }
	
    public StreamingOutput executePost(final String vdbName, final int version, final String procedureName, final MultipartFormDataInput parameters,
            final String charSet, final boolean passthroughAuth, final boolean usingReturn) throws SQLException {
    	return new StreamingOutput() {
			
			@Override
			public void write(OutputStream output) throws IOException,
					WebApplicationException {
				Connection conn = null;
				try {
					conn = getConnection(vdbName, version, passthroughAuth);
					LinkedHashMap<String, Object> updatedParameters = convertParameters(conn, vdbName, procedureName, parameters);
			        InputStream is = executeProc(conn, procedureName, updatedParameters, charSet, usingReturn);
			        ObjectConverterUtil.write(output, is, -1);
				} catch (SQLException e) {
					throw new WebApplicationException(e);
				} finally {
		            if (conn != null) {
		                try {
		                    conn.close();
		                } catch (SQLException e) {
		                }
		            }
		        }
			}
		};
    }
    
    public InputStream executeProc(Connection conn, String procedureName, LinkedHashMap<String, Object> parameters,
            String charSet, boolean usingReturn) throws SQLException {
    	//the generated code sends a empty string rather than null.
        if (charSet != null && charSet.trim().isEmpty()) {
            charSet = null;
        }
        Object result = null;
    	StringBuilder sb = new StringBuilder();
    	sb.append("{ "); //$NON-NLS-1$
    	if (usingReturn) {
    		sb.append("? = "); //$NON-NLS-1$
    	}
    	sb.append("CALL ").append(procedureName); //$NON-NLS-1$
    	sb.append("("); //$NON-NLS-1$
    	boolean first = true;
    	for (Map.Entry<String, Object> entry : parameters.entrySet()) {
    		if (entry.getValue() == null) {
    			continue;
    		}
    		if (!first) {
    			sb.append(", "); //$NON-NLS-1$
    		}
    		first = false;
    		sb.append(SQLStringVisitor.escapeSinglePart(entry.getKey())).append("=>?"); //$NON-NLS-1$
    	}
    	sb.append(") }"); //$NON-NLS-1$
    	
        CallableStatement statement = conn.prepareCall(sb.toString());
        if (!parameters.isEmpty()) {
            int i = usingReturn?2:1;
            for (Object value : parameters.values()) {
            	if (value == null) {
            		continue;
            	}
				statement.setObject(i++, value);
            }
        }

        final boolean hasResultSet = statement.execute();
        if (hasResultSet) {
            ResultSet rs = statement.getResultSet();
            if (rs.next()) {
                result = rs.getObject(1);
            } else {
            	throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50092));
            }
        }
        else if (!usingReturn){
        	throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50092));
        } else {
        	result = statement.getObject(1);
        }
        return handleResult(charSet, result);
    }

    private LinkedHashMap<String, Object> convertParameters(Connection conn, String vdbName, String procedureName,
            LinkedHashMap<String, String> inputParameters) throws SQLException {
        
        Map<String, Class> expectedTypes = getParameterTypes(conn, vdbName, procedureName);
        LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<String, Object>();
        try {
            for (String columnName : inputParameters.keySet()) {
                Class runtimeType = expectedTypes.get(columnName);
                if (runtimeType == null) {
                    throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50105, columnName,
                            procedureName));
                }                
                Object value = inputParameters.get(columnName);
                if (runtimeType.isAssignableFrom(Array.class)) {
                    List<String> array = StringUtil.split((String)value, ","); //$NON-NLS-1$
                    value = array.toArray(new String[array.size()]);
                }
                else if (runtimeType.isAssignableFrom(DataTypeManager.DefaultDataClasses.VARBINARY)) {
                    value = Base64.decode((String)value);
                }
                else {
                    if (value != null && DataTypeManager.isTransformable(String.class, runtimeType)) {
                        Transform t = DataTypeManager.getTransform(String.class, runtimeType);
                        value = t.transform(value, runtimeType);
                    }
                }
                expectedValues.put(columnName, value);
            }
            return expectedValues;
        } catch (TransformationException e) {
            throw new SQLException(e);
        }
    }
    
    private LinkedHashMap<String, Object> convertParameters(Connection conn, String vdbName, String procedureName,
            MultipartFormDataInput form) throws SQLException {
        
        Map<String, Class> runtimeTypes = getParameterTypes(conn, vdbName, procedureName);
        LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<String, Object>();
        Map<String, List<InputPart>> inputParameters = form.getFormDataMap();
        
        for (String columnName : inputParameters.keySet()) {
            Class runtimeType = runtimeTypes.get(columnName);
            if (runtimeType == null) {
                throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50105, columnName, procedureName));
            }
            if (runtimeType.isAssignableFrom(Array.class)) {
                List<InputPart> valueStreams = inputParameters.get(columnName);
                ArrayList array = new ArrayList();
                try {
                    for (InputPart part : valueStreams) {
                        array.add(part.getBodyAsString());
                    }
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                expectedValues.put(columnName, array.toArray(new Object[array.size()]));
            } else {
                final InputPart part = inputParameters.get(columnName).get(0);
                try {
                    expectedValues.put(columnName, convertToRuntimeType(runtimeType, part));
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        }
        return expectedValues;
    }

    private Object convertToRuntimeType(Class runtimeType, final InputPart part) throws IOException,
            SQLException {
        if (runtimeType.isAssignableFrom(SQLXML.class)) {
            SQLXMLImpl xml = new SQLXMLImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return part.getBody(InputStream.class, null);
                }
            });
            if (charset(part) != null) {
                xml.setEncoding(charset(part));
            }
            return xml;
        }
        else if (runtimeType.isAssignableFrom(Blob.class)) {
            return new BlobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return part.getBody(InputStream.class, null);
                }
            });
        }
        else if (runtimeType.isAssignableFrom(Clob.class)) {
            ClobImpl clob = new ClobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return part.getBody(InputStream.class, null);
                }
            }, -1);
            if (charset(part) != null) {
                clob.setEncoding(charset(part));
            }            
            return clob;
        }
        else if (runtimeType.isAssignableFrom(DataTypeManager.DefaultDataClasses.VARBINARY)) {
            return Base64.decode(part.getBodyAsString());
        }
        else if (DataTypeManager.isTransformable(String.class, runtimeType)) {
            try {
                return DataTypeManager.transformValue(part.getBodyAsString(), runtimeType);
            } catch (TransformationException e) {
                throw new SQLException(e);
            }
        }
        return part.getBodyAsString();
    }

    private String charset(final InputPart part) {
        return part.getMediaType().getParameters().get("charset"); //$NON-NLS-1$
    }    
    
    private LinkedHashMap<String, Class> getParameterTypes(Connection conn, String vdbName, String procedureName)
            throws SQLException {
        String schemaName = procedureName.substring(0, procedureName.lastIndexOf('.')).replace('\"', ' ').trim();
        String procName = procedureName.substring(procedureName.lastIndexOf('.')+1).replace('\"', ' ').trim();	    
        LinkedHashMap<String, Class> expectedTypes = new LinkedHashMap<String, Class>();
        try {
            ResultSet rs = conn.getMetaData().getProcedureColumns(vdbName, schemaName, procName, "%"); //$NON-NLS-1$
            while(rs.next()) {
                String columnName = rs.getString(4);
                int columnDataType = rs.getInt(6);
                Class runtimeType = DataTypeManager
                        .getRuntimeType(Class.forName(JDBCSQLTypeInfo.getJavaClassName(columnDataType)));
                expectedTypes.put(columnName, runtimeType);
            }
            rs.close();
            return expectedTypes;
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }	    
	}

	private InputStream handleResult(String charSet, Object result) throws SQLException {
        if (result == null) {
        	return null; //or should this be an empty result?
        }

		if (result instanceof SQLXML) {
			if (charSet != null) {
		    	XMLSerialize serialize = new XMLSerialize();
		    	serialize.setTypeString("blob"); //$NON-NLS-1$
		    	serialize.setDeclaration(true);
		    	serialize.setEncoding(charSet);
		    	serialize.setDocument(true);
		    	try {
					return ((BlobType)XMLSystemFunctions.serialize(serialize, new XMLType((SQLXML)result))).getBinaryStream();
				} catch (TransformationException e) {
					throw new SQLException(e);
				}
			}
			return ((SQLXML)result).getBinaryStream();
		}
		else if (result instanceof Blob) {
			return ((Blob)result).getBinaryStream();
		}
		else if (result instanceof Clob) {
			return new ReaderInputStream(((Clob)result).getCharacterStream(), charSet==null?Charset.defaultCharset():Charset.forName(charSet));
		}
		return new ByteArrayInputStream(result.toString().getBytes(charSet==null?Charset.defaultCharset():Charset.forName(charSet)));
	}

	public StreamingOutput executeQuery(final String vdbName, final int vdbVersion, final String sql, boolean json, final boolean passthroughAuth) 
	        throws SQLException {
		return new StreamingOutput() {
			
			@Override
			public void write(OutputStream output) throws IOException,
					WebApplicationException {
				Connection conn = null;
				try {
					conn = getConnection(vdbName, vdbVersion, passthroughAuth);					
					Statement statement = conn.createStatement();
		            final boolean hasResultSet = statement.execute(sql);
		            Object result = null;
		            if (hasResultSet) {
		                ResultSet rs = statement.getResultSet();
		                if (rs.next()) {
		                    result = rs.getObject(1);
		                } else {
		                	throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50092));
		                }
		            }
					InputStream is = handleResult(Charset.defaultCharset().name(), result);
					ObjectConverterUtil.write(output, is, -1);
				} catch (SQLException e) {
					throw new WebApplicationException(e);
				} finally {
					try {
						if (conn != null) {
							conn.close();
						}
					} catch (SQLException e) {
					}
				}
			}
		};
	}

	private Connection getConnection(String vdbName, int version, boolean passthough) throws SQLException {
		TeiidDriver driver = new TeiidDriver();
		return driver.connect("jdbc:teiid:"+vdbName+"."+version+";"+(passthough?"PassthroughAuthentication=true;":""), null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
	}
}
