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
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public abstract class TeiidRSProvider {

	public InputStream execute(String vdbName, int version,	String procedureName, 
			LinkedHashMap<String, String> parameters, String charSet, boolean passthroughAuth, boolean usingReturn) throws SQLException {
        Object result = null;
        //the generated code sends a empty string rather than null.
        if (charSet != null && charSet.trim().isEmpty()) {
        	charSet = null;
        }
        
        Connection conn = getConnection(vdbName, version, passthroughAuth);
        try {
        	StringBuilder sb = new StringBuilder();
        	sb.append("{ "); //$NON-NLS-1$
        	if (usingReturn) {
        		sb.append("? = "); //$NON-NLS-1$
        	}
        	sb.append("CALL ").append(procedureName); //$NON-NLS-1$
        	sb.append("("); //$NON-NLS-1$
        	boolean first = true;
        	for (Map.Entry<String, String> entry : parameters.entrySet()) {
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
                for (String value : parameters.values()) {
                	if (value == null) {
                		continue;
                	}
					statement.setString(i++, value);
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
                rs.close();
            }
            else if (!usingReturn){
            	throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50092)); 
            } else {
            	result = statement.getObject(1);
            }
            statement.close();
            return handleResult(charSet, result);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
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
	
	public InputStream executeQuery(String vdbName, int vdbVersion, String sql, boolean json, boolean passthroughAuth) throws SQLException {
		Connection conn = getConnection(vdbName, vdbVersion, passthroughAuth);
		Object result = null;
		try {
			Statement statement = conn.createStatement();
            final boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                ResultSet rs = statement.getResultSet();
                if (rs.next()) {
                    result = rs.getObject(1);
                } else {
                	throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50092)); 
                }
                rs.close();
            }			
			statement.close();
			return handleResult(Charset.defaultCharset().name(), result);
		} finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
		}
	}
	
	private Connection getConnection(String vdbName, int version, boolean passthough) throws SQLException {
		TeiidDriver driver = new TeiidDriver();
		return driver.connect("jdbc:teiid:"+vdbName+"."+version+";"+(passthough?"PassthroughAuthentication=true;":""), null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
	}
}
