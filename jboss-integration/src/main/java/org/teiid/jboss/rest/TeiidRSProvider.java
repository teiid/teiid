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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.jboss.IntegrationPlugin;

public abstract class TeiidRSProvider {

    public InputStream execute(String procedureSignature, Map<String, String> parameters, String charSet) throws SQLException {
        Object result = null;
        Connection conn = getConnection();
        try {
        	List<String> paramTypes = getPathTypes(procedureSignature);
            final String executeStatement = "call " + procedureSignature.substring(0, procedureSignature.indexOf('(')) + (parameters.isEmpty() ? "()" : createParmString(parameters.size())) + ";"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 

            PreparedStatement statement = conn.prepareStatement(executeStatement);
            if (!parameters.isEmpty()) {
                int i = 1;
                for (Object value : parameters.values()) {
                	try {
						Transform t = DataTypeManager.getTransform(DataTypeManager.DefaultDataTypes.STRING, paramTypes.get(i-1));
						if (t != null) {
							statement.setObject(i++, t.transform(value));
						}
						else {
							statement.setString(i++, (String)value);
						}
					} catch (TransformationException e) {
						throw new SQLException(e);
					}
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
            else {
            	throw new SQLException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50092)); 
            }

            statement.close();
            
            if (result instanceof SQLXML) {
            	return ((SQLXML)result).getBinaryStream();
            }
            else if (result instanceof Blob) {
            	return ((Blob)result).getBinaryStream();
            }
            else if (result instanceof Clob) {
            	return new ReaderInputStream(((Clob)result).getCharacterStream(), Charset.forName(charSet));
            }
            return new ByteArrayInputStream(result.toString().getBytes());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    protected String createParmString(int paramCount ) {
        StringBuilder sb = new StringBuilder();
        sb.append("(?"); //$NON-NLS-1$
        for (int i = 1; i < paramCount; i++) {
            sb.append(","); //$NON-NLS-1$
            sb.append("?"); //$NON-NLS-1$
        }
        sb.append(")"); //$NON-NLS-1$
        return sb.toString();
    }
    
    private ArrayList<String> getPathTypes(String pathStr ) {
        ArrayList pathParams = new ArrayList();
        
        String parms = pathStr.substring(pathStr.indexOf('(')+1, pathStr.indexOf(')'));
        StringTokenizer st = new StringTokenizer(parms, ","); //$NON-NLS-1$
        while (st.hasMoreTokens()) {
        	pathParams.add(st.nextToken());
        }
        return pathParams;
    }    
    
    protected abstract Connection getConnection() throws SQLException;

}
