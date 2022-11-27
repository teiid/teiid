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
import org.teiid.jdbc.TeiidDriver;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public abstract class TeiidRSProvider {

    public StreamingOutput execute(final String vdbName, final String version, final String procedureName, final LinkedHashMap<String, String> parameters,
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
                    ObjectConverterUtil.write(output, is, -1, false, true);
                    output.flush();
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

    public StreamingOutput executePost(final String vdbName, final String version, final String procedureName, final MultipartFormDataInput parameters,
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
                    ObjectConverterUtil.write(output, is, -1, false, true);
                    output.flush();
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
                throw new SQLException(RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28002));
            }
        }
        else if (!usingReturn){
            throw new SQLException(RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28002));
        } else {
            result = statement.getObject(1);
        }
        return handleResult(charSet, result);
    }

    private LinkedHashMap<String, Object> convertParameters(Connection conn, String vdbName, String procedureName,
            LinkedHashMap<String, String> inputParameters) throws SQLException {

        Map<String, Class<?>> expectedTypes = getParameterTypes(conn, vdbName, procedureName);
        LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<String, Object>();
        try {
            for (String columnName : inputParameters.keySet()) {
                Class<?> runtimeType = expectedTypes.get(columnName);
                if (runtimeType == null) {
                    throw new SQLException(RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28001, columnName,
                            procedureName));
                }
                Object value = inputParameters.get(columnName);
                if (value != null) {
                    if (Array.class.isAssignableFrom(runtimeType)) {
                        List<String> array = StringUtil.split((String)value, ","); //$NON-NLS-1$
                        value = array.toArray(new String[array.size()]);
                    }
                    else if (DataTypeManager.DefaultDataClasses.VARBINARY.isAssignableFrom(runtimeType)) {
                        value = Base64.decode((String)value);
                    }
                    else {
                        if (DataTypeManager.isTransformable(String.class, runtimeType)) {
                            Transform t = DataTypeManager.getTransform(String.class, runtimeType);
                            value = t.transform(value, runtimeType);
                        }
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

        Map<String, Class<?>> runtimeTypes = getParameterTypes(conn, vdbName, procedureName);
        LinkedHashMap<String, Object> expectedValues = new LinkedHashMap<String, Object>();
        Map<String, List<InputPart>> inputParameters = form.getFormDataMap();

        for (String columnName : inputParameters.keySet()) {
            Class<?> runtimeType = runtimeTypes.get(columnName);
            if (runtimeType == null) {
                throw new SQLException(RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28001, columnName, procedureName));
            }
            if (runtimeType.isAssignableFrom(Array.class)) {
                List<InputPart> valueStreams = inputParameters.get(columnName);
                ArrayList<Object> array = new ArrayList<Object>();
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

    private Object convertToRuntimeType(Class<?> runtimeType, final InputPart part) throws IOException,
            SQLException {
        if (SQLXML.class.isAssignableFrom(runtimeType)) {
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
        else if (Blob.class.isAssignableFrom(runtimeType)) {
            return new BlobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return part.getBody(InputStream.class, null);
                }
            });
        }
        else if (Clob.class.isAssignableFrom(runtimeType)) {
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
        else if (DataTypeManager.DefaultDataClasses.VARBINARY.isAssignableFrom(runtimeType)) {
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

    private LinkedHashMap<String, Class<?>> getParameterTypes(Connection conn, String vdbName, String procedureName)
            throws SQLException {
        String schemaName = procedureName.substring(0, procedureName.lastIndexOf('.')).replace('\"', ' ').trim();
        String procName = procedureName.substring(procedureName.lastIndexOf('.')+1).replace('\"', ' ').trim();
        LinkedHashMap<String, Class<?>> expectedTypes = new LinkedHashMap<String, Class<?>>();
        try {
            ResultSet rs = conn.getMetaData().getProcedureColumns(vdbName, schemaName, procName, "%"); //$NON-NLS-1$
            while(rs.next()) {
                String columnName = rs.getString(4);
                int columnDataType = rs.getInt(6);
                Class<?> runtimeType = DataTypeManager
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

    public StreamingOutput executeQuery(final String vdbName, final String vdbVersion, final String sql, boolean json, final boolean passthroughAuth)
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
                            throw new SQLException(RestServicePlugin.Util.gs(RestServicePlugin.Event.TEIID28002));
                        }
                    }
                    InputStream is = handleResult(Charset.defaultCharset().name(), result);
                    ObjectConverterUtil.write(output, is, -1, false, true);
                    output.flush();
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

    private Connection getConnection(String vdbName, String version, boolean passthough) throws SQLException {
        TeiidDriver driver = new TeiidDriver();
        return driver.connect("jdbc:teiid:"+vdbName+"."+version+";"+(passthough?"PassthroughAuthentication=true;":""), null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
}
