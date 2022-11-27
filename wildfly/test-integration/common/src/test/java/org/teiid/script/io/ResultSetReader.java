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

package org.teiid.script.io;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;


/**
 * This object wraps/extends a SQL ResultSet object as Reader object. Once the
 * ResultSet can read as reader then it can be persisted, printed or compared
 * easily without lot of code hassle of walking it every time.
 *
 * <p>PS: remember this is a Reader not InputStream, so all the fields read
 * going to be converted to strings before they returned.
 *
 * @since 4.3
 */
public class ResultSetReader extends StringLineReader {
    ResultSet source = null;

    // Number of columns in the result set
    int columnCount = 0;

    // delimiter between the fields while reading each row
    String delimiter = "    "; //$NON-NLS-1$

    boolean firstTime = true;
    int[] columnTypes = null;

    private int rowCount;

    public ResultSetReader(ResultSet in) {
        this.source = in;
    }

    public ResultSetReader(ResultSet in, String delimiter) {
        this.source = in;
        this.delimiter = delimiter;
    }

    /**
     * @see java.io.Reader#close()
     * @since 4.3
     */
    public void close() throws IOException {
        try {
            source.close();
            super.close();
        } catch (SQLException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * Get the next line of results from the ResultSet. The first line will be the
     * metadata of the resultset and then followed by the result rows. Each row will be
     * returned as one line.
     * @return next result line from result set.
     */
    protected String nextLine() throws IOException{
        try {
            if (firstTime) {
                firstTime = false;
                ResultSetMetaData metadata = source.getMetaData();
                columnCount = metadata.getColumnCount();
                columnTypes  = new int[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    columnTypes[i] = metadata.getColumnType(i+1);
                }
                return resultSetMetaDataToString(metadata, delimiter);
            }

            // if you get here then we are ready to read the results.
            if (source.next()) {
                rowCount++;
                StringBuffer sb = new StringBuffer();
                // Walk through column values in this row
                for (int col = 1; col <= columnCount; col++) {
                    Object anObj = source.getObject(col);
                    if (columnTypes[col-1] == Types.CLOB) {
                        sb.append(anObj != null ? source.getString(col) : "null"); //$NON-NLS-1$
                    }
                    else if (columnTypes[col-1] == Types.BLOB) {
                        sb.append(anObj != null ? "BLOB" : "null"); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    else if (columnTypes[col-1] == Types.SQLXML) {
                        SQLXML xml = (SQLXML)anObj;
                        sb.append(anObj != null ? prettyPrint(xml) : "null"); //$NON-NLS-1$
                    }
                    else {
                        sb.append(anObj != null ? anObj : "null"); //$NON-NLS-1$
                    }
                    if (col != columnCount) {
                        sb.append(delimiter);
                    }
                }
                sb.append("\n"); //$NON-NLS-1$
                return sb.toString();
            }
        } catch (SQLException e) {
            throw new IOException(e.getMessage());
        }
        return null;
    }

    public int getRowCount() {
        return rowCount;
    }

    /**
     * Get the first line from the result set. This is the resultset metadata line where
     * we gather the column names and their types.
     * @return
     * @throws SQLException
     */
    public static String resultSetMetaDataToString(ResultSetMetaData metadata, String delimiter) throws SQLException{
        StringBuffer sb = new StringBuffer();
        int columnCount = metadata.getColumnCount();
        for (int col = 1; col <= columnCount; col++) {
            sb.append(metadata.getColumnName(col))
                .append("[")          //$NON-NLS-1$
                .append(metadata.getColumnTypeName(col))
                .append("]");       //$NON-NLS-1$
            if (col != columnCount) {
                sb.append(delimiter);
            }
        }
        sb.append("\n"); //$NON-NLS-1$
        return sb.toString();
    }

    public static String prettyPrint(SQLXML xml) throws SQLException {
        try {
            TransformerFactory transFactory = TransformerFactory.newInstance();
            transFactory.setAttribute("indent-number", new Integer(2)); //$NON-NLS-1$

            Transformer tf = transFactory.newTransformer();
            tf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
            tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");//$NON-NLS-1$
            tf.setOutputProperty(OutputKeys.INDENT, "yes");//$NON-NLS-1$
            tf.setOutputProperty(OutputKeys.METHOD, "xml");//$NON-NLS-1$
            tf.setOutputProperty(OutputKeys.STANDALONE, "yes");//$NON-NLS-1$
            tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4"); //$NON-NLS-1$ //$NON-NLS-2$

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            StreamResult xmlOut = new StreamResult(new BufferedOutputStream(out));
            tf.transform(xml.getSource(StreamSource.class), xmlOut);

            return out.toString();
        } catch (Exception e) {
            return xml.getString();
        }
    }
}
