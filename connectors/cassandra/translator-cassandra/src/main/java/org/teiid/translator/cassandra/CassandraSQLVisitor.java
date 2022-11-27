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

package org.teiid.translator.cassandra;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.TypeFacility;

public class CassandraSQLVisitor extends SQLStringVisitor {

    private static final String ALLOW_FILTERING = "ALLOW FILTERING";

    public String getTranslatedSQL() {
        return buffer.toString();
    }

    @Override
    protected String replaceElementName(String group, String element) {
        return element;
    }

    public void translateSQL(LanguageObject obj) {
        append(obj);
    }

    @Override
    public void visit(Select obj) {
        boolean allowFiltering = false;

        buffer.append(SELECT).append(Tokens.SPACE);
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()){
            NamedTable table = (NamedTable)obj.getFrom().get(0);

            allowFiltering = Boolean.parseBoolean(
                      table.getMetadataObject().getProperties().get(CassandraMetadataProcessor.ALLOWFILTERING));

            if(table.getMetadataObject().getColumns() !=  null){
                append(obj.getDerivedColumns());
            }
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);
            append(obj.getFrom());
        }


        if(obj.getWhere() != null){
            buffer.append(Tokens.SPACE).append(WHERE).append(Tokens.SPACE);
            append(obj.getWhere());
        }

        if(obj.getOrderBy() != null){
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }

        if(obj.getLimit() != null){
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }

        if (allowFiltering) {
              buffer.append(Tokens.SPACE);
              buffer.append(ALLOW_FILTERING);
        }
    }

    @Override
    public void visit(Literal obj) {
        if (obj.getValue() == null) {
            super.visit(obj);
            return;
        }
        if (obj.getValue() instanceof Timestamp) {
            buffer.append(((Timestamp)obj.getValue()).getTime());
            return;
        }
        //cassandra directly parses uuids
        if (obj.getValue() instanceof UUID) {
            buffer.append(obj.getValue());
            return;
        }
        //TODO: only supported with Cassandra 2 or later
        /*if (obj.isBindEligible()
                || obj.getType() == TypeFacility.RUNTIME_TYPES.OBJECT
                || type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
            if (values == null) {
                values = new ArrayList<Object>();
            }
            buffer.append('?');
            if (type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
                values.add(ByteBuffer.wrap(((BinaryType)obj.getValue()).getBytesDirect()));
            } else {
                values.add(obj.getValue());
            }
            return;
        }*/
        Class<?> type = obj.getType();
        if (type == TypeFacility.RUNTIME_TYPES.VARBINARY) {
            buffer.append("0x") //$NON-NLS-1$
              .append(obj.getValue());
            return;
        }
        if (type == TypeFacility.RUNTIME_TYPES.BLOB) {
            buffer.append("0x"); //$NON-NLS-1$
            Blob b = (Blob)obj.getValue();
            InputStream binaryStream = null;
            try {
                if (b.length() > Integer.MAX_VALUE) {
                    throw new AssertionError("Blob is too large"); //$NON-NLS-1$
                }
                binaryStream = b.getBinaryStream();
                PropertiesUtils.toHex(buffer, binaryStream);
            } catch (SQLException e) {
                throw new TeiidRuntimeException(e);
            } catch (IOException e) {
                throw new TeiidRuntimeException(e);
            } finally {
                if (binaryStream != null) {
                    try {
                        binaryStream.close();
                    } catch (IOException e) {
                    }
                }
            }
            return;
        }
        if (!Number.class.isAssignableFrom(type)
                && type != TypeFacility.RUNTIME_TYPES.BOOLEAN
                && type != TypeFacility.RUNTIME_TYPES.VARBINARY) {
            //just handle as strings things like timestamp
            type = TypeFacility.RUNTIME_TYPES.STRING;
        }
        super.appendLiteral(obj, type);
    }
}
