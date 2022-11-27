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
package org.teiid.translator.jdbc.pi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;

public class PIMetadataProcessor extends JDBCMetadataProcessor {
    static Pattern guidPattern = Pattern.compile(Pattern.quote("guid"), Pattern.CASE_INSENSITIVE);

    @ExtensionMetadataProperty(applicable= {Table.class, Procedure.class},
            datatype=String.class, display="Is Table Value Function",
            description="Marks the table as Table Value Function")
    public static final String TVF = MetadataFactory.PI_PREFIX+"TVF"; //$NON-NLS-1$

    public PIMetadataProcessor() {
        setStartQuoteString("[");
        setEndQuoteString("]");
    }

    protected String getRuntimeType(int type, String typeName, int precision, int scale) {
        if (typeName != null && guidPattern.matcher(typeName).find()) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        }
        return super.getRuntimeType(type, typeName, precision, scale);
    }

    public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
            throws SQLException, TranslatorException {
        super.getConnectorMetadata(conn, metadataFactory);
        for (String name:metadataFactory.getSchema().getTables().keySet()) {
            if (name.startsWith("ft_")) {
                Table table = metadataFactory.getSchema().getTable(name);
                table.setProperty(TVF, "true");
            }
        }
        for (String name:metadataFactory.getSchema().getProcedures().keySet()) {
            Procedure proc = metadataFactory.getSchema().getProcedure(name);
            proc.setProperty(TVF, "true");
        }
    }
}
