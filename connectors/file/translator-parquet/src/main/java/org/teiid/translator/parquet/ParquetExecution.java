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

package org.teiid.translator.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


public class ParquetExecution extends BaseParquetExecution implements ResultSetExecution {

    private Class<?>[] expectedColumnTypes;

    public ParquetExecution(Select query, ExecutionContext executionContext,
                            RuntimeMetadata metadata, VirtualFileConnection connection, boolean immutable)
            throws TranslatorException {
        super(executionContext, metadata, connection, immutable);
        this.expectedColumnTypes = query.getColumnTypes();
        visit(query);
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        Group row = nextRow();
        if (row == null) {
            return null;
        }
        return projectRow(row);
    }

    List<Object> projectRow(Group row) {
        ArrayList<Object> output = new ArrayList<Object>(this.visitor.getProjectedColumns().size());
        int fieldCount = row.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = row.getFieldRepetitionCount(field);
            Type fieldType = row.getType().getType(field);
            PrimitiveType.PrimitiveTypeName type = fieldType.asPrimitiveType().getPrimitiveTypeName();
            String fieldName = fieldType.getName();
            for (int index = 0; index < valueCount; index++) {
                switch (type) {
                    case INT96:
                        output.add(row.getInt96(field, index));
                        break;
                    case INT64:
                        output.add(row.getLong(field, index));
                        break;
                    case INT32:
                        output.add(row.getInteger(field, index));
                        break;
                    case BOOLEAN:
                        output.add(row.getBoolean(field, index));
                        break;
                    case FLOAT:
                        output.add(row.getFloat(field, index));
                        break;
                    case DOUBLE:
                        output.add(row.getDouble(field, index));
                        break;
                    case BINARY:
                        output.add(row.getBinary(field,index));
                        break;
                }
            }
        }
        return output;
    }
}
