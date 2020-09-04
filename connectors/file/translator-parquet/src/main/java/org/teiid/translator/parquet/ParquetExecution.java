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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.teiid.core.types.ArrayImpl;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


public class ParquetExecution extends BaseParquetExecution implements ResultSetExecution {

    public ParquetExecution(Select query, ExecutionContext executionContext,
                            RuntimeMetadata metadata, VirtualFileConnection connection, boolean immutable)
            throws TranslatorException {
        super(executionContext, metadata, connection, immutable);
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

    List<Object> projectRow(Group row) throws TranslatorException {
        final List<Object> output = new ArrayList<Object>();
        int field = 0;
        List<String> expectedColumnNames = this.visitor.getProjectedColumnNames();
        for (int i = 0; i < expectedColumnNames.size(); i++) {
            if(this.visitor.getPartitionedColumns().containsKey(expectedColumnNames.get(i))){
                output.add(partitionedColumnsValue.get(expectedColumnNames.get(i)));
            }else {
                Type fieldType = row.getType().getType(field);
                int valueCount = row.getFieldRepetitionCount(field);
                if ("REPEATED".equals(fieldType.getRepetition().toString())) {
                    output.add(getRepeatedList(row, field));
                }
                if (fieldType.getLogicalTypeAnnotation() != null && "LIST".equals(fieldType.getLogicalTypeAnnotation().toString())) {
                    output.add(getList(row.getGroup(field, 0)));
                    field++;
                    continue;
                }
                if (!fieldType.isPrimitive()) {
                    throw new TranslatorException("We don't support any logical type other than LIST as of now.");
                }
                if (valueCount == 0) {
                    output.add(null);
                } else {
                    PrimitiveType.PrimitiveTypeName type = fieldType.asPrimitiveType().getPrimitiveTypeName();
                    output.add(getPrimitiveTypeObject(type, field, 0, row, fieldType));
                }
                field++;
            }
        }
        return output;
    }

    private ArrayImpl getList(Group group) throws TranslatorException {
        ArrayList<Object> outputList = new ArrayList<Object>();
        int fieldCount = group.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = group.getFieldRepetitionCount(field);
            for (int index = 0; index < valueCount; index++) {
                Group listGroup = group.getGroup(field, index);
                Type fieldType = listGroup.getType().getType(0);
                if(!fieldType.isPrimitive()) {
                    throw new TranslatorException("We don't support any logical types in a list.");
                }
                PrimitiveType.PrimitiveTypeName type = fieldType.asPrimitiveType().getPrimitiveTypeName();
                outputList.add(getPrimitiveTypeObject(type, 0, 0, listGroup, fieldType));
            }
        }
        return new ArrayImpl(outputList.toArray());
    }

    private ArrayImpl getRepeatedList(Group row, int field) throws TranslatorException {
        ArrayList<Object> outputList = new ArrayList();
        int valueCount = row.getFieldRepetitionCount(field);
        for (int index = 0; index < valueCount; index++) {
            Group listGroup = row.getGroup(field, index);
            Type fieldType = listGroup.getType().getType(0);
            if(!fieldType.isPrimitive()) {
                throw new TranslatorException("We don't support any logical types in a list.");
            }
            if(listGroup.getFieldRepetitionCount(0) == 0){
                outputList.add(null);
            }
            PrimitiveType.PrimitiveTypeName type = fieldType.asPrimitiveType().getPrimitiveTypeName();
            outputList.add(getPrimitiveTypeObject(type, 0, 0, listGroup, fieldType));
        }
        return new ArrayImpl(outputList.toArray());
    }

    Object getPrimitiveTypeObject(PrimitiveType.PrimitiveTypeName primitiveTypeName, int field, int index, Group row, Type fieldType) {
        switch (primitiveTypeName) {
            case INT96:
                byte[] bytes = row.getInt96(field, index).getBytesUnsafe();
                BigInteger bigInteger = new BigInteger(bytes);
                return bigInteger;
            case INT64:
                return row.getLong(field, index);
            case INT32:
                return row.getInteger(field, index);
            case BOOLEAN:
                return row.getBoolean(field, index);
            case FLOAT:
                return row.getFloat(field, index);
            case DOUBLE:
                return row.getDouble(field, index);
            case BINARY:
                if ("STRING".equals(fieldType.getLogicalTypeAnnotation().toString())) {
                    return row.getBinary(field, index).toStringUsingUTF8();
                }
                bytes = row.getBinary(field, index).getBytesUnsafe();
                return bytes;
            case FIXED_LEN_BYTE_ARRAY:
                bytes = row.getBinary(field, index).getBytesUnsafe();
                return bytes;
        }
        return null;
    }
}
