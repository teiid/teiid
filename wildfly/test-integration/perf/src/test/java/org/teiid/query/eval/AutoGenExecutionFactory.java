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
package org.teiid.query.eval;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.language.NamedTable;
import org.teiid.language.QueryExpression;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.runtime.HardCodedExecutionFactory;

class AutoGenExecutionFactory extends HardCodedExecutionFactory {

    private static final Character CHAR_VAL = new Character('c');
    private static final Byte BYTE_VAL = new Byte((byte)0);
    private static final Clob CLOB_VAL = new ClobImpl(new InputStreamFactory() {
        @Override
        public InputStream getInputStream() throws IOException {
            return new ByteArrayInputStream("hello world".getBytes(Streamable.CHARSET));
        }
    }, -1);
    private static final Boolean BOOLEAN_VAL = Boolean.FALSE;
    private static final BigInteger BIG_INTEGER_VAL = new BigInteger("0"); //$NON-NLS-1$
    private static final java.sql.Date SQL_DATE_VAL = TimestampUtil.createDate(69, 11, 31);
    private static final java.sql.Time TIME_VAL = new java.sql.Time(0);
    private static final java.sql.Timestamp TIMESTAMP_VAL = new java.sql.Timestamp(0);

    private static final java.sql.Timestamp IN_RANGE = TimestampUtil.createTimestamp(117, 2, 0, 0, 0, 0, 0);

    static Object getValue(Class<?> type, int row, int max) {
        if(type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
            //return "a";
            return String.valueOf(10000000 + row%max);
            //return "" + String.valueOf(100000+(row/4)*4); //sample;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.INTEGER)) {
            return row%max;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.SHORT)) {
            return (short)row%max;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.LONG)) {
            return (long)row%max;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.FLOAT)) {
            return (float)(row%max);
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
            return (double)(row%max);
        } else if(type.equals(DataTypeManager.DefaultDataClasses.CHAR)) {
            return CHAR_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BYTE)) {
            return BYTE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            return BOOLEAN_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_INTEGER)) {
            return BIG_INTEGER_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
            return BigDecimal.valueOf(row%max);
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            return SQL_DATE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
            return TIME_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            if (row%3==0) {
                return IN_RANGE;
            }
            return TIMESTAMP_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.CLOB)) {
            return CLOB_VAL;
        } else {
            return null;
        }
    }

    private Map<String, Integer> rowCounts = new HashMap<String, Integer>();

    @Override
    protected List<? extends List<?>> getData(QueryExpression command) {
        List<? extends List<?>> result = super.getData(command);
        if (result != null) {
            return result;
        }
        Class<?>[] types = command.getColumnTypes();
        String name = ((NamedTable)command.getProjectedQuery().getFrom().get(0)).getMetadataObject().getName();
        int rowCount = rowCounts.get(name);
        List resultList = new ArrayList();
        for (int i = 0; i < rowCount; i++) {
            List row = new ArrayList();
            for (int j = 0; j < types.length; j++) {
                Object value = getValue(types[j], i, Integer.MAX_VALUE);
                row.add(value);
            }
            resultList.add(row);
        }
        return resultList;
    }

    public void addRowCount(String name, int count) {
        this.rowCounts.put(name, count);
    }
}