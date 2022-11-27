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

package org.teiid.translator.loopback;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;


/**
 * Represents the execution of a command.
 */
public class LoopbackExecution implements UpdateExecution, ProcedureExecution {
//     Connector resources
    private LoopbackExecutionFactory config;
    private Command command;
    private String staticStringValue = ""; //$NON-NLS-1$

    // Execution state
    private Random randomNumber = new Random(0);
    private List<Object> row;
    private List<Class<?>> types;
    private List<Integer> lengths;
    private boolean waited = false;
    private int rowsReturned = 0;
    private int rowsNeeded = 1;
    private BigInteger rowNumber = BigInteger.ZERO;
    private int batchSize = 256;

    public LoopbackExecution(Command command, LoopbackExecutionFactory config, ExecutionContext context) {
        this.config = config;
        this.command = command;
        if (context != null) {
            this.batchSize = context.getBatchSize();
        }
        staticStringValue = constructIncrementedString(config.getCharacterValuesSize());
    }

    /**
     * Creates string "ABCD...ZABC..." of length characterValueSize
     * @param characterValuesSize
     * @return
     */
    public static String constructIncrementedString(int characterValuesSize) {
        //Create string of length as defined in the translator configuration
        StringBuffer alphaString = new StringBuffer();
        int genAlphaSize = characterValuesSize;
        char currentChar = 'A';
        while (genAlphaSize-- != 0){
            alphaString.append(currentChar);
            if (currentChar =='Z')
                currentChar = 'A';
            else
                currentChar += 1;
        }
        return alphaString.toString();
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        // Wait on first batch if necessary
        if(this.config.getWaitTime() > 0 && !waited) {
            // Wait a random amount of time up to waitTime milliseconds
            int randomTimeToWait = randomNumber.nextInt(this.config.getWaitTime());

            if (rowsReturned > 0) {
                randomTimeToWait/=2;
            }

            // If we're asynch and the wait time was longer than the poll interval,
            // then just say we don't have results instead
            if(this.config.getPollIntervalInMilli() >= 0 && randomTimeToWait > this.config.getPollIntervalInMilli()) {
                waited = true;
                DataNotAvailableException dnae = new DataNotAvailableException(randomTimeToWait);
                dnae.setStrict(true);
                throw dnae;
            }
            try {
                Thread.sleep(randomTimeToWait);
            } catch(InterruptedException e) {
            }
            waited = true;
        }

        List<Object> resultRow = row;
        if(rowsReturned < this.rowsNeeded && resultRow.size() > 0) {
            rowsReturned++;
            if (config.getIncrementRows()) {
                rowNumber = rowNumber.add(BigInteger.ONE);
                generateRow();
            }
            if (waited && (rowsReturned%batchSize)==0) {
                waited = false;
            }
            return resultRow;
        }

        return null;
    }




    @Override
    public void execute() throws TranslatorException {

        // Log our command
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Loopback executing command: " + command); //$NON-NLS-1$

        if(this.config.isThrowError()) {
            throw new TranslatorException("Failing because Error=true"); //$NON-NLS-1$
        }

        this.rowsNeeded = this.config.getRowCount();

        if (command instanceof QueryExpression) {
            QueryExpression queryCommand = (QueryExpression)command;
            if (queryCommand.getLimit() != null) {
                int limit = queryCommand.getLimit().getRowLimit();
                int offset = queryCommand.getLimit().getRowOffset();
                this.rowsNeeded -= offset;
                this.rowsNeeded = Math.min(rowsNeeded, limit);
                this.rowNumber = BigInteger.valueOf(offset);
            }
        }


        // Prepare for execution
        determineOutputTypes();
        generateRow();
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
            TranslatorException {
        return new int [] {0};
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        Call proc = (Call)this.command;
        int count = proc.getReturnType() != null ? 1:0;
        for (Argument param : proc.getArguments()) {
            if (param.getDirection() == Direction.INOUT || param.getDirection() == Direction.OUT) {
                count++;
            }
        }
        return Arrays.asList(new Object[count]);
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void cancel() throws TranslatorException {

    }

    private void determineOutputTypes() {
        // Get select columns and lookup the types in metadata
        if(command instanceof QueryExpression) {
            QueryExpression query = (QueryExpression) command;
            types = Arrays.asList(query.getColumnTypes());
            lengths = query.getProjectedQuery().getDerivedColumns().stream()
                    .map(d -> getExpressionLength(d.getExpression()))
                    .collect(Collectors.toList());
            return;
        }
        if (command instanceof Call) {
            types = Arrays.asList(((Call)command).getResultSetColumnTypes());
            return;
        }
        types = new ArrayList<Class<?>>(1);
        types.add(Integer.class);
    }

    private static int getExpressionLength(Expression ex) {
        if (ex instanceof ColumnReference) {
            Column c = ((ColumnReference)ex).getMetadataObject();
            if (c != null) {
                return c.getLength();
            }
        }
        return -1;
    }

    public static final Long DAY_SECONDS=86400L;
    private static final int DATE_PERIOD = 365*(8099-1970);

    /**
     * Increments value in each column. If the value is bounded type (e.g. short) it will cycle the values.
     */
    private void generateRow() {
        List<Object> newRow = new ArrayList<Object>(types.size());
        String incrementedString = incrementString(staticStringValue,rowNumber);
        for (int i = 0; i < types.size(); i++) {
            Class<?> type = types.get(i);
            Object val = getVal(incrementedString, type, i);
            newRow.add(val);
        }
        row = newRow;
    }

    Object getVal(String incrementedString, Class<?> type, int index) {
        Object val;
        if (type.equals(Integer.class)) {
            val = rowNumber.intValue();
        } else if (type.equals(java.lang.Short.class)) {
            val = rowNumber.shortValue();
        } else if (type.equals(java.lang.Long.class)) {
            val = rowNumber.longValue();
        } else if (type.equals(java.lang.Float.class)) {
            val = rowNumber.floatValue()/10;
        } else if (type.equals(java.lang.Double.class)) {
            val = rowNumber.doubleValue()/10;
        } else if (type.equals(java.lang.Character.class)) {
            val = (char)((((rowNumber.byteValue()&0xff) + 2)%26) + 97);
        } else if (type.equals(java.lang.Byte.class)) {
            val = rowNumber.byteValue();
        } else if (type.equals(java.lang.Boolean.class)) {
            val = rowNumber.intValue()%2!=0;
        } else if (type.equals(java.math.BigInteger.class)) {
            val = rowNumber;
        } else if (type.equals(java.math.BigDecimal.class)) {
            val = new BigDecimal(rowNumber, 1);
        } else if (type.equals(java.sql.Date.class)) {
            val = new Date(DAY_SECONDS * 1000 * (rowNumber.intValue()%DATE_PERIOD));
        } else if (type.equals(java.sql.Time.class)) {
            val = new Time((rowNumber.intValue()%DAY_SECONDS) * 1000);
        } else if (type.equals(java.sql.Timestamp.class)) {
            val = new Timestamp(rowNumber.longValue());
        } else if (type.equals(TypeFacility.RUNTIME_TYPES.CLOB)) {
            val = this.config.getTypeFacility().convertToRuntimeType(incrementedString.toCharArray());
        } else if (type.equals(TypeFacility.RUNTIME_TYPES.BLOB) || type.equals(TypeFacility.RUNTIME_TYPES.VARBINARY)) {
            val = this.config.getTypeFacility().convertToRuntimeType(incrementedString.getBytes());
        } else if (type.isArray()) {
            val = Array.newInstance(type.getComponentType(), 1);
            Array.set(val, 0, getVal(incrementedString, type.getComponentType(), index));
        } else {
            val = incrementedString;
            if (lengths != null) {
                int length = lengths.get(index);
                if (length > -1) {
                    val = incrementedString.substring(0, Math.min(length, incrementedString.length()));
                }
            }
        }
        return val;
    }

    /**
     * Increments the string by appending unique number in sequence. Preserves string length.
     * @param number Number in the sequence of strings (which row)
     * @return
     */
    public static String incrementString(String string, BigInteger number) {
        String numberString= number.toString();
        if (number.equals(BigInteger.ZERO)) {
            return string;//Backward compatibility for first string
        }
        return string.substring(0,string.length()-numberString.length())+ numberString;
    }

}
