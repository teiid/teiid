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

package org.teiid.dqp.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorWork;
import org.teiid.dqp.internal.datamgr.ConnectorWorkItem;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;


/**
 * This data service will automatically generate results when called with a query - basically
 * the same as the old loopback connector.
 */
@SuppressWarnings("nls")
public class AutoGenDataService extends ConnectorManager{

    // Number of rows that will be generated for each query
    private int rows = 10;
    private SourceCapabilities caps;
    public boolean throwExceptionOnExecute;
    public Integer dataNotAvailable;
    public boolean strict;
    public int sleep;
    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private boolean useIntCounter;
    public boolean addWarning;
    public CacheDirective cacheDirective;
    public boolean dataAvailable;
    public boolean threadBound;
    public CountDownLatch latch;

    public AutoGenDataService() {
        super("FakeConnector","FakeConnector"); //$NON-NLS-1$ //$NON-NLS-2$
        caps = TestOptimizer.getTypicalCapabilities();
    }

    public void setUseIntCounter(boolean useIntCounter) {
        this.useIntCounter = useIntCounter;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }

    public void setCaps(SourceCapabilities caps) {
        this.caps = caps;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getRows() {
        return this.rows;
    }

    @Override
    public ConnectorWork registerRequest(AtomicRequestMessage message)
            throws TeiidComponentException {
        List projectedSymbols = (message.getCommand()).getProjectedSymbols();
        List[] results = createResults(projectedSymbols, rows, useIntCounter);
        if (RelationalNodeUtil.isUpdate(message.getCommand())) {
            results = new List[] {Arrays.asList(1)};
        }

        final AtomicResultsMessage msg = ConnectorWorkItem.createResultsMessage(results);
        msg.setFinalRow(rows);
        return new ConnectorWork() {

            boolean returnedInitial;

            @Override
            public boolean isDataAvailable() {
                return dataAvailable;
            }

            @Override
            public AtomicResultsMessage more() throws TranslatorException {
                if (dataNotAvailable != null) {
                    int delay = dataNotAvailable;
                    dataNotAvailable = null;
                    DataNotAvailableException dnae = new DataNotAvailableException(delay);
                    dnae.setStrict(strict);
                    throw dnae;
                }
                if (addWarning) {
                    msg.setWarnings(Arrays.asList(new Exception()));
                }
                if (!returnedInitial) {
                    returnedInitial = true;
                    return msg;
                }
                throw new RuntimeException("Should not be called"); //$NON-NLS-1$
            }

            @Override
            public void execute() throws TranslatorException {
                executeCount.incrementAndGet();
                if (sleep > 0) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (latch != null) {
                    try {
                        latch.countDown();
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (throwExceptionOnExecute) {
                    throw new TranslatorException("Connector Exception"); //$NON-NLS-1$
                }
            }

            @Override
            public void close() {
                closeCount.incrementAndGet();
            }

            @Override
            public void cancel(boolean abnormal) {

            }

            @Override
            public CacheDirective getCacheDirective() {
                return cacheDirective;
            }

            @Override
            public boolean isForkable() {
                return true;
            }

            @Override
            public boolean isThreadBound() {
                return threadBound;
            }

            @Override
            public AtomicRequestID getId() {
                return null;
            }
        };
    }

    public AtomicInteger getExecuteCount() {
        return executeCount;
    }

    public AtomicInteger getCloseCount() {
        return closeCount;
    }

    public static List[] createResults(List symbols, int rowCount, boolean useIntCounter) {
        List[] rows = new List[rowCount];

        for(int i=0; i<rowCount; i++) {
            List row = new ArrayList();
            Iterator iter = symbols.iterator();
            while(iter.hasNext()) {
                Expression symbol = (Expression) iter.next();
                Class type = symbol.getType();
                row.add( getValue(type, i, useIntCounter) );
            }
            rows[i] = row;
        }

        return rows;
    }

    private static final String STRING_VAL = "ABCDEFG"; //$NON-NLS-1$
    private static final Integer INTEGER_VAL = new Integer(0);
    private static final Long LONG_VAL = new Long(0);
    private static final Float FLOAT_VAL = new Float(0.0);
    private static final Short SHORT_VAL = new Short((short)0);
    private static final Double DOUBLE_VAL = new Double(0.0);
    private static final Character CHAR_VAL = new Character('c');
    private static final Byte BYTE_VAL = new Byte((byte)0);
    public static final Clob CLOB_VAL = new ClobImpl(new InputStreamFactory() {
        @Override
        public InputStream getInputStream() throws IOException {
            return new ByteArrayInputStream("hello world".getBytes(Streamable.CHARSET));
        }
    }, -1);
    private static final Boolean BOOLEAN_VAL = Boolean.FALSE;
    private static final BigInteger BIG_INTEGER_VAL = new BigInteger("0"); //$NON-NLS-1$
    private static final BigDecimal BIG_DECIMAL_VAL = new BigDecimal("0"); //$NON-NLS-1$
    private static final java.sql.Date SQL_DATE_VAL = new java.sql.Date(0);
    private static final java.sql.Time TIME_VAL = new java.sql.Time(0);
    private static final java.sql.Timestamp TIMESTAMP_VAL = new java.sql.Timestamp(0);

    static Object getValue(Class<?> type, int row, boolean useIntCounter) {
        if(type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
            return STRING_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.INTEGER)) {
            return useIntCounter?row:INTEGER_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.SHORT)) {
            return SHORT_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.LONG)) {
            return LONG_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.FLOAT)) {
            return FLOAT_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
            return DOUBLE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.CHAR)) {
            return CHAR_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BYTE)) {
            return BYTE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            return BOOLEAN_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_INTEGER)) {
            return BIG_INTEGER_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
            return BIG_DECIMAL_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            return SQL_DATE_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
            return TIME_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            return TIMESTAMP_VAL;
        } else if(type.equals(DataTypeManager.DefaultDataClasses.CLOB)) {
            return CLOB_VAL;
        } else {
            return null;
        }
    }

    @Override
    public SourceCapabilities getCapabilities(){
        return caps;
    }

}
