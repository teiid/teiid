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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.util.CommandContext;

public class TupleSourceCache {

    final static class CachableVisitor extends LanguageVisitor {
        boolean cacheable = true;
        List<Object> parameters;

        @Override
        public void visit(Constant c) {
            if (c.isMultiValued()) {
                notCachable();
            } else if (DataTypeManager.isLOB(c.getType())) {
                if (parameters == null) {
                    parameters = new ArrayList<Object>();
                }
                parameters.add(c.getValue());
            }
        }

        private void notCachable() {
            cacheable = false;
            setAbort(true);
        }

        @Override
        public void visit(DependentSetCriteria obj) {
            notCachable();
        }
    }

    private static class SharedState {
        TupleBuffer tb;
        TupleSource ts;
        int id;
        int expectedReaders;

        private void remove() {
            ts.closeSource();
            tb.remove();
            tb = null;
            ts = null;
        }
    }

    public abstract static class CopyOnReadTupleSource implements TupleSource {
        int rowNumber = 1;
        TupleBuffer tb;
        TupleSource ts;

        protected CopyOnReadTupleSource(TupleBuffer tb, TupleSource ts) {
            this.tb = tb;
            this.ts = ts;
        }

        @Override
        public List<?> nextTuple() throws TeiidComponentException,
                TeiidProcessingException {
            synchronized (tb) {
                if (rowNumber <= tb.getRowCount()) {
                    return tb.getBatch(rowNumber).getTuple(rowNumber++);
                }
                if (tb.isFinal()) {
                    return null;
                }
                List<?> row = ts.nextTuple();
                if (row == null) {
                    tb.setFinal(true);
                } else {
                    tb.addTuple(row);
                    rowNumber++;
                }
                return row;
            }
        }

    }

    private class SharedTupleSource extends CopyOnReadTupleSource {
        private SharedState state;
        private boolean closed = false;

        public SharedTupleSource(SharedState state) {
            super(state.tb, state.ts);
            this.state = state;
        }

        @Override
        public void closeSource() {
            if (!closed && state.expectedReaders != -1 && --state.expectedReaders == 0 && sharedStates != null && sharedStates.containsKey(state.id)) {
                state.remove();
                sharedStates.remove(state.id);
            }
            closed = true;
        }

    }

    private Map<Integer, SharedState> sharedStates;

    public void close() {
        if (sharedStates != null) {
            for (SharedState ss : sharedStates.values()) {
                ss.remove();
            }
            sharedStates = null;
        }
    }

    public TupleSource getSharedTupleSource(CommandContext context, Command command, String modelName, RegisterRequestParameter parameterObject, BufferManager bufferMgr, ProcessorDataManager pdm) throws TeiidComponentException, TeiidProcessingException {
        if (sharedStates == null) {
            sharedStates = new HashMap<Integer, SharedState>();
        }
        SharedState state = sharedStates.get(parameterObject.info.id);
        if (state == null) {
            state = new SharedState();
            state.expectedReaders = parameterObject.info.sharingCount;
            RegisterRequestParameter param = new RegisterRequestParameter(parameterObject.connectorBindingId, parameterObject.nodeID, -1);
            param.fetchSize = parameterObject.fetchSize;
            param.copyStreamingLobs = true;
            state.ts = pdm.registerRequest(context, command, modelName, param);
            if (param.doNotCache) {
                return state.ts;
            }
            state.tb = bufferMgr.createTupleBuffer(command.getProjectedSymbols(), context.getConnectionId(), TupleSourceType.PROCESSOR);
            state.id = parameterObject.info.id;
            sharedStates.put(parameterObject.info.id, state);
        }
        return new SharedTupleSource(state);
    }

}
