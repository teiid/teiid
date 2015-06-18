/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class FakeConnector extends ExecutionFactory<Object, Object> {
    
    private int connectionCount;
    private int executionCount;
    private int closeCount;
    private boolean returnSingleUpdate;

    public int getConnectionCount() {
		return connectionCount;
	}
    
    public int getExecutionCount() {
		return executionCount;
	}
    
    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws TranslatorException {
    	executionCount++;
        FakeExecution result = new FakeExecution(executionContext);
        if (command instanceof BatchedUpdates || (command instanceof BatchedCommand && ((BatchedCommand)command).getParameterValues() != null)) {
        	result.batchOrBulk = true;
        } 
        return result;
    }
    
    @Override
    public Object getConnection(Object factory) throws TranslatorException {
    	connectionCount++;
    	return factory;
    }
    
    @Override
    public void closeConnection(Object connection, Object factory) {
    	closeCount++;
    }
    
    public int getCloseCount() {
		return closeCount;
	}
    
    public final class FakeExecution implements ResultSetExecution, UpdateExecution {
        private int rowCount;
        boolean batchOrBulk;
        ExecutionContext ec;
        
        public FakeExecution(ExecutionContext ec) {
            this.ec = ec;
        }
        @Override
        public void execute() throws TranslatorException {
            ec.addWarning(new Exception("Some warning")); //$NON-NLS-1$
        }
        @Override
        public List<?> next() throws TranslatorException, DataNotAvailableException {
            if (this.rowCount == 1) {
            	return null;
            }
            this.rowCount++;
            return new ArrayList<Object>(Arrays.asList(this.rowCount - 1));
        }
		@Override
		public int[] getUpdateCounts() throws DataNotAvailableException,
				TranslatorException {
			if (batchOrBulk) {
				if (returnSingleUpdate) {
					return new int[] {2};
				}
				return new int[] {1, 1};
			}
			return new int[] {1};
		}
		
		@Override
		public void close() {
		}
		
		@Override
		public void cancel() throws TranslatorException {
		}
    }
    
    public void setReturnSingleUpdate(boolean returnSingleUpdate) {
		this.returnSingleUpdate = returnSingleUpdate;
	}
    
    @Override
    public boolean returnsSingleUpdateCount() {
    	return returnSingleUpdate;
    }

	
}
