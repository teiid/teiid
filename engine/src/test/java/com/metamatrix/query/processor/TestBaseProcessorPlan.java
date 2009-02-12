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

package com.metamatrix.query.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.util.CommandContext;

public class TestBaseProcessorPlan extends TestCase {

    public TestBaseProcessorPlan(String name) {
        super(name);
    }

    public void testGetAndClearWarnings() {        
        FakeProcessorPlan plan = new FakeProcessorPlan();
        MetaMatrixException warning = new MetaMatrixException("test"); //$NON-NLS-1$
        plan.addWarning(warning);
        
        List warnings = plan.getAndClearWarnings();
        assertEquals("Did not get expected number of warnings", 1, warnings.size()); //$NON-NLS-1$
        assertEquals("Did not get expected warning", warning, warnings.get(0)); //$NON-NLS-1$
        assertNull("Did not clear warnings from plan", plan.getAndClearWarnings());         //$NON-NLS-1$
    }



    private static class FakeProcessorPlan extends BaseProcessorPlan {
            
        
            /**
         * @see java.lang.Object#clone()
         */
        public Object clone() {
            return null;
        }

        /**
         * @see com.metamatrix.query.processor.ProcessorPlan#close()
         */
        public void close() throws MetaMatrixComponentException {
        }

        /**
         * @see com.metamatrix.query.processor.ProcessorPlan#connectTupleSource(com.metamatrix.common.buffer.TupleSource, int)
         */
        public void connectTupleSource(TupleSource source, int dataRequestID) {
        }

        /**
         * @see com.metamatrix.query.processor.ProcessorPlan#getOutputElements()
         */
        public List getOutputElements() {
            return null;
        }

        /**
         * @see com.metamatrix.query.processor.ProcessorPlan#initialize(com.metamatrix.query.processor.ProcessorDataManager, java.lang.Object, com.metamatrix.common.buffer.BufferManager, java.lang.String, int)
         */
        public void initialize(
            CommandContext context,
            ProcessorDataManager dataMgr,
            BufferManager bufferMgr) {
        }

        /**
         * @see com.metamatrix.query.processor.ProcessorPlan#nextBatch()
         */
        public TupleBatch nextBatch() throws BlockedException, MetaMatrixComponentException {
            return null;
        }

        /**
         * @see com.metamatrix.query.processor.ProcessorPlan#open()
         */
        public void open() throws MetaMatrixComponentException {
        }

        /* (non-Javadoc)
         * @see com.metamatrix.query.processor.ProcessorPlan#getUpdateCount()
         */
        public int getUpdateCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        public Map getDescriptionProperties() {
            return new HashMap();
        }
        
        /** 
         * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
         * @since 4.2
         */
        public Collection getChildPlans() {
            return Collections.EMPTY_LIST;
        }
        

    }
}
