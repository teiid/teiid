/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.language;

import java.util.Collections;
import java.util.Iterator;

import com.metamatrix.data.language.IParameter;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.lang.StoredProcedure;

import junit.framework.TestCase;

public class TestProcedureImpl extends TestCase {

    /**
     * Constructor for TestExecuteImpl.
     * @param name
     */
    public TestProcedureImpl(String name) {
        super(name);
    }

    public static ProcedureImpl example() throws Exception {
        String sql = "EXEC pm1.sq3('x', 1)"; //$NON-NLS-1$
        Command command = new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, TstLanguageBridgeFactory.metadata);                
        return (ProcedureImpl)TstLanguageBridgeFactory.factory.translate((StoredProcedure)command);
    }
    
    public void testGetProcedureName() throws Exception {
        assertEquals("pm1.sq3", example().getProcedureName()); //$NON-NLS-1$
    }

    public void testGetParameters() throws Exception {
        ProcedureImpl exec = example();
        assertNotNull(exec.getParameters());
        assertEquals(3, exec.getParameters().size());
        for (Iterator i = exec.getParameters().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IParameter);
        }
    }
    
    public void testEquals1() {
        ProcedureImpl proc1 = new ProcedureImpl("proc1", Collections.EMPTY_LIST, null); //$NON-NLS-1$
        ProcedureImpl proc2 = new ProcedureImpl("proc1", Collections.EMPTY_LIST, null); //$NON-NLS-1$
        assertEquals(proc1, proc2);        
    }

    public void testEquals2() {
        ProcedureImpl proc1 = new ProcedureImpl("proc1", Collections.EMPTY_LIST, null); //$NON-NLS-1$
        ProcedureImpl proc2 = new ProcedureImpl("proc2", Collections.EMPTY_LIST, null); //$NON-NLS-1$
        assertTrue(! proc1.equals(proc2));        
    }
    
}
