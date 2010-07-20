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

import junit.framework.TestCase;

import org.teiid.language.Call;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;


public class TestProcedureImpl extends TestCase {

    /**
     * Constructor for TestExecuteImpl.
     * @param name
     */
    public TestProcedureImpl(String name) {
        super(name);
    }

    public static Call example() throws Exception {
        String sql = "EXEC pm1.sq3('x', 1)"; //$NON-NLS-1$
        Command command = new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, TstLanguageBridgeFactory.metadata);                
        return TstLanguageBridgeFactory.factory.translate((StoredProcedure)command);
    }
    
    public void testGetProcedureName() throws Exception {
        assertEquals("sq3", example().getProcedureName()); //$NON-NLS-1$
    }

    public void testGetParameters() throws Exception {
        Call exec = example();
        assertNotNull(exec.getArguments());
        assertEquals(2, exec.getArguments().size());
    }
    
}
