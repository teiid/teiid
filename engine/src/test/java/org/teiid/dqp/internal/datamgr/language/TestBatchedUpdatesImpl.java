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

package org.teiid.dqp.internal.datamgr.language;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IUpdate;
import org.teiid.dqp.internal.datamgr.language.BatchedUpdatesImpl;

import junit.framework.TestCase;

import com.metamatrix.query.sql.lang.BatchedUpdateCommand;


/** 
 * @since 4.2
 */
public class TestBatchedUpdatesImpl extends TestCase {

    public TestBatchedUpdatesImpl(String name) {
        super(name);
    }

    public static BatchedUpdateCommand helpExample() {
        List updates = new ArrayList();
        updates.add(TestInsertImpl.helpExample("a.b")); //$NON-NLS-1$
        updates.add(TestUpdateImpl.helpExample());
        updates.add(TestDeleteImpl.helpExample());
        return new BatchedUpdateCommand(updates);
    }
    
    public static BatchedUpdatesImpl example() throws Exception {
        return (BatchedUpdatesImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetUpdateCommands() throws Exception {
        List updates = example().getUpdateCommands();
        assertEquals(3, updates.size());
        assertTrue(updates.get(0) instanceof IInsert);
        assertTrue(updates.get(1) instanceof IUpdate);
        assertTrue(updates.get(2) instanceof IDelete);
    }

}
