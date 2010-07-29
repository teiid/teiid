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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;

/**
 */
public class TestCollectionTupleSource {

    @Test public void testNullSource() {
        List<SingleElementSymbol> elements = new ArrayList<SingleElementSymbol>();
        elements.add(new ElementSymbol("x")); //$NON-NLS-1$
        elements.add(new ElementSymbol("y")); //$NON-NLS-1$
        CollectionTupleSource nts = CollectionTupleSource.createNullTupleSource();   
        
        // Walk it and get no data
        List tuple = nts.nextTuple();
        nts.closeSource();

        assertEquals("Didn't get termination tuple for first tuple", null, tuple);             //$NON-NLS-1$
    }
    
    @Test public void testUpdateCountSource() {
        CollectionTupleSource nts = CollectionTupleSource.createUpdateCountTupleSource(5);   
        
        // Walk it and get no data
        List tuple = nts.nextTuple();
        nts.closeSource();

        assertEquals("Didn't get termination tuple for first tuple", Arrays.asList(5), tuple);             //$NON-NLS-1$
    }


}
