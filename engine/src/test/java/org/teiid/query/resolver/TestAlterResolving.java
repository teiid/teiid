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

package org.teiid.query.resolver;

import static org.junit.Assert.*;
import static org.teiid.query.resolver.TestResolver.*;

import org.junit.Test;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestAlterResolving {

	@Test public void testAlterView() {
		AlterView alterView = (AlterView) helpResolve("alter view SmallA_2589 as select 2", RealMetadataFactory.exampleBQTCached());
		assertNotNull(alterView.getTarget().getMetadataID());
	}
	
	@Test public void testAlterProcedure() {
		AlterProcedure alterProc = (AlterProcedure) helpResolve("alter procedure MMSP5 as begin select param1; end", RealMetadataFactory.exampleBQTCached());
		assertNotNull(alterProc.getTarget().getMetadataID());
		Query q = (Query)alterProc.getDefinition().getResultsCommand();
		assertTrue(((ElementSymbol)q.getSelect().getSymbol(0)).isExternalReference());
	}
	
	@Test public void testAlterTriggerInsert() {
		AlterTrigger alterTrigger = (AlterTrigger) helpResolve("alter trigger on SmallA_2589 instead of insert as for each row select new.intkey;", RealMetadataFactory.exampleBQTCached());
		assertNotNull(alterTrigger.getTarget().getMetadataID());
	}
	
	@Test public void testAlterTriggerInsert_Invalid() {
		helpResolveException("alter trigger on SmallA_2589 instead of insert as for each row select old.intkey;", RealMetadataFactory.exampleBQTCached());
	}
	
	@Test public void testAlterView_Invalid() {
		helpResolveException("alter view bqt1.SmallA as select 2", RealMetadataFactory.exampleBQTCached());
	}

}
