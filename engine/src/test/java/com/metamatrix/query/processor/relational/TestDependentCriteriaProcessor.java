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

package com.metamatrix.query.processor.relational;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import org.junit.Test;

import com.metamatrix.query.processor.relational.DependentAccessNode;
import com.metamatrix.query.processor.relational.DependentCriteriaProcessor;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.util.CommandContext;

public class TestDependentCriteriaProcessor {

	@Test public void testNegatedSetCriteria() throws Exception {
		DependentAccessNode dan = new DependentAccessNode(0);
		SetCriteria sc = new SetCriteria(new ElementSymbol("e1"), Arrays.asList(new Constant(1), new Constant(2))); //$NON-NLS-1$
		DependentCriteriaProcessor dcp = new DependentCriteriaProcessor(1, dan, sc);
		Criteria result = dcp.prepareCriteria();
		assertEquals(new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(1)), result); //$NON-NLS-1$ 
		assertTrue(dcp.hasNextCommand());
	}
	
	@Test public void testEvaluatedSetCriteria() throws Exception {
		DependentAccessNode dan = new DependentAccessNode(0);
		CommandContext cc = new CommandContext();
		dan.setContext(cc);
		List<Reference> references = Arrays.asList(new Reference(1), new Reference(2));
		for (Reference reference : references) {
			cc.getVariableContext().setGlobalValue(reference.getContextSymbol(), 1);
		}
		SetCriteria sc = new SetCriteria(new ElementSymbol("e1"), references); //$NON-NLS-1$
		DependentCriteriaProcessor dcp = new DependentCriteriaProcessor(1, dan, sc);
		Criteria result = dcp.prepareCriteria();
		assertEquals(new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(1)), result); //$NON-NLS-1$ 
		assertFalse(dcp.hasNextCommand());
	}
	
}
