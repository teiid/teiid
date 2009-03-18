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

package org.teiid.dqp.internal.process.validator;

import org.teiid.dqp.internal.process.Request;
import org.teiid.dqp.internal.process.validator.ModelVisibilityValidationVisitor;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestModelVisibilityValidationVisitor extends TestCase {
	
	private void helpTestLookupVisibility(boolean visible) throws QueryParserException, QueryValidatorException, MetaMatrixComponentException {
		FakeVDBService vdbService = new FakeVDBService();
		String vdbName = "foo"; //$NON-NLS-1$
		String vdbVersion = "1"; //$NON-NLS-1$
		String modelName = "pm1"; //$NON-NLS-1$
		vdbService.addModel(vdbName, vdbVersion, modelName, visible?ModelInfo.PUBLIC:ModelInfo.PRIVATE, false);
		ModelVisibilityValidationVisitor mvvv = new ModelVisibilityValidationVisitor(vdbService, vdbName, vdbVersion);
		
		String sql = "select lookup('pm1.g1', 'e1', 'e2', 1)"; //$NON-NLS-1$
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		Request.validateWithVisitor(mvvv, FakeMetadataFactory.example1Cached(), command, true);
	}
	
	public void testLookupVisibility() throws Exception {
		helpTestLookupVisibility(true);
	}
	
	public void testLookupVisibilityFails() throws Exception {
		try {
			helpTestLookupVisibility(false);
			fail("expected exception"); //$NON-NLS-1$
		} catch (QueryValidatorException e) {
			assertEquals("Group does not exist: pm1.g1", e.getMessage()); //$NON-NLS-1$
		}
	}

}
