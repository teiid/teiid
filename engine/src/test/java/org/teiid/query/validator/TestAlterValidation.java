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

package org.teiid.query.validator;

import org.junit.Test;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestAlterValidation {

	@Test public void testValidateAlterView() {
		TestValidator.helpValidate("alter view SmallA_2589 as select 2", new String[] {"SELECT 2"}, RealMetadataFactory.exampleBQTCached());
		TestValidator.helpValidate("alter view Defect15355 as select 'a', 1", new String[] {"SELECT 'a', 1"}, RealMetadataFactory.exampleBQTCached());
		TestValidator.helpValidate("alter view Defect15355 as select 'a', cast(1 as biginteger)", new String[] {}, RealMetadataFactory.exampleBQTCached());
		
		TestValidator.helpValidate("alter view SmallA_2589 as select * from bqt1.smalla", new String[] {}, RealMetadataFactory.exampleBQTCached());
	}
	
	@Test public void testValidateAlterViewDeep() {
		TestValidator.helpValidate("alter view Defect15355 as select xpathvalue('a', ':'), cast(1 as biginteger)", new String[] {"xpathvalue('a', ':')"}, RealMetadataFactory.exampleBQTCached());
	}
	
	@Test public void testValidateAlterTrigger() {
		TestValidator.helpValidate("alter trigger on SmallA_2589 instead of insert as for each row begin atomic select 1; end", new String[] {"SmallA_2589"}, RealMetadataFactory.exampleBQTCached());
	}
	
	@Test public void testValidateAlterProcedure() {
		TestValidator.helpValidate("alter procedure spTest8a as begin select 1; end", new String[] {"spTest8a"}, RealMetadataFactory.exampleBQTCached());
		TestValidator.helpValidate("alter procedure MMSP1 as begin select 1; end", new String[] {"BEGIN\nSELECT 1;\nEND"}, RealMetadataFactory.exampleBQTCached());
	}
	
}
