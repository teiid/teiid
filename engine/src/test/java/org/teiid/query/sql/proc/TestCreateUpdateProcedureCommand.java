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

package org.teiid.query.sql.proc;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.unittest.RealMetadataFactory;


/**
 *
 * @author gchadalavadaDec 9, 2002
 */
@SuppressWarnings("nls")
public class TestCreateUpdateProcedureCommand  extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestCreateUpdateProcedureCommand(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	

	public static final CreateProcedureCommand sample1() { 
	    return new CreateProcedureCommand(TestBlock.sample1());
	}

	public static final CreateProcedureCommand sample2() { 
	    return new CreateProcedureCommand(TestBlock.sample2());
	}
	
	public static final CreateProcedureCommand sample3() { 
	    return new CreateProcedureCommand(TestBlock.sample1());
	}

	// ################################## ACTUAL TESTS ################################	

	public void testGetBlock() {
		CreateProcedureCommand b1 = sample1();
        assertTrue("Incorrect Block on command", b1.getBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
	}
	
	public void testSetBlock() {
		CreateProcedureCommand b1 = (CreateProcedureCommand)sample1().clone();
		b1.setBlock(TestBlock.sample2());
        assertTrue("Incorrect Block on command", b1.getBlock().equals(TestBlock.sample2())); //$NON-NLS-1$
	}	
	
	public void testSelfEquivalence(){
		CreateProcedureCommand s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		CreateProcedureCommand s1 = sample1();
		CreateProcedureCommand s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		CreateProcedureCommand s1 = sample1();
		CreateProcedureCommand s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
    
    /**
     * Test cloning obj with mostly null state, test for NPE 
     */
    public void testCloneMethod3() {
        CreateProcedureCommand s1 = new CreateProcedureCommand();
        CreateProcedureCommand s2 = (CreateProcedureCommand)s1.clone();
        UnitTestUtil.helpTestEquivalence(0, s1, s2);
    }
    
    public void testProjectedSymbols() {
    	CreateProcedureCommand cupc = new CreateProcedureCommand();
    	cupc.setUpdateProcedure(false);
    	StoredProcedure sp = (StoredProcedure)TestResolver.helpResolve("call TEIIDSP9(p1=>1, p2=>?)", RealMetadataFactory.exampleBQTCached());
    	sp.setCallableStatement(true);
    	cupc.setResultsCommand(sp);
    	assertEquals(1, cupc.getProjectedSymbols().size());
    }
    
}
