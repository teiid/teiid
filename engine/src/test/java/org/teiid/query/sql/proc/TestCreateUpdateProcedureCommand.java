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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.GroupSymbol;
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

	public static final CreateUpdateProcedureCommand sample1() { 
	    return new CreateUpdateProcedureCommand(TestBlock.sample1());
	}

	public static final CreateUpdateProcedureCommand sample2() { 
	    return new CreateUpdateProcedureCommand(TestBlock.sample2());
	}
	
	public static final CreateUpdateProcedureCommand sample3() { 
	    return new CreateUpdateProcedureCommand(TestBlock.sample1());
	}

	// ################################## ACTUAL TESTS ################################	

	public void testGetBlock() {
		CreateUpdateProcedureCommand b1 = sample1();
        assertTrue("Incorrect Block on command", b1.getBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
	}
	
	public void testSetBlock() {
		CreateUpdateProcedureCommand b1 = (CreateUpdateProcedureCommand)sample1().clone();
		b1.setBlock(TestBlock.sample2());
        assertTrue("Incorrect Block on command", b1.getBlock().equals(TestBlock.sample2())); //$NON-NLS-1$
	}	
	
	public void testSelfEquivalence(){
		CreateUpdateProcedureCommand s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		CreateUpdateProcedureCommand s1 = sample1();
		CreateUpdateProcedureCommand s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		CreateUpdateProcedureCommand s1 = sample1();
		CreateUpdateProcedureCommand s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
    
    /**
     * We've had some defects in the past with state added to 
     * CreateUpdateProcedureCommand but not added to the clone
     * method.  We don't currently have any unit tests that exercise
     * the clone method very well.  So this method simply asserts
     * that the number of fields that this test class thinks is in
     * CreateUpdateProcedureCommand is still current.  I.e., if someone
     * adds state to CreateUpdateProcedureCommand but doesn't update
     * this test, the test will fail.  The failure message is a reminder
     * to update the clone() method.  So this isn't really a test of the
     * clone() method, per se.
     * see defect 14018 
     * @since 4.2
     */
    public void testCloneMethod() {
        Field[] fields = CreateUpdateProcedureCommand.class.getDeclaredFields();

        final int EXPECTED_NUMBER_OF_FIELDS = 7; //<---update me if necessary!
        int actualNumberOfFields = fields.length;
        
        // Workaround for Java bug 4546736 and 4407429 (same bug) -
        // the Class literal "synthetic" field can be returned from the call
        // to getDeclaredFields(), it seems to show up as the last entry in the
        // Field array, and has a dollar sign "$" in it's name.
        if (fields[fields.length-1].getType().equals(Class.class) &&
            fields[fields.length-1].getName().indexOf("$") != -1) { //$NON-NLS-1$

            actualNumberOfFields--;
        }
        
        assertEquals("New state has been added to the class CreateUpdateProcedureCommand; please update this test, and update the clone() method if necessary.", EXPECTED_NUMBER_OF_FIELDS, actualNumberOfFields); //$NON-NLS-1$
    }
	
    /** 
     * tests that a CreateUpdateProcedureCommand with non-null variables for all of its
     * state produces a clone that has non-null variables for all of its state.
     */
    public void testCloneMethod2() throws Exception{
        CreateUpdateProcedureCommand s1 = sample1();
        
        //Command class state
        s1.addExternalGroupsToContext(new HashSet());
        s1.setIsResolved(true);
        s1.setOption(new Option());
        s1.setTemporaryMetadata(new HashMap());

        //CreateUpdateProcedure class state
        s1.setProjectedSymbols(new ArrayList());
        s1.setResultsCommand(new Query());
        s1.setSymbolMap(new HashMap());
        s1.setUpdateProcedure(true);
        s1.setVirtualGroup(new GroupSymbol("x")); //$NON-NLS-1$
        s1.setUserCommand(new Query());

        CreateUpdateProcedureCommand cloned = (CreateUpdateProcedureCommand)s1.clone();
        
        Class clazz = CreateUpdateProcedureCommand.class;
        Class superClazz = Command.class;
        
        Field field = null;
        //Command class state
        field = superClazz.getDeclaredField("tempGroupIDs"); //$NON-NLS-1$
        field.setAccessible( true );
        assertNotNull(field.get(cloned));
        field = superClazz.getDeclaredField("externalGroups"); //$NON-NLS-1$
        field.setAccessible( true );
        assertNotNull(field.get(cloned));
        field = superClazz.getDeclaredField("isResolved"); //$NON-NLS-1$
        field.setAccessible( true );
        assertTrue(((Boolean)field.get(cloned)).booleanValue());
        field = superClazz.getDeclaredField("option"); //$NON-NLS-1$
        field.setAccessible( true );
        assertNotNull(field.get(cloned));
        
        //CreateUpdateProcedure class state
        field = clazz.getDeclaredField("block"); //$NON-NLS-1$
        field.setAccessible( true );
        assertNotNull(field.get(cloned));
        field = clazz.getDeclaredField("symbolMap"); //$NON-NLS-1$
        field.setAccessible( true );
        assertNotNull(field.get(cloned));
        field = clazz.getDeclaredField("isUpdateProcedure"); //$NON-NLS-1$
        field.setAccessible( true );
        assertTrue(((Boolean)field.get(cloned)).booleanValue());
        field = clazz.getDeclaredField("projectedSymbols"); //$NON-NLS-1$
        field.setAccessible( true );
        assertNotNull(field.get(cloned));
    }
    
    /**
     * Test cloning obj with mostly null state, test for NPE 
     */
    public void testCloneMethod3() {
        CreateUpdateProcedureCommand s1 = new CreateUpdateProcedureCommand();
        CreateUpdateProcedureCommand s2 = (CreateUpdateProcedureCommand)s1.clone();
        UnitTestUtil.helpTestEquivalence(0, s1, s2);
    }
    
    public void testProjectedSymbols() {
    	CreateUpdateProcedureCommand cupc = new CreateUpdateProcedureCommand();
    	cupc.setUpdateProcedure(false);
    	StoredProcedure sp = (StoredProcedure)TestResolver.helpResolve("call TEIIDSP9(p1=>1, p2=>?)", RealMetadataFactory.exampleBQTCached(), null);
    	sp.setCallableStatement(true);
    	cupc.setResultsCommand(sp);
    	assertEquals(1, cupc.getProjectedSymbols().size());
    }
    
}
