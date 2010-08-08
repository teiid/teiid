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

import java.util.Arrays;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;

import junit.framework.TestCase;


/**
 *
 * @author gchadalavadaDec 11, 2002
 */
public class TestAssignmentStatement  extends TestCase {

	/**
	 * Constructor for TestAssignmentStatement.
	 */
	public TestAssignmentStatement(String name) { 
		super(name);
	}
	
	// ################################## TEST HELPERS ################################	

	public static final AssignmentStatement sample1() { 
		return new AssignmentStatement(new ElementSymbol("a"), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public static final AssignmentStatement sample2() {
    	Query query = new Query();
    	query.setSelect(new Select(Arrays.asList(new ElementSymbol("x")))); //$NON-NLS-1$
    	query.setFrom(new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("y"))))); //$NON-NLS-1$
    	return new AssignmentStatement(new ElementSymbol("b"), query); //$NON-NLS-1$
	}
	
	// ################################## ACTUAL TESTS ################################	
	
	public void testGetVariable() {
		AssignmentStatement s1 = sample1();
		assertEquals("Didn't get the same parts ", s1.getVariable(), new ElementSymbol("a"));		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testGetExpression() {
		AssignmentStatement s1 = sample1();
		assertEquals("Didn't get the same parts ", s1.getExpression(), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testGetCommand() throws Exception {
		AssignmentStatement s2 = sample2();
		Query query = (Query) QueryParser.getQueryParser().parseCommand("Select x from y"); //$NON-NLS-1$
		assertEquals("Didn't get the same parts ", ((ScalarSubquery)s2.getExpression()).getCommand(), query); //$NON-NLS-1$
	}
	
	public void testSelfEquivalence(){
		AssignmentStatement s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		AssignmentStatement s1 = sample1();
		AssignmentStatement s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		AssignmentStatement s1 = sample1();
		AssignmentStatement s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}

}
