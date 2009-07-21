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

package com.metamatrix.query.parser;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Create;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Drop;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.PredicateCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.BreakStatement;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.ContinueStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.DeclareStatement;
import com.metamatrix.query.sql.proc.HasCriteria;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.proc.RaiseErrorStatement;
import com.metamatrix.query.sql.proc.Statement;
import com.metamatrix.query.sql.proc.TranslateCriteria;
import com.metamatrix.query.sql.proc.WhileStatement;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.AllInGroupSymbol;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.TestCaseExpression;
import com.metamatrix.query.sql.symbol.TestSearchedCaseExpression;

public class TestParser extends TestCase {

	private ParseInfo info = new ParseInfo();
	
	// ################################## FRAMEWORK ################################
	
	public TestParser(String name) { 
		super(name);
	}	
	
	public void setUp() {
		info.allowDoubleQuotedVariable = true;
	}

	// ################################## TEST HELPERS ################################

    static void helpTest(String sql, String expectedString, Command expectedCommand) {
        helpTest(sql, expectedString, expectedCommand, new ParseInfo());
    }
	static void helpTest(String sql, String expectedString, Command expectedCommand, ParseInfo info) {
		Command actualCommand = null;
		String actualString = null;
		try {
			actualCommand = QueryParser.getQueryParser().parseCommand(sql, info);
			actualString = actualCommand.toString();
		} catch(Throwable e) { 
		    throw new RuntimeException(e);
		}

		assertEquals("Parse string does not match: ", expectedString, actualString); //$NON-NLS-1$
		assertEquals("Command objects do not match: ", expectedCommand, actualCommand);				 //$NON-NLS-1$
	}
    
    static void helpException(String sql) {
        helpException(sql, null);
    }
    
    static void helpException(String sql, String expected){
        try {
            QueryParser.getQueryParser().parseCommand(sql);
            fail("Expected exception for parsing " + sql); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            if (expected != null) {
                assertEquals(expected, e.getMessage());
            }
        }       
    }

    
    private void helpBlockTest(String block, String expectedString, Block expectedBlock) {
        Block actualBlock = null;
        String actualString = null;
        try {
            actualBlock = QueryParser.getQueryParser().parseBlock(block);
            actualString = actualBlock.toString();
                
        } catch(Throwable e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        assertEquals("Parse string does not match: ", expectedString, actualString); //$NON-NLS-1$
        assertEquals("Block does not match: ", expectedBlock, actualBlock);              //$NON-NLS-1$
    }
    
    private void helpCriteriaSelectorTest(String selector, String expectedString, CriteriaSelector expectedSelector) {
        
        CriteriaSelector actualSelector = null;
        String actualString = null;
        try {
            actualSelector = QueryParser.getQueryParser().parseCriteriaSelector(selector);
            actualString = actualSelector.toString();
                             
        } catch(Throwable e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        assertEquals("Parse string does not match: ", expectedString, actualString); //$NON-NLS-1$
        assertEquals("CriteriaSelector does not match: ", expectedSelector, actualSelector);              //$NON-NLS-1$
    }
	
    private void helpCriteriaTest(String crit, String expectedString, Criteria expectedCrit) {
               
        Criteria actualCrit = null;
        String actualString = null;
        try {
            //actualCrit = QueryParser.getQueryParser().parsePredicateCriteria(crit);
            actualCrit = QueryParser.getQueryParser().parseCriteria(crit);
            actualString = actualCrit.toString();
                
        } catch(Throwable e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        assertEquals("Parse string does not match: ", expectedString, actualString); //$NON-NLS-1$
        assertEquals("Criteria does not match: ", expectedCrit, actualCrit);              //$NON-NLS-1$
        
    }
    
    private void helpStmtTest(String stmt, String expectedString, Statement expectedStmt) {
        Statement actualStmt = null;
        String actualString = null;
        try {
            actualStmt = QueryParser.getQueryParser().parseStatement(stmt);
            actualString = actualStmt.toString();
                
        } catch(Throwable e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        assertEquals("Parse string does not match: ", expectedString, actualString); //$NON-NLS-1$
        assertEquals("Language objects do not match: ", expectedStmt, actualStmt);              //$NON-NLS-1$
    }
        
	// ################################## ACTUAL TESTS ################################

    // ======================== Joins ===============================================

	/** SELECT * FROM g1 inner join g2 on g1.a1=g2.a2 */
	public void testInnerJoin() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("g1.a1"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("g2.a2")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g1, g2, JoinType.JOIN_INNER, crits);
		From from = new From();
		from.addClause(jp);

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT * FROM g1 inner join g2 on g1.a1=g2.a2",  //$NON-NLS-1$
				 "SELECT * FROM g1 INNER JOIN g2 ON g1.a1 = g2.a2",  //$NON-NLS-1$
				 query);
	}

	/** SELECT * FROM g1 cross join g2 */
	public void testCrossJoin() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2")); //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g1, g2, JoinType.JOIN_CROSS);		
		From from = new From();
		from.addClause(jp);	

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM g1 cross join g2",  //$NON-NLS-1$
				 "SELECT * FROM g1 CROSS JOIN g2",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT * FROM (g1 cross join g2), g3 */
	public void testFromClauses() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2")); //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g1, g2, JoinType.JOIN_CROSS);		
		From from = new From();
		from.addClause(jp);	
		from.addClause(new UnaryFromClause(new GroupSymbol("g3"))); //$NON-NLS-1$

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM (g1 cross join g2), g3",  //$NON-NLS-1$
				 "SELECT * FROM g1 CROSS JOIN g2, g3",  //$NON-NLS-1$
				 query);
	}

	/** SELECT * FROM g1 inner join g2 */
	public void testInvalidInnerJoin() {
		helpException("SELECT * FROM g1 inner join g2");		 //$NON-NLS-1$
	}

	/** SELECT * FROM (g1 cross join g2) cross join g3 */
	public void testMultiCrossJoin() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g1, g2, JoinType.JOIN_CROSS);
		JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(new GroupSymbol("g3")), JoinType.JOIN_CROSS);		 //$NON-NLS-1$
		From from = new From();
		from.addClause(jp2);	

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM (g1 cross join g2) cross join g3",  //$NON-NLS-1$
				 "SELECT * FROM (g1 CROSS JOIN g2) CROSS JOIN g3",  //$NON-NLS-1$
				 query);
	}

	/** SELECT * FROM (g1 cross join g2) cross join (g3 cross join g4) */
	public void testMultiCrossJoin2() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g1, g2, JoinType.JOIN_CROSS);
		UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("g3")); //$NON-NLS-1$
		UnaryFromClause g4 = new UnaryFromClause(new GroupSymbol("g4"));		 //$NON-NLS-1$
		JoinPredicate jp2 = new JoinPredicate(g3, g4, JoinType.JOIN_CROSS);
		JoinPredicate jp3 = new JoinPredicate(jp, jp2, JoinType.JOIN_CROSS);		
		From from = new From();
		from.addClause(jp3);	

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM (g1 cross join g2) cross join (g3 cross join g4)",  //$NON-NLS-1$
				 "SELECT * FROM (g1 CROSS JOIN g2) CROSS JOIN (g3 CROSS JOIN g4)",  //$NON-NLS-1$
				 query);
	}

	/** SELECT * FROM g1 cross join (g2 cross join g3) */
	public void testMultiCrossJoin3() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("g3")); //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g2, g3, JoinType.JOIN_CROSS);
		JoinPredicate jp2 = new JoinPredicate(g1, jp, JoinType.JOIN_CROSS);
		From from = new From();
		from.addClause(jp2);	

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM g1 cross join (g2 cross join g3)",  //$NON-NLS-1$
				 "SELECT * FROM g1 CROSS JOIN (g2 CROSS JOIN g3)",  //$NON-NLS-1$
				 query);
	}

	/** SELECT * FROM g1 cross join (g2 cross join g3), g4 */
	public void testMixedJoin() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("g3")); //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g2, g3, JoinType.JOIN_CROSS);
		JoinPredicate jp2 = new JoinPredicate(g1, jp, JoinType.JOIN_CROSS);
		From from = new From();
		from.addClause(jp2);
		from.addClause(new UnaryFromClause(new GroupSymbol("g4")));	 //$NON-NLS-1$

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM g1 cross join (g2 cross join g3), g4",  //$NON-NLS-1$
				 "SELECT * FROM g1 CROSS JOIN (g2 CROSS JOIN g3), g4",  //$NON-NLS-1$
				 query);
	}

	/** SELECT * FROM g1 cross join (g2 cross join g3), g4, g5 cross join g6 */
	public void testMixedJoin2() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("g3")); //$NON-NLS-1$
		UnaryFromClause g4 = new UnaryFromClause(new GroupSymbol("g4")); //$NON-NLS-1$
		UnaryFromClause g5 = new UnaryFromClause(new GroupSymbol("g5")); //$NON-NLS-1$
		UnaryFromClause g6 = new UnaryFromClause(new GroupSymbol("g6"));		 //$NON-NLS-1$
		JoinPredicate jp = new JoinPredicate(g2, g3, JoinType.JOIN_CROSS);
		JoinPredicate jp2 = new JoinPredicate(g1, jp, JoinType.JOIN_CROSS);
		JoinPredicate jp3 = new JoinPredicate(g5, g6, JoinType.JOIN_CROSS);
		From from = new From();
		from.addClause(jp2);
		from.addClause(g4);
		from.addClause(jp3);	

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT * FROM g1 cross join (g2 cross join g3), g4, g5 cross join g6",  //$NON-NLS-1$
				 "SELECT * FROM g1 CROSS JOIN (g2 CROSS JOIN g3), g4, g5 CROSS JOIN g6",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT * FROM g1, g2 inner join g3 on g2.a=g3.a */
	public void testMixedJoin3() {
		UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("g1")); //$NON-NLS-1$
		UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("g2"));		 //$NON-NLS-1$
		UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("g3")); //$NON-NLS-1$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("g2.a"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("g3.a")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g2, g3, JoinType.JOIN_INNER, crits);
		From from = new From();
		from.addClause(g1);
		from.addClause(jp);

		AllSymbol all = new AllSymbol();
		Select select = new Select();
		select.addSymbol(all);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT * FROM g1, g2 inner join g3 on g2.a=g3.a",  //$NON-NLS-1$
				 "SELECT * FROM g1, g2 INNER JOIN g3 ON g2.a = g3.a",  //$NON-NLS-1$
				 query);
	}
	
	/** Select myG.a myA, myH.b from g myG right outer join h myH on myG.x=myH.x */
	public void testRightOuterJoinWithAliases() {
		UnaryFromClause g = new UnaryFromClause(new GroupSymbol("myG", "g")); //$NON-NLS-1$ //$NON-NLS-2$
		UnaryFromClause h = new UnaryFromClause(new GroupSymbol("myH", "h"));		 //$NON-NLS-1$ //$NON-NLS-2$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("myG.x"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("myH.x")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g, h, JoinType.JOIN_RIGHT_OUTER, crits);
		From from = new From();
		from.addClause(jp);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("myG.a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("Select myG.a myA, myH.b from g myG right outer join h myH on myG.x=myH.x",  //$NON-NLS-1$
				 "SELECT myG.a AS myA, myH.b FROM g AS myG RIGHT OUTER JOIN h AS myH ON myG.x = myH.x",  //$NON-NLS-1$
				 query);
	}
	
	/** Select myG.x myX, myH.y from g myG right join h myH on myG.x=myH.x */
	public void testRightJoinWithAliases() {
		UnaryFromClause g = new UnaryFromClause(new GroupSymbol("myG", "g")); //$NON-NLS-1$ //$NON-NLS-2$
		UnaryFromClause h = new UnaryFromClause(new GroupSymbol("myH", "h"));		 //$NON-NLS-1$ //$NON-NLS-2$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("myG.x"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("myH.x")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g, h, JoinType.JOIN_RIGHT_OUTER, crits);
		From from = new From();
		from.addClause(jp);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("myG.a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("Select myG.a myA, myH.b from g myG right join h myH on myG.x=myH.x",  //$NON-NLS-1$
				 "SELECT myG.a AS myA, myH.b FROM g AS myG RIGHT OUTER JOIN h AS myH ON myG.x = myH.x",  //$NON-NLS-1$
				 query);
	}
	
	/** Select myG.a myA, myH.b from g myG left outer join h myH on myG.x=myH.x */
	public void testLeftOuterJoinWithAliases() {
		UnaryFromClause g = new UnaryFromClause(new GroupSymbol("myG", "g")); //$NON-NLS-1$ //$NON-NLS-2$
		UnaryFromClause h = new UnaryFromClause(new GroupSymbol("myH", "h"));		 //$NON-NLS-1$ //$NON-NLS-2$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("myG.x"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("myH.x")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g, h, JoinType.JOIN_LEFT_OUTER, crits);
		From from = new From();
		from.addClause(jp);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("myG.a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("Select myG.a myA, myH.b from g myG left outer join h myH on myG.x=myH.x",  //$NON-NLS-1$
				 "SELECT myG.a AS myA, myH.b FROM g AS myG LEFT OUTER JOIN h AS myH ON myG.x = myH.x",  //$NON-NLS-1$
				 query);
	}	

	/** Select myG.a myA, myH.b from g myG left join h myH on myG.x=myH.x */
	public void testLeftJoinWithAliases() {
		UnaryFromClause g = new UnaryFromClause(new GroupSymbol("myG", "g")); //$NON-NLS-1$ //$NON-NLS-2$
		UnaryFromClause h = new UnaryFromClause(new GroupSymbol("myH", "h"));		 //$NON-NLS-1$ //$NON-NLS-2$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("myG.x"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("myH.x")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g, h, JoinType.JOIN_LEFT_OUTER, crits);
		From from = new From();
		from.addClause(jp);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("myG.a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("Select myG.a myA, myH.b from g myG left join h myH on myG.x=myH.x",  //$NON-NLS-1$
				 "SELECT myG.a AS myA, myH.b FROM g AS myG LEFT OUTER JOIN h AS myH ON myG.x = myH.x",  //$NON-NLS-1$
				 query);
	}	
	
	/** Select myG.a myA, myH.b from g myG full outer join h myH on myG.x=myH.x */
	public void testFullOuterJoinWithAliases() {
		UnaryFromClause g = new UnaryFromClause(new GroupSymbol("myG", "g")); //$NON-NLS-1$ //$NON-NLS-2$
		UnaryFromClause h = new UnaryFromClause(new GroupSymbol("myH", "h"));		 //$NON-NLS-1$ //$NON-NLS-2$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("myG.x"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("myH.x")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g, h, JoinType.JOIN_FULL_OUTER, crits);
		From from = new From();
		from.addClause(jp);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("myG.a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("Select myG.a myA, myH.b from g myG full outer join h myH on myG.x=myH.x",  //$NON-NLS-1$
				 "SELECT myG.a AS myA, myH.b FROM g AS myG FULL OUTER JOIN h AS myH ON myG.x = myH.x",  //$NON-NLS-1$
				 query);
	}
	
	/** Select g.a, h.b from g full join h on g.x=h.x */
	public void testFullJoin() {
		UnaryFromClause g = new UnaryFromClause(new GroupSymbol("g")); //$NON-NLS-1$
		UnaryFromClause h = new UnaryFromClause(new GroupSymbol("h"));		 //$NON-NLS-1$
		CompareCriteria jcrit = new CompareCriteria(
			new ElementSymbol("g.x"), //$NON-NLS-1$
			CompareCriteria.EQ,
			new ElementSymbol("h.x")); //$NON-NLS-1$
		ArrayList crits = new ArrayList();
		crits.add(jcrit);		
		JoinPredicate jp = new JoinPredicate(g, h, JoinType.JOIN_FULL_OUTER, crits);
		From from = new From();
		from.addClause(jp);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("g.a")); //$NON-NLS-1$
		select.addSymbol(new ElementSymbol("h.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("Select g.a, h.b from g full join h on g.x=h.x",  //$NON-NLS-1$
				 "SELECT g.a, h.b FROM g FULL OUTER JOIN h ON g.x = h.x",  //$NON-NLS-1$
				 query);
	}
	
    // ======================= Convert ==============================================

	/** SELECT CONVERT(a, string) FROM g */
	public void testConversionFunction() {
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Function f = new Function("CONVERT", new Expression[] {new ElementSymbol("a", false), new Constant("string")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT CONVERT(a, string) FROM g",  //$NON-NLS-1$
				 "SELECT CONVERT(a, string) FROM g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT CONVERT(CONVERT(a, timestamp), string) FROM g */
	public void testConversionFunction2() {
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Function f = new Function("CONVERT", new Expression[] {new ElementSymbol("a", false), new Constant("timestamp")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		Function f2 = new Function("CONVERT", new Expression[] {f, new Constant("string")}); //$NON-NLS-1$ //$NON-NLS-2$
		ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT CONVERT(CONVERT(a, timestamp), string) FROM g",  //$NON-NLS-1$
				 "SELECT CONVERT(CONVERT(a, timestamp), string) FROM g",  //$NON-NLS-1$
				 query);
	}

    // ======================= Functions ==============================================

	/** SELECT 5 + length(concat(a, 'x')) FROM g */
	public void testMultiFunction() {
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Function f = new Function("concat", new Expression[] {new ElementSymbol("a", false), new Constant("x")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		Function f2 = new Function("length", new Expression[] {f}); //$NON-NLS-1$
		Function f3 = new Function("+", new Expression[] {new Constant(new Integer(5)), f2}); //$NON-NLS-1$
		ExpressionSymbol es = new ExpressionSymbol("expr", f3); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT 5 + length(concat(a, 'x')) FROM g",  //$NON-NLS-1$
				 "SELECT (5 + length(concat(a, 'x'))) FROM g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT REPLACE(a, 'x', 'y') AS y FROM g */
	public void testAliasedFunction() {
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Function f = new Function("replace", new Expression[] {new ElementSymbol("a", false), new Constant("x"), new Constant("y")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		ExpressionSymbol es = new ExpressionSymbol("y", f); //$NON-NLS-1$
		AliasSymbol as = new AliasSymbol("y", es); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(as);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT REPLACE(a, 'x', 'y') AS y FROM g",  //$NON-NLS-1$
				 "SELECT REPLACE(a, 'x', 'y') AS y FROM g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT cast(a as string) FROM g */
	public void testCastFunction() {
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Function f = new Function("cast", new Expression[] {new ElementSymbol("a", false), new Constant("string")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT cast(a as string) FROM g",  //$NON-NLS-1$
				 "SELECT cast(a AS string) FROM g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT cast(cast(a as timestamp) as string) FROM g */
	public void testMultiCastFunction() {
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Function f = new Function("cast", new Expression[] {new ElementSymbol("a", false), new Constant("timestamp")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		Function f2 = new Function("cast", new Expression[] {f, new Constant("string")}); //$NON-NLS-1$ //$NON-NLS-2$
		ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT cast(cast(a as timestamp) as string) FROM g",  //$NON-NLS-1$
				 "SELECT cast(cast(a AS timestamp) AS string) FROM g",  //$NON-NLS-1$
				 query);
	}

    /** SELECT left(fullname, 3) as x FROM system.groups */
    public void testLeftFunction() {
        GroupSymbol g = new GroupSymbol("system.groups"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("left", new Expression[] {new ElementSymbol("fullname", false), new Constant(new Integer(3))}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT left(fullname, 3) as x FROM system.groups",  //$NON-NLS-1$
                 "SELECT left(fullname, 3) AS x FROM system.groups",  //$NON-NLS-1$
                 query);
    }

    /** SELECT right(fullname, 3) as x FROM system.groups */
    public void testRightFunction() {
        GroupSymbol g = new GroupSymbol("system.groups"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("right", new Expression[] {new ElementSymbol("fullname", false), new Constant(new Integer(3))}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT right(fullname, 3) as x FROM system.groups",  //$NON-NLS-1$
                 "SELECT right(fullname, 3) AS x FROM system.groups",  //$NON-NLS-1$
                 query);
    }

    /** SELECT char('x') AS x FROM system.groups */
    public void testCharFunction() {
        GroupSymbol g = new GroupSymbol("system.groups"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("char", new Expression[] { new Constant("x")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT char('x') AS x FROM system.groups",  //$NON-NLS-1$
                 "SELECT char('x') AS x FROM system.groups",  //$NON-NLS-1$
                 query);
    }

    /** SELECT insert('x', 1, 'a') as x FROM system.groups */
    public void testInsertFunction() {
        GroupSymbol g = new GroupSymbol("system.groups"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("insert", new Expression[] { new Constant("x"), new Constant(new Integer(1)), new Constant("a")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT insert('x', 1, 'a') AS x FROM system.groups",  //$NON-NLS-1$
                 "SELECT insert('x', 1, 'a') AS x FROM system.groups",  //$NON-NLS-1$
                 query);
    }


    
    public void testInsertIntoSelect() {
        GroupSymbol g = new GroupSymbol("system.groups"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Insert insert = new Insert();
        GroupSymbol groupSymbol = new GroupSymbol( "tempA" );   //$NON-NLS-1$
        insert.setGroup(groupSymbol);
        
        Select select = new Select();
//        select.addSymbol( new ExpressionSymbol( new Constant( new Integer(1) ) ) );
        select.addSymbol(  new ExpressionSymbol( "exp", new Constant( new Integer(1) ) ) );    //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        
        insert.setQueryExpression( query );        

        helpTest("insert into tempA SELECT 1",  //$NON-NLS-1$
                 "INSERT INTO tempA SELECT 1",  //$NON-NLS-1$
                 insert);
    }

    /** SELECT translate('x', 'x', 'y') FROM system.groups */
    public void testTranslateFunction() {
        GroupSymbol g = new GroupSymbol("system.groups"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("translate", new Expression[] { new Constant("x"), new Constant("x"), new Constant("y")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT translate('x', 'x', 'y') FROM system.groups",  //$NON-NLS-1$
                 "SELECT translate('x', 'x', 'y') FROM system.groups",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_FRAC_SECOND, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionFracSecond() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_FRAC_SECOND"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_FRAC_SECOND, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_FRAC_SECOND, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_SECOND, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionSecond() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_SECOND"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_SECOND, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_SECOND, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_MINUTE, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionMinute() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_MINUTE"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_MINUTE, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_MINUTE, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_HOUR, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionHour() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_HOUR"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_HOUR, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_HOUR, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_DAY, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionDay() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_DAY"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_DAY, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_DAY, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_WEEK, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionWeek() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_WEEK"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_WEEK, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_WEEK, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_QUARTER, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionQuarter() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_QUARTER"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_QUARTER, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_QUARTER, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampadd(SQL_TSI_YEAR, 10, '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampaddFunctionYear() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampadd", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_YEAR"), new Constant(new Integer(10)), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampadd(SQL_TSI_YEAR, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampadd(SQL_TSI_YEAR, 10, '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }

    /** SELECT timestampdiff(SQL_TSI_FRAC_SECOND, '2003-05-01 10:20:10', '2003-05-01 10:20:30') as x FROM my.group1 */
    public void testTimestampdiffFunctionFracSecond() {
        GroupSymbol g = new GroupSymbol("my.group1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("timestampdiff", new Expression[] { //$NON-NLS-1$
            new Constant("SQL_TSI_FRAC_SECOND"), new Constant("2003-05-01 10:20:10"), new Constant("2003-05-01 10:20:30")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol as = new AliasSymbol("x", es); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(as);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT timestampdiff(SQL_TSI_FRAC_SECOND, '2003-05-01 10:20:10', '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 "SELECT timestampdiff(SQL_TSI_FRAC_SECOND, '2003-05-01 10:20:10', '2003-05-01 10:20:30') AS x FROM my.group1",  //$NON-NLS-1$
                 query);
    }
    
    /** SELECT 5 + 2 + 3 FROM g */
    public void testArithmeticOperatorPrecedence1() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("+", new Expression[] {new Constant(new Integer(5)), new Constant(new Integer(2))}); //$NON-NLS-1$
        Function f2 = new Function("+", new Expression[] {f, new Constant(new Integer(3))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 + 2 + 3 FROM g",  //$NON-NLS-1$
                 "SELECT ((5 + 2) + 3) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 5 + 2 - 3 FROM g */
    public void testArithmeticOperatorPrecedence2() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("+", new Expression[] {new Constant(new Integer(5)), new Constant(new Integer(2))}); //$NON-NLS-1$
        Function f2 = new Function("-", new Expression[] {f, new Constant(new Integer(3))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 + 2 - 3 FROM g",  //$NON-NLS-1$
                 "SELECT ((5 + 2) - 3) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 5 + 2 * 3 FROM g */
    public void testArithmeticOperatorPrecedence3() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("*", new Expression[] {new Constant(new Integer(2)), new Constant(new Integer(3))}); //$NON-NLS-1$
        Function f2 = new Function("+", new Expression[] {new Constant(new Integer(5)), f}); //$NON-NLS-1$
        
        ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 + 2 * 3 FROM g",  //$NON-NLS-1$
                 "SELECT (5 + (2 * 3)) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 5 * 2 + 3 FROM g */
    public void testArithmeticOperatorPrecedence4() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("*", new Expression[] {new Constant(new Integer(5)), new Constant(new Integer(2))}); //$NON-NLS-1$
        Function f2 = new Function("+", new Expression[] {f, new Constant(new Integer(3))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 * 2 + 3 FROM g",  //$NON-NLS-1$
                 "SELECT ((5 * 2) + 3) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 5 * 2 * 3 FROM g */
    public void testArithmeticOperatorPrecedence5() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("*", new Expression[] {new Constant(new Integer(5)), new Constant(new Integer(2))}); //$NON-NLS-1$
        Function f2 = new Function("*", new Expression[] {f, new Constant(new Integer(3))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 * 2 * 3 FROM g",  //$NON-NLS-1$
                 "SELECT ((5 * 2) * 3) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 1 + 2 * 3 + 4 * 5 FROM g */
    public void testArithmeticOperatorPrecedenceMixed1() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("*", new Expression[] {new Constant(new Integer(2)), new Constant(new Integer(3))}); //$NON-NLS-1$
        Function f2 = new Function("*", new Expression[] {new Constant(new Integer(4)), new Constant(new Integer(5))}); //$NON-NLS-1$
        Function f3 = new Function("+", new Expression[] {new Constant(new Integer(1)), f}); //$NON-NLS-1$
        Function f4 = new Function("+", new Expression[] {f3, f2}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f4); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 1 + 2 * 3 + 4 * 5 FROM g",  //$NON-NLS-1$
                 "SELECT ((1 + (2 * 3)) + (4 * 5)) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 1 * 2 + 3 * 4 + 5 FROM g */
    public void testArithmeticOperatorPrecedenceMixed2() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("*", new Expression[] {new Constant(new Integer(1)), new Constant(new Integer(2))}); //$NON-NLS-1$
        Function f2 = new Function("*", new Expression[] {new Constant(new Integer(3)), new Constant(new Integer(4))}); //$NON-NLS-1$
        Function f3 = new Function("+", new Expression[] {f, f2}); //$NON-NLS-1$
        Function f4 = new Function("+", new Expression[] {f3, new Constant(new Integer(5))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f4); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 1 * 2 + 3 * 4 + 5 FROM g",  //$NON-NLS-1$
                 "SELECT (((1 * 2) + (3 * 4)) + 5) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 5 - 4 - 3 - 2 FROM g --> SELECT ((5 - 4) - 3) - 2 FROM g */
    public void testLeftAssociativeExpressions1() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("-", new Expression[] {new Constant(new Integer(5)), new Constant(new Integer(4))}); //$NON-NLS-1$
        Function f2 = new Function("-", new Expression[] {f, new Constant(new Integer(3))}); //$NON-NLS-1$
        Function f3 = new Function("-", new Expression[] {f2, new Constant(new Integer(2))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f3); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 - 4 - 3 - 2 FROM g",  //$NON-NLS-1$
                 "SELECT (((5 - 4) - 3) - 2) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 5 / 4 / 3 / 2 FROM g --> SELECT ((5 / 4) / 3) / 2 FROM g */
    public void testLeftAssociativeExpressions2() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("/", new Expression[] {new Constant(new Integer(5)), new Constant(new Integer(4))}); //$NON-NLS-1$
        Function f2 = new Function("/", new Expression[] {f, new Constant(new Integer(3))}); //$NON-NLS-1$
        Function f3 = new Function("/", new Expression[] {f2, new Constant(new Integer(2))}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f3); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 5 / 4 / 3 / 2 FROM g",  //$NON-NLS-1$
                 "SELECT (((5 / 4) / 3) / 2) FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 'a' || 'b' || 'c' FROM g */
    public void testConcatOperator1() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("||", new Expression[] {new Constant("a"), new Constant("b")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Function f2 = new Function("||", new Expression[] {f, new Constant("c")}); //$NON-NLS-1$ //$NON-NLS-2$
        ExpressionSymbol es = new ExpressionSymbol("expr", f2); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 'a' || 'b' || 'c' FROM g",  //$NON-NLS-1$
                 "SELECT (('a' || 'b') || 'c') FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT 2 + 3 || 5 + 1 * 2 FROM g */
    public void testMixedOperators1() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Function f = new Function("*", new Expression[] {new Constant(new Integer(1)), new Constant(new Integer(2))}); //$NON-NLS-1$
        Function f2 = new Function("+", new Expression[] {new Constant(new Integer(5)), f}); //$NON-NLS-1$
        Function f3 = new Function("+", new Expression[] {new Constant(new Integer(2)), new Constant(new Integer(3))}); //$NON-NLS-1$
        Function f4 = new Function("||", new Expression[] {f3, f2}); //$NON-NLS-1$
        ExpressionSymbol es = new ExpressionSymbol("expr", f4); //$NON-NLS-1$
        Select select = new Select();
        select.addSymbol(es);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT 2 + 3 || 5 + 1 * 2 FROM g",  //$NON-NLS-1$
                 "SELECT ((2 + 3) || (5 + (1 * 2))) FROM g",  //$NON-NLS-1$
                 query);
    }

    // ======================= Group By ==============================================
	
	/** SELECT a FROM m.g GROUP BY b, c */
	public void testGroupBy() {
		GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("a", false)); //$NON-NLS-1$
		
		GroupBy groupBy = new GroupBy();
		groupBy.addSymbol(new ElementSymbol("b", false));		 //$NON-NLS-1$
		groupBy.addSymbol(new ElementSymbol("c", false)); //$NON-NLS-1$
		

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setGroupBy(groupBy);
		helpTest("SELECT a FROM m.g GROUP BY b, c",  //$NON-NLS-1$
				 "SELECT a FROM m.g GROUP BY b, c",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a FROM m.g GROUP BY b, c HAVING b=5*/
	public void testGroupByHaving() {
		GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("a", false)); //$NON-NLS-1$
		
		GroupBy groupBy = new GroupBy();
		groupBy.addSymbol(new ElementSymbol("b", false));		 //$NON-NLS-1$
		groupBy.addSymbol(new ElementSymbol("c", false)); //$NON-NLS-1$
		
		CompareCriteria having = new CompareCriteria(new ElementSymbol("b", false), CompareCriteria.EQ, new Constant(new Integer(5))); //$NON-NLS-1$
		
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setGroupBy(groupBy);
		query.setHaving(having);
		helpTest("SELECT a FROM m.g GROUP BY b, c HAVING b=5",  //$NON-NLS-1$
				 "SELECT a FROM m.g GROUP BY b, c HAVING b = 5",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT COUNT(a) AS c FROM m.g */
	public void testAggregateFunction() {
		GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new AliasSymbol("c",  //$NON-NLS-1$
			new AggregateSymbol("c", "COUNT", false, new ElementSymbol("a", false)))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
				
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT COUNT(a) AS c FROM m.g",  //$NON-NLS-1$
				 "SELECT COUNT(a) AS c FROM m.g",  //$NON-NLS-1$
				 query);
	}

    /** SELECT (COUNT(a)) AS c FROM m.g - this kind of query is generated by ODBC sometimes */
    public void testAggregateFunctionWithParens() {
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new AliasSymbol("c",  //$NON-NLS-1$
            new AggregateSymbol("c", "COUNT", false, new ElementSymbol("a", false)))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
                
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT (COUNT(a)) AS c FROM m.g",  //$NON-NLS-1$
                 "SELECT COUNT(a) AS c FROM m.g",  //$NON-NLS-1$
                 query);
    }

	/** SELECT a FROM m.g GROUP BY a HAVING COUNT(b) > 0*/
	public void testHavingFunction() {
		GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
		
		GroupBy groupBy = new GroupBy();
		groupBy.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
		
		Criteria having = new CompareCriteria(
			new AggregateSymbol("count", "COUNT", false, new ElementSymbol("b", false)), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			CompareCriteria.GT,
			new Constant(new Integer(0)) );
				
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setGroupBy(groupBy);
		query.setHaving(having);
		
		helpTest("SELECT a FROM m.g GROUP BY a HAVING COUNT(b) > 0",  //$NON-NLS-1$
				 "SELECT a FROM m.g GROUP BY a HAVING COUNT(b) > 0",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a FROM m.g GROUP BY a, b HAVING COUNT(b) > 0 AND b+5 > 0 */
	public void testCompoundHaving() {
		GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
		
		GroupBy groupBy = new GroupBy();
		groupBy.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
		groupBy.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$
		
		CompoundCriteria having = new CompoundCriteria();
		having.setOperator(CompoundCriteria.AND);
		having.addCriteria(new CompareCriteria(
			new AggregateSymbol("count", "COUNT", false, new ElementSymbol("b", false)), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			CompareCriteria.GT,
			new Constant(new Integer(0)) ));
		having.addCriteria(new CompareCriteria(
			new Function("+", new Expression[] { new ElementSymbol("b", false), new Constant(new Integer(5)) }), //$NON-NLS-1$ //$NON-NLS-2$
			CompareCriteria.GT,
			new Constant(new Integer(0)) ));
				
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setGroupBy(groupBy);
		query.setHaving(having);
		
		helpTest("SELECT a FROM m.g GROUP BY a, b HAVING COUNT(b) > 0 AND b+5 > 0",  //$NON-NLS-1$
				 "SELECT a FROM m.g GROUP BY a, b HAVING (COUNT(b) > 0) AND ((b + 5) > 0)",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a FROM m.g GROUP BY a, b HAVING COUNT(AVG(b)) */
	public void testFailNestedAggregateInHaving() {
		helpException("SELECT a FROM m.g GROUP BY a, b HAVING COUNT(b) AS x = 5");		 //$NON-NLS-1$
	}

	/** SELECT a FROM m.g GROUP BY a, b AS x */
	public void testFailAliasInHaving() {
		helpException("SELECT a FROM m.g GROUP BY a, b AS x");		 //$NON-NLS-1$
	}
 
	/** SELECT a FROM m.g GROUP BY count(a) */
	public void testFailAggregateInGroupBy() {
		helpException("SELECT a FROM m.g GROUP BY count(a)");		 //$NON-NLS-1$
	}
	
	
	public void testExceptionLength() {
        String sql = "SELECT * FROM Customer where Customer.Name = (select lastname from CUSTOMER where acctid = 9"; ////$NON-NLS-1$
        try {
            QueryParser.getQueryParser().parseCommand(sql);
            fail("Expected exception for parsing " + sql); //$NON-NLS-1$
        } catch (MetaMatrixException e) {
            //e.printStackTrace();
            //if (e.getMessage().length() > 1000) {
            //    fail("Expected max length of message 1000; but received  " + e.getMessage().length()); //$NON-NLS-1$
            //}
        }
    }

	
    
    public void testFunctionOfAggregates() {
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        AggregateSymbol agg1 = new AggregateSymbol("count", "COUNT", false, new ElementSymbol("a", false)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        AggregateSymbol agg2 = new AggregateSymbol("sum", "SUM", false, new ElementSymbol("a", false)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Function f = new Function("*", new Expression[] { agg1, agg2 }); //$NON-NLS-1$
        ExpressionSymbol expr = new ExpressionSymbol("expr", f); //$NON-NLS-1$
        AliasSymbol alias = new AliasSymbol("c", expr); //$NON-NLS-1$
        select.addSymbol(alias);        
                
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT COUNT(a) * SUM(a) AS c FROM m.g",  //$NON-NLS-1$
                 "SELECT (COUNT(a) * SUM(a)) AS c FROM m.g",  //$NON-NLS-1$
                 query);
        
    }
	
	/** SELECT 5-null, a.g1.c1 FROM a.g1 */
	public void testArithmeticNullFunction() { 
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol(
			"expr",  //$NON-NLS-1$
			new Function("-", new Expression[] { new Constant(new Integer(5)), new Constant(null) }) ) ); //$NON-NLS-1$
		select.addSymbol(new ElementSymbol("a.g1.c1")); //$NON-NLS-1$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 5-null, a.g1.c1 FROM a.g1",  //$NON-NLS-1$
				 "SELECT (5 - null), a.g1.c1 FROM a.g1",  //$NON-NLS-1$
				 query);
	}

	/** SELECT 'abc' FROM a.g1 */
	public void testStringLiteral() { 
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant("abc"))); //$NON-NLS-1$ //$NON-NLS-2$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 'abc' FROM a.g1",  //$NON-NLS-1$
				 "SELECT 'abc' FROM a.g1",  //$NON-NLS-1$
				 query);
	}


	/** SELECT 'O''Leary' FROM a.g1 */
	public void testStringLiteralEscapedTick() { 
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant("O'Leary"))); //$NON-NLS-1$ //$NON-NLS-2$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 'O''Leary' FROM a.g1",  //$NON-NLS-1$
				 "SELECT 'O''Leary' FROM a.g1",  //$NON-NLS-1$
				 query);
	}

	/** SELECT '''abc''' FROM a.g1 */
	public void testStringLiteralEscapedTick2() { 
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant("'abc'"))); //$NON-NLS-1$ //$NON-NLS-2$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT '''abc''' FROM a.g1",  //$NON-NLS-1$
				 "SELECT '''abc''' FROM a.g1",  //$NON-NLS-1$
				 query);
	}

	/** SELECT 'a''b''c' FROM a.g1 */
	public void testStringLiteralEscapedTick3() { 
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant("a'b'c"))); //$NON-NLS-1$ //$NON-NLS-2$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 'a''b''c' FROM a.g1",  //$NON-NLS-1$
				 "SELECT 'a''b''c' FROM a.g1",  //$NON-NLS-1$
				 query);
	}

	/** SELECT " "" " FROM a.g1 */
	public void testStringLiteralEscapedTick4() { 
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant(" \" "))); //$NON-NLS-1$ //$NON-NLS-2$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT \" \"\" \" FROM a.g1",  //$NON-NLS-1$
				 "SELECT ' \" ' FROM a.g1",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT 123456789012 FROM a.g1 */
	public void testLongLiteral() {
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant(new Long(123456789012L)))); //$NON-NLS-1$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 123456789012 FROM a.g1",  //$NON-NLS-1$
				 "SELECT 123456789012 FROM a.g1",  //$NON-NLS-1$
				 query);		    
	}
	
	/** SELECT 1000000000000000000000000 FROM a.g1 */
	public void testBigIntegerLiteral() {
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant(new BigInteger("1000000000000000000000000")))); //$NON-NLS-1$ //$NON-NLS-2$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 1000000000000000000000000 FROM a.g1",  //$NON-NLS-1$
				 "SELECT 1000000000000000000000000 FROM a.g1",  //$NON-NLS-1$
				 query);		    
	}
	
	/** SELECT 1.3e8 FROM a.g1 */
	public void testFloatWithE() {
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant(new Double(1.3e8)))); //$NON-NLS-1$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT 1.3e8 FROM a.g1",  //$NON-NLS-1$
				 "SELECT 1.3E8 FROM a.g1",  //$NON-NLS-1$
				 query);		    
	}	
	
	/** SELECT -1.3e-6 FROM a.g1 */
	public void testFloatWithMinusE() {
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant(new Double(-1.3e-6)))); //$NON-NLS-1$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT -1.3e-6 FROM a.g1",  //$NON-NLS-1$
				 "SELECT -1.3E-6 FROM a.g1",  //$NON-NLS-1$
				 query);		    
	}	
	
	/** SELECT -1.3e+8 FROM a.g1 */
	public void testFloatWithPlusE() {
		GroupSymbol g = new GroupSymbol("a.g1"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", new Constant(new Double(-1.3e+8)))); //$NON-NLS-1$
						
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT -1.3e+8 FROM a.g1",  //$NON-NLS-1$
				 "SELECT -1.3E8 FROM a.g1",  //$NON-NLS-1$
				 query);		    
	}	

    /** SELECT {d'2002-10-02'} FROM m.g1 */
    public void testDateLiteral1() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(java.sql.Date.valueOf("2002-10-02")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {d'2002-10-02'} FROM m.g1",  //$NON-NLS-1$
                 "SELECT {d'2002-10-02'} FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }

    /** SELECT {d'2002-9-1'} FROM m.g1 */
    public void testDateLiteral2() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(java.sql.Date.valueOf("2002-09-01")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {d'2002-9-1'} FROM m.g1",  //$NON-NLS-1$
                 "SELECT {d'2002-09-01'} FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }

    /** SELECT {d'bad'} FROM m.g1 */
    public void testDateLiteralFail() {
        helpException("SELECT {d'bad'} FROM m.g1"); //$NON-NLS-1$
    }
        
    /** SELECT {t '11:10:00' } FROM m.g1 */
    public void testTimeLiteral1() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(java.sql.Time.valueOf("11:10:00")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {t '11:10:00' } FROM m.g1",  //$NON-NLS-1$
                 "SELECT {t'11:10:00'} FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }

    /** SELECT {t '5:10:00'} FROM m.g1 */
    public void testTimeLiteral2() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(java.sql.Time.valueOf("5:10:00")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {t '5:10:00'} FROM m.g1",  //$NON-NLS-1$
                 "SELECT {t'05:10:00'} FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }

    /** SELECT {t 'xyz'} FROM m.g1 */
    public void testTimeLiteralFail() {
        helpException("SELECT {t 'xyz'} FROM m.g1"); //$NON-NLS-1$
    }
    
    /** SELECT {ts'2002-10-02 19:00:02.50'} FROM m.g1 */
    public void testTimestampLiteral() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(java.sql.Timestamp.valueOf("2002-10-02 19:00:02.50")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {ts'2002-10-02 19:00:02.50'} FROM m.g1",  //$NON-NLS-1$
                 "SELECT {ts'2002-10-02 19:00:02.5'} FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }

    /** SELECT {b'true'} FROM m.g1 */
    public void testBooleanLiteralTrue() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(Boolean.valueOf("true")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {b'true'} FROM m.g1",  //$NON-NLS-1$
                 "SELECT TRUE FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }
    /** SELECT TRUE FROM m.g1 */
    public void testBooleanLiteralTrue2() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(Boolean.valueOf("true")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT TRUE FROM m.g1",  //$NON-NLS-1$
                 "SELECT TRUE FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }
  
    /** SELECT {b'false'} FROM m.g1 */
    public void testBooleanLiteralFalse() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(Boolean.valueOf("false")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT {b'false'} FROM m.g1",  //$NON-NLS-1$
                 "SELECT FALSE FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }
    
    /** SELECT FALSE FROM m.g1 */
    public void testBooleanLiteralFalse2() {
        GroupSymbol g = new GroupSymbol("m.g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ExpressionSymbol("expr", new Constant(Boolean.valueOf("false")))); //$NON-NLS-1$ //$NON-NLS-2$
                        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        helpTest("SELECT FALSE FROM m.g1",  //$NON-NLS-1$
                 "SELECT FALSE FROM m.g1",  //$NON-NLS-1$
                 query);                    
    }
    
    /** SELECT {b 'xyz'} FROM m.g1 */
    public void testBooleanLiteralFail() {
        helpException("SELECT {b 'xyz'} FROM m.g1"); //$NON-NLS-1$
    }    
    
	/** SELECT DISTINCT a FROM g */
	public void testSelectDistinct(){
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
		select.setDistinct(true);
		
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT DISTINCT a FROM g",  //$NON-NLS-1$
				 "SELECT DISTINCT a FROM g",  //$NON-NLS-1$
				 query);
	}

    /** SELECT ALL a FROM g */
    public void testSelectAll(){
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
        select.setDistinct(false);
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT ALL a FROM g",  //$NON-NLS-1$
                 "SELECT a FROM g",  //$NON-NLS-1$
                 query);
    }
	
    //=========================Aliasing==============================================

	/** SELECT a AS myA, b FROM g */
	public void testAliasInSelect(){
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT a AS myA, b FROM g",  //$NON-NLS-1$
				 "SELECT a AS myA, b FROM g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a myA, b FROM g, h */
	public void testAliasInSelect2(){
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		GroupSymbol h = new GroupSymbol("h"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);
		from.addGroup(h);

		AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("a")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		select.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT a myA, b FROM g, h",  //$NON-NLS-1$
				 "SELECT a AS myA, b FROM g, h",  //$NON-NLS-1$
				 query);	    
	}
	
	/** SELECT myG.a FROM g AS myG */
	public void testAliasInFrom(){
		GroupSymbol g = new GroupSymbol("myG", "g"); //$NON-NLS-1$ //$NON-NLS-2$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("myG.a")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT myG.a FROM g AS myG",  //$NON-NLS-1$
				 "SELECT myG.a FROM g AS myG",  //$NON-NLS-1$
				 query);
	}

	/** SELECT myG.*, myH.b FROM g AS myG, h AS myH */
	public void testAliasesInFrom(){
		GroupSymbol g = new GroupSymbol("myG", "g"); //$NON-NLS-1$ //$NON-NLS-2$
		GroupSymbol h = new GroupSymbol("myH", "h"); //$NON-NLS-1$ //$NON-NLS-2$
		From from = new From();
		from.addGroup(g);
		from.addGroup(h);

		Select select = new Select();
		AllInGroupSymbol myG = new AllInGroupSymbol("myG.*"); //$NON-NLS-1$
		select.addSymbol(myG);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT myG.*, myH.b FROM g AS myG, h AS myH",  //$NON-NLS-1$
				 "SELECT myG.*, myH.b FROM g AS myG, h AS myH",  //$NON-NLS-1$
				 query);	    
	}

	/** SELECT myG.a, myH.b FROM g myG, h myH */
	public void testHiddenAliasesInFrom(){
		GroupSymbol g = new GroupSymbol("myG", "g"); //$NON-NLS-1$ //$NON-NLS-2$
		GroupSymbol h = new GroupSymbol("myH", "h"); //$NON-NLS-1$ //$NON-NLS-2$
		From from = new From();
		from.addGroup(g);
		from.addGroup(h);

		Select select = new Select();
		AllInGroupSymbol myG = new AllInGroupSymbol("myG.*"); //$NON-NLS-1$
		select.addSymbol(myG);
		select.addSymbol(new ElementSymbol("myH.b")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT myG.*, myH.b FROM g myG, h myH",  //$NON-NLS-1$
				 "SELECT myG.*, myH.b FROM g AS myG, h AS myH",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a AS or FROM g */
	public void testAliasInSelectUsingKeywordFails(){
		helpException("SELECT a AS or FROM g");		 //$NON-NLS-1$
	}

	/** SELECT or.a FROM g AS or */
	public void testAliasInFromUsingKeywordFails(){
		helpException("SELECT or.a FROM g AS or");		 //$NON-NLS-1$
	}

    // ======================= Misc ==============================================

    /** Select a From db.g Where a IS NULL */
    public void testIsNullCriteria1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Criteria crit = new IsNullCriteria(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("Select a From db.g Where a IS NULL",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE a IS NULL",  //$NON-NLS-1$
                 query);
    }
    
    /** Select a From db.g Where a IS NOT NULL */
    public void testIsNullCriteria2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        IsNullCriteria crit = new IsNullCriteria(a);
        crit.setNegated(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("Select a From db.g Where a IS NOT NULL",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE a IS NOT NULL",  //$NON-NLS-1$
                 query);
    }
    
	/** Select a From db.g Where Not a IS NULL */
	public void testNotIsNullCriteria(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit = new NotCriteria(new IsNullCriteria(a));

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("Select a From db.g Where Not a IS NULL",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE NOT (a IS NULL)",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a from db.g where a <> "value" */
	public void testStringNotEqualDoubleTicks(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression constant = new Constant("value"); //$NON-NLS-1$
		Criteria crit = new CompareCriteria(a, CompareCriteria.NE, constant);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where a <> \"value\"",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a <> 'value'",  //$NON-NLS-1$
				 query);
	}

    /** SELECT a from db.g where a != "value" */
    public void testNotEquals2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Expression constant = new Constant("value"); //$NON-NLS-1$
        Criteria crit = new CompareCriteria(a, CompareCriteria.NE, constant);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a from db.g where a != \"value\"",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE a <> 'value'",  //$NON-NLS-1$
                 query);
    }

	/** SELECT a from db."g" where a = 5 */
	public void testPartlyQuotedGroup(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);
		
		Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, new Constant(new Integer(5)));

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.\"g\" where a = 5",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a = 5",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a from "db"."g" where a = 5 */
	public void testFullyQuotedGroup(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);
		
		Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, new Constant(new Integer(5)));

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from \"db\".\"g\" where a = 5",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a = 5",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT "db".g.a from db.g */
	public void testPartlyQuotedElement1(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("db.g.a");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT \"db\".g.a from db.g",  //$NON-NLS-1$
				 "SELECT db.g.a FROM db.g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT "db"."g".a from db.g */
	public void testPartlyQuotedElement2(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("db.g.a");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT \"db\".\"g\".a from db.g",  //$NON-NLS-1$
				 "SELECT db.g.a FROM db.g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT "db"."g"."a" from db.g */
	public void testPartlyQuotedElement3(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("db.g.a");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT \"db\".\"g\".\"a\" from db.g",  //$NON-NLS-1$
				 "SELECT db.g.a FROM db.g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT ""g"".""a" from db.g */
	public void testStringLiteralLikeQuotedElement(){
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ExpressionSymbol a = new ExpressionSymbol("expr", new Constant("g\".\"a"));  //$NON-NLS-1$ //$NON-NLS-2$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT \"g\"\".\"\"a\" from g",  //$NON-NLS-1$
				 "SELECT 'g\".\"a' FROM g",  //$NON-NLS-1$
				 query);
	}

	/** SELECT g.x AS "select" FROM g */
	public void testQuotedAlias(){
		GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		AliasSymbol a = new AliasSymbol("select", new ElementSymbol("g.x"));  //$NON-NLS-1$ //$NON-NLS-2$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT g.x AS \"select\" FROM g",  //$NON-NLS-1$
				 "SELECT g.x AS \"select\" FROM g",  //$NON-NLS-1$
				 query);
	}

    /** SELECT g.x AS year FROM g */
    public void testQuotedAlias2(){
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        AliasSymbol a = new AliasSymbol("year", new ElementSymbol("g.x"));  //$NON-NLS-1$ //$NON-NLS-2$
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        helpTest("SELECT g.x AS year FROM g",  //$NON-NLS-1$
                 "SELECT g.x AS year FROM g",  //$NON-NLS-1$
                 query);
    }
    
    /** SELECT g."select" FROM g */
    public void testReservedWordElement1(){
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("g.select");  //$NON-NLS-1$
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        helpTest("SELECT g.\"select\" FROM g",  //$NON-NLS-1$
                 "SELECT g.\"select\" FROM g",  //$NON-NLS-1$
                 query);
    }

    /** SELECT newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet.x FROM newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet */
    public void testReservedWordElement2() {
        GroupSymbol g = new GroupSymbol("newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet.x");  //$NON-NLS-1$
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        helpTest("SELECT newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet.x FROM newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet",  //$NON-NLS-1$
                 "SELECT newModel5.ResultSetDocument.MappingClasses.\"from\".\"from\".Query1InputSet.x FROM newModel5.ResultSetDocument.MappingClasses.\"from\".\"from\".Query1InputSet",  //$NON-NLS-1$
                 query);        
    }
    
    /** SELECT * FROM newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet  */
    public void testReservedWordGroup1(){
        GroupSymbol g = new GroupSymbol("newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new AllSymbol());

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        helpTest("SELECT * FROM newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet",  //$NON-NLS-1$
                 "SELECT * FROM newModel5.ResultSetDocument.MappingClasses.\"from\".\"from\".Query1InputSet",  //$NON-NLS-1$
                 query);
    }

    /** SELECT * FROM newModel5."ResultSetDocument.MappingClasses.from.from.Query1InputSet"  */
    public void testReservedWordGroup2(){
        GroupSymbol g = new GroupSymbol("newModel5.ResultSetDocument.MappingClasses.from.from.Query1InputSet"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new AllSymbol());

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        helpTest("SELECT * FROM newModel5.\"ResultSetDocument.MappingClasses.from.from.Query1InputSet\"",  //$NON-NLS-1$
                 "SELECT * FROM newModel5.ResultSetDocument.MappingClasses.\"from\".\"from\".Query1InputSet",  //$NON-NLS-1$
                 query);
    }    
    
    /** SELECT * FROM model.doc WHERE ab.cd.@ef = 'abc' */
    public void testXMLCriteriaWithAttribute() {
        GroupSymbol g = new GroupSymbol("model.doc"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new AllSymbol());

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        ElementSymbol elem = new ElementSymbol("ab.cd.@ef"); //$NON-NLS-1$
        query.setCriteria(new CompareCriteria(elem, CompareCriteria.EQ, new Constant("abc"))); //$NON-NLS-1$

        helpTest("SELECT * FROM model.doc WHERE ab.cd.@ef = 'abc'",  //$NON-NLS-1$
                 "SELECT * FROM model.doc WHERE ab.cd.@ef = 'abc'",  //$NON-NLS-1$
                 query);   
    }

    /** SELECT * FROM model.doc WHERE @ef = 'abc' */
    public void testXMLCriteriaWithUnqualifiedAttribute() {
        helpException("SELECT * FROM model.doc WHERE @ef = 'abc'");    //$NON-NLS-1$
    }

	/** SELECT a from db.g where a <> 'value' */
	public void testStringNotEqual(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression constant = new Constant("value"); //$NON-NLS-1$
		Criteria crit = new CompareCriteria(a, CompareCriteria.NE, constant);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where a <> 'value'",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a <> 'value'",  //$NON-NLS-1$
				 query);
	}	

    /** SELECT a from db.g where a BETWEEN 1000 AND 2000 */
    public void testBetween1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Expression constant1 = new Constant(new Integer(1000));
        Expression constant2 = new Constant(new Integer(2000));
        Criteria crit = new BetweenCriteria(a, constant1, constant2);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a from db.g where a BETWEEN 1000 AND 2000",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE a BETWEEN 1000 AND 2000",  //$NON-NLS-1$
                 query);
    }   
    
    /** SELECT a from db.g where a NOT BETWEEN 1000 AND 2000 */
    public void testBetween2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Expression constant1 = new Constant(new Integer(1000));
        Expression constant2 = new Constant(new Integer(2000));
        BetweenCriteria crit = new BetweenCriteria(a, constant1, constant2);
        crit.setNegated(true);
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a from db.g where a NOT BETWEEN 1000 AND 2000",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE a NOT BETWEEN 1000 AND 2000",  //$NON-NLS-1$
                 query);
    }   
    
	/** SELECT a from db.g where a < 1000 */
	public void testCompareLT(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression constant = new Constant(new Integer(1000));
		Criteria crit = new CompareCriteria(a, CompareCriteria.LT, constant);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where a < 1000",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a < 1000",  //$NON-NLS-1$
				 query);
	}	
	
	/** SELECT a from db.g where a > 1000 */
	public void testCompareGT(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression constant = new Constant(new Integer(1000));
		Criteria crit = new CompareCriteria(a, CompareCriteria.GT, constant);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where a > 1000",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a > 1000",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT a from db.g where a <= 1000 */
	public void testCompareLE(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression constant = new Constant(new Integer(1000));
		Criteria crit = new CompareCriteria(a, CompareCriteria.LE, constant);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where a <= 1000",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a <= 1000",  //$NON-NLS-1$
				 query);
	}		

	/** SELECT a from db.g where a >= 1000 */
	public void testCompareGE(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression constant = new Constant(new Integer(1000));
		Criteria crit = new CompareCriteria(a, CompareCriteria.GE, constant);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where a >= 1000",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE a >= 1000",  //$NON-NLS-1$
				 query);
	}

	/** SELECT a from db.g where b = x and a = 1000 */
	public void testCompoundCompare1(){
	    helpTestCompoundCompare("SELECT a from db.g where b = x and a = 1000"); //$NON-NLS-1$
	}

	/** SELECT a from db.g where (b = x and a = 1000) */
	public void testCompoundCompare2(){
	    helpTestCompoundCompare("SELECT a from db.g where (b = x and a = 1000)"); //$NON-NLS-1$
	}

	/** SELECT a from db.g where ((b = x) and (a = 1000)) */
	public void testCompoundCompare3(){
	    helpTestCompoundCompare("SELECT a from db.g where ((b = x) and (a = 1000))"); //$NON-NLS-1$
	}

	/** SELECT a from db.g where (((b = x) and (a = 1000))) */
	public void testCompoundCompare4(){
	    helpTestCompoundCompare("SELECT a from db.g where (((b = x) and (a = 1000)))"); //$NON-NLS-1$
	}

	/** SELECT a FROM db.g WHERE (b = x) AND (a = 1000) */
	private void helpTestCompoundCompare(String testSQL){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit1 = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
		Expression constant = new Constant(new Integer(1000));
		Criteria crit2 = new CompareCriteria(a, CompareCriteria.EQ, constant);
		Criteria crit = new CompoundCriteria(CompoundCriteria.AND, crit1, crit2);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest(testSQL, 
				 "SELECT a FROM db.g WHERE (b = x) AND (a = 1000)",  //$NON-NLS-1$
				 query);
	}
	
    /** SELECT a FROM db.g WHERE b IN (1000,5000)*/
    public void testSetCriteria0(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Expression constant1 = new Constant(new Integer(1000));
        Expression constant2 = new Constant(new Integer(5000));
        Collection constants = new ArrayList(2);
        constants.add(constant1);
        constants.add(constant2);
        Criteria crit = new SetCriteria(new ElementSymbol("b"), constants); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a FROM db.g WHERE b IN (1000,5000)",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE b IN (1000, 5000)",  //$NON-NLS-1$
                 query);
    }   
    
    /** SELECT a FROM db.g WHERE b NOT IN (1000,5000)*/
    public void testSetCriteria1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Expression constant1 = new Constant(new Integer(1000));
        Expression constant2 = new Constant(new Integer(5000));
        Collection constants = new ArrayList(2);
        constants.add(constant1);
        constants.add(constant2);
        SetCriteria crit = new SetCriteria(new ElementSymbol("b"), constants); //$NON-NLS-1$
        crit.setNegated(true);
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a FROM db.g WHERE b NOT IN (1000,5000)",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE b NOT IN (1000, 5000)",  //$NON-NLS-1$
                 query);
    }   
    
	// ================================== order by ==================================
	
	/** SELECT a FROM db.g WHERE b = aString order by c*/
	public void testOrderBy(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new ElementSymbol("aString")); //$NON-NLS-1$ //$NON-NLS-2$

		ArrayList elements = new ArrayList();
		elements.add(new ElementSymbol("c")); //$NON-NLS-1$
		OrderBy orderBy = new OrderBy(elements);

		Query query = new Query(select, from, crit, orderBy, null);
		helpTest("SELECT a FROM db.g WHERE b = aString ORDER BY c",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE b = aString ORDER BY c",  //$NON-NLS-1$
				 query);
	}	

	/** SELECT a FROM db.g WHERE b = aString order by c desc*/
	public void testOrderByDesc(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new ElementSymbol("aString")); //$NON-NLS-1$ //$NON-NLS-2$

		ArrayList elements = new ArrayList();
		elements.add(new ElementSymbol("c")); //$NON-NLS-1$
		ArrayList orderTypes = new ArrayList();
		orderTypes.add(Boolean.FALSE);
		OrderBy orderBy = new OrderBy(elements, orderTypes);

		Query query = new Query(select, from, crit, orderBy, null);
		helpTest("SELECT a FROM db.g WHERE b = aString ORDER BY c desc",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE b = aString ORDER BY c DESC",  //$NON-NLS-1$
				 query);
	}	

	/** SELECT a FROM db.g WHERE b = aString order by c,d*/
	public void testOrderBys(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new ElementSymbol("aString")); //$NON-NLS-1$ //$NON-NLS-2$

		ArrayList elements = new ArrayList();
		elements.add(new ElementSymbol("c")); //$NON-NLS-1$
		elements.add(new ElementSymbol("d")); //$NON-NLS-1$
		OrderBy orderBy = new OrderBy(elements);

		Query query = new Query(select, from, crit, orderBy, null);
		helpTest("SELECT a FROM db.g WHERE b = aString ORDER BY c,d",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE b = aString ORDER BY c, d",  //$NON-NLS-1$
				 query);
	}	

	/** SELECT a FROM db.g WHERE b = aString order by c desc,d desc*/
	public void testOrderBysDesc(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new ElementSymbol("aString")); //$NON-NLS-1$ //$NON-NLS-2$

		ArrayList elements = new ArrayList();
		elements.add(new ElementSymbol("c")); //$NON-NLS-1$
		elements.add(new ElementSymbol("d")); //$NON-NLS-1$
		ArrayList orderTypes = new ArrayList();
		orderTypes.add(Boolean.FALSE);
		orderTypes.add(Boolean.FALSE);
		OrderBy orderBy = new OrderBy(elements, orderTypes);

		Query query = new Query(select, from, crit, orderBy, null);
		helpTest("SELECT a FROM db.g WHERE b = aString ORDER BY c desc,d desc",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE b = aString ORDER BY c DESC, d DESC",  //$NON-NLS-1$
				 query);
	}
	
	/** SELECT a FROM db.g WHERE b = aString order by c desc,d*/
	public void testMixedOrderBys(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new ElementSymbol("aString")); //$NON-NLS-1$ //$NON-NLS-2$

		ArrayList elements = new ArrayList();
		elements.add(new ElementSymbol("c")); //$NON-NLS-1$
		elements.add(new ElementSymbol("d")); //$NON-NLS-1$
		ArrayList orderTypes = new ArrayList();
		orderTypes.add(Boolean.FALSE);
		orderTypes.add(Boolean.TRUE);
		OrderBy orderBy = new OrderBy(elements, orderTypes);

		Query query = new Query(select, from, crit, orderBy, null);
		helpTest("SELECT a FROM db.g WHERE b = aString ORDER BY c desc,d",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE b = aString ORDER BY c DESC, d",  //$NON-NLS-1$
				 query);
	}	

	// ================================== match ====================================

    /** SELECT a FROM db.g WHERE b LIKE 'aString'*/
    public void testLike0(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Expression string1 = new Constant("aString"); //$NON-NLS-1$
        Criteria crit = new MatchCriteria(new ElementSymbol("b"), string1); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a FROM db.g WHERE b LIKE 'aString'",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE b LIKE 'aString'",  //$NON-NLS-1$
                 query);
    }   

    /** SELECT a FROM db.g WHERE b NOT LIKE 'aString'*/
    public void testLike1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Expression string1 = new Constant("aString"); //$NON-NLS-1$
        MatchCriteria crit = new MatchCriteria(new ElementSymbol("b"), string1); //$NON-NLS-1$
        crit.setNegated(true);
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a FROM db.g WHERE b NOT LIKE 'aString'",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE b NOT LIKE 'aString'",  //$NON-NLS-1$
                 query);
    }   

	/** SELECT a from db.g where b like '#String' escape '#'*/
	public void testLikeWithEscape(){
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
		select.addSymbol(a);

		Expression string1 = new Constant("#String"); //$NON-NLS-1$
		Criteria crit = new MatchCriteria(new ElementSymbol("b"), string1, '#'); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		helpTest("SELECT a from db.g where b like '#String' escape '#'",  //$NON-NLS-1$
				 "SELECT a FROM db.g WHERE b LIKE '#String' ESCAPE '#'",  //$NON-NLS-1$
				 query);
	}
	
    public void testLikeWithEscapeException(){
        helpException("SELECT a from db.g where b like '#String' escape '#1'", "Parsing error: Like escape value must be a single character.");  //$NON-NLS-1$ //$NON-NLS-2$
    }   

    
	// ==================== modeler literals ===========================

	/** Select mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0 */
	public void testModelerID() {
		GroupSymbol g = new GroupSymbol("mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("Select mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
				 "SELECT mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 FROM mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
				 query);		
	}
	
	/** Select mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0 */
	public void testModelerFullID() {
		GroupSymbol g = new GroupSymbol("mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("Select mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
				 "SELECT mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 FROM mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
				 query);		
	    
	}

	/** Select a.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0 as a */
	public void testModelerAliasElement() {
		GroupSymbol g = new GroupSymbol("a", "mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0"); //$NON-NLS-1$ //$NON-NLS-2$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("a.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("Select a.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0 as a",  //$NON-NLS-1$
				 "SELECT a.mmuuid:abcf22c0-3236-1dfa-9931-e83d04ce10a0 FROM mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0 AS a",  //$NON-NLS-1$
				 query);		
	    
	}

	/** Select mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.* From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0 */
	public void testModelerGroupStar() {
		GroupSymbol g = new GroupSymbol("mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		AllInGroupSymbol a = new AllInGroupSymbol("mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.*");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("Select mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.* From mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
				 "SELECT mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0.* FROM mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
				 query);		
	    
	}
    
    /** SELECT * FROM mmuuid:66f628c1-3455-1dfa-9931-e83d04ce10a0 ORDER BY mmuuid:66f628c2-3455-1dfa-9931-e83d04ce10a0 */
    public void testModelerOrderBy() {
        GroupSymbol g = new GroupSymbol("mmuuid:66f628c1-3455-1dfa-9931-e83d04ce10a0"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new AllSymbol());

        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("mmuuid:66f628c2-3455-1dfa-9931-e83d04ce10a0")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOrderBy(orderBy);
        
        helpTest("SELECT * FROM mmuuid:66f628c1-3455-1dfa-9931-e83d04ce10a0 ORDER BY mmuuid:66f628c2-3455-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
                 "SELECT * FROM mmuuid:66f628c1-3455-1dfa-9931-e83d04ce10a0 ORDER BY mmuuid:66f628c2-3455-1dfa-9931-e83d04ce10a0",  //$NON-NLS-1$
                 query);        
        
    }

	/** SELECT "date"."time" from db.g */
	public void testReservedWordsInElement() {
		GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		ElementSymbol a = new ElementSymbol("date.time");  //$NON-NLS-1$
		select.addSymbol(a);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		helpTest("SELECT \"date\".\"time\" from db.g",  //$NON-NLS-1$
				 "SELECT \"date\".\"time\" FROM db.g",  //$NON-NLS-1$
				 query);
	    
	}
    
    /** SELECT a */
    public void testNoFromClause(){
        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        ExpressionSymbol b = new ExpressionSymbol("expr", new Constant(new Integer(5), Integer.class)); //$NON-NLS-1$
        select.addSymbol(a);
        select.addSymbol(b);
        Query query = new Query();
        query.setSelect(select);
        helpTest("SELECT a, 5", "SELECT a, 5", query);       //$NON-NLS-1$ //$NON-NLS-2$
    }
    
	// ==================== misc queries that should fail ===========================

	/** FROM g WHERE a = 'aString' */
	public void testFailsNoSelectClause(){
		helpException("FROM g WHERE a = 'aString'");		 //$NON-NLS-1$
	}
	
	/** SELECT a WHERE a = 'aString' */
	public void testFailsNoFromClause(){
		helpException("SELECT a WHERE a = 'aString'");		 //$NON-NLS-1$
	}
	
	/** SELECT xx.yy%.a from xx.yy */
	public void testFailsWildcardInSelect(){
		helpException("SELECT xx.yy%.a from xx.yy");		 //$NON-NLS-1$
	}

	/** SELECT a or b from g */
	public void testFailsOrInSelect(){
		helpException("SELECT a or b from g");		 //$NON-NLS-1$
	}
	
	/** SELECT a FROM g WHERE a LIKE x*/
	public void testLikeWOConstant(){
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        Criteria crit = new MatchCriteria(a, x);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a FROM g WHERE a LIKE x",  //$NON-NLS-1$
                 "SELECT a FROM g WHERE a LIKE x",  //$NON-NLS-1$
                 query);        
	}

	/** SELECT a from g ORDER BY b DSC*/
	public void testFailsDSCMisspelled(){
		helpException("SELECT a from g ORDER BY b DSC");		 //$NON-NLS-1$
	}
	
	/** Test reusability of parser */	
	public void testReusabilityOfParserObject() {
		GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
		From from = new From();
		from.addGroup(g);

		Select select = new Select();
		select.addSymbol(new ElementSymbol("a", false)); //$NON-NLS-1$
		
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);

		helpTest("SELECT a FROM m.g",  //$NON-NLS-1$
				 "SELECT a FROM m.g",  //$NON-NLS-1$
				 query);		    

		helpTest("SELECT a FROM m.g",  //$NON-NLS-1$
				 "SELECT a FROM m.g",  //$NON-NLS-1$
				 query);		    
	}
	
    /** SELECT a from db.g where b LIKE ? */
    public void testParameter1() {
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Reference ref1 = new Reference(0);
        Criteria crit = new MatchCriteria(new ElementSymbol("b"), ref1); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT a from db.g where b LIKE ?",  //$NON-NLS-1$
                 "SELECT a FROM db.g WHERE b LIKE ?",  //$NON-NLS-1$
                 query);        
    }

    /** SELECT a from db.g where b LIKE ? */
    public void testParameter2() {
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        Reference ref0 = new Reference(0);
        ExpressionSymbol expr = new ExpressionSymbol("expr", ref0); //$NON-NLS-1$
        select.addSymbol(expr);

        Reference ref1 = new Reference(1);
        Criteria crit = new MatchCriteria(new ElementSymbol("b"), ref1); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        helpTest("SELECT ? from db.g where b LIKE ?",  //$NON-NLS-1$
                 "SELECT ? FROM db.g WHERE b LIKE ?",  //$NON-NLS-1$
                 query);        
    }

    /** SELECT neato."cheese brain" FROM neato */
    public void XXX_testSpacesInIdentifiers() {
        GroupSymbol g = new GroupSymbol("neato"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol symbol = new ElementSymbol("cheese brain"); //$NON-NLS-1$
        symbol.setGroupSymbol(g);
        select.addSymbol(symbol);
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT neato.\"cheese brain\" FROM neato",  //$NON-NLS-1$
                 "SELECT neato.\"cheese brain\" FROM neato",  //$NON-NLS-1$
                 query);        
    }
    
    /** SELECT a, b FROM (SELECT c FROM m.g) AS y */
    public void testSubquery1() {
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol symbol = new ElementSymbol("c"); //$NON-NLS-1$
        symbol.setGroupSymbol(g);
        select.addSymbol(symbol);
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        SubqueryFromClause sfc = new SubqueryFromClause("y", query); //$NON-NLS-1$
        From from2 = new From();
        from2.addClause(sfc);
        
        Select select2 = new Select();
        select2.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
        select2.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$
        
        Query query2 = new Query();
        query2.setSelect(select2);
        query2.setFrom(from2);
        
        helpTest("SELECT a, b FROM (SELECT c FROM m.g) AS y",  //$NON-NLS-1$
                 "SELECT a, b FROM (SELECT c FROM m.g) AS y",  //$NON-NLS-1$
                 query2);             
    }
    
    /** SELECT a, b FROM ((SELECT c FROM m.g)) AS y */
    public void testSubquery1a() {
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol symbol = new ElementSymbol("c"); //$NON-NLS-1$
        symbol.setGroupSymbol(g);
        select.addSymbol(symbol);
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        SubqueryFromClause sfc = new SubqueryFromClause("y", query); //$NON-NLS-1$
        From from2 = new From();
        from2.addClause(sfc);
        
        Select select2 = new Select();
        select2.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
        select2.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$
        
        Query query2 = new Query();
        query2.setSelect(select2);
        query2.setFrom(from2);
        
        helpTest("SELECT a, b FROM ((SELECT c FROM m.g)) AS y",  //$NON-NLS-1$
                 "SELECT a, b FROM (SELECT c FROM m.g) AS y",  //$NON-NLS-1$
                 query2);             
    }    

    /** SELECT a, b FROM m.g1 JOIN (SELECT c FROM m.g2) AS y ON m.g1.a = y.c */
    public void testSubquery2() {
        GroupSymbol g = new GroupSymbol("m.g2"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol symbol = new ElementSymbol("c"); //$NON-NLS-1$
        symbol.setGroupSymbol(g);
        select.addSymbol(symbol);
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        UnaryFromClause ufc = new UnaryFromClause(new GroupSymbol("m.g1")); //$NON-NLS-1$
        SubqueryFromClause sfc = new SubqueryFromClause("y", query); //$NON-NLS-1$
        CompareCriteria join = new CompareCriteria(new ElementSymbol("m.g1.a"), CompareCriteria.EQ, new ElementSymbol("y.c")); //$NON-NLS-1$ //$NON-NLS-2$
        List crits = new ArrayList();
        crits.add(join);
        JoinPredicate jp = new JoinPredicate(ufc, sfc, JoinType.JOIN_INNER, crits);
        From from2 = new From();
        from2.addClause(jp);
                        
        Select select2 = new Select();
        select2.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
        select2.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$
        
        Query query2 = new Query();
        query2.setSelect(select2);
        query2.setFrom(from2);
        
        helpTest("SELECT a, b FROM m.g1 JOIN (SELECT c FROM m.g2) AS y ON m.g1.a = y.c",  //$NON-NLS-1$
                 "SELECT a, b FROM m.g1 INNER JOIN (SELECT c FROM m.g2) AS y ON m.g1.a = y.c",  //$NON-NLS-1$
                 query2);             
    }

	/** SELECT a, b FROM (SELECT c FROM m.g2) */
	public void testSubqueryInvalid() {
		helpException("SELECT a, b FROM (SELECT c FROM m.g2)"); //$NON-NLS-1$
	}
		    
    /** INSERT INTO m.g (a) VALUES (?) */
    public void testInsertWithReference() {
        Insert insert = new Insert();
        insert.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        List vars = new ArrayList();
        vars.add(new ElementSymbol("a"));         //$NON-NLS-1$
        insert.setVariables(vars);
        List values = new ArrayList();
        values.add(new Reference(0));
        insert.setValues(values);
        helpTest("INSERT INTO m.g (a) VALUES (?)",  //$NON-NLS-1$
                 "INSERT INTO m.g (a) VALUES (?)",  //$NON-NLS-1$
                 insert);                     
    }
        
    public void testStoredQueryWithNoParameter(){
    	StoredProcedure storedQuery = new StoredProcedure();
    	storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
    	helpTest("exec proc1()", "EXEC proc1()", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    	helpTest("execute proc1()", "EXEC proc1()", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testStoredQueryWithNoParameter2(){
    	StoredProcedure storedQuery = new StoredProcedure();
    	storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
    	
        From from = new From();
        SubqueryFromClause sfc = new SubqueryFromClause("x", storedQuery); //$NON-NLS-1$
        from.addClause(sfc);
        
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x.a")); //$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
    	helpTest("SELECT X.A FROM (exec proc1()) AS X", "SELECT X.A FROM (EXEC proc1()) AS X", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testStoredQuery(){
    	StoredProcedure storedQuery = new StoredProcedure();
    	storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
    	SPParameter parameter = new SPParameter(1, new Constant("param1")); //$NON-NLS-1$
        parameter.setParameterType(ParameterInfo.IN);
    	storedQuery.setParameter(parameter);
    	helpTest("Exec proc1('param1')", "EXEC proc1('param1')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    	helpTest("execute proc1('param1')", "EXEC proc1('param1')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testStoredQuery2(){
    	StoredProcedure storedQuery = new StoredProcedure();
    	storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
    	SPParameter parameter = new SPParameter(1, new Constant("param1")); //$NON-NLS-1$
    	storedQuery.setParameter(parameter);
    	From from = new From();
        SubqueryFromClause sfc = new SubqueryFromClause("x", storedQuery); //$NON-NLS-1$
        from.addClause(sfc);
        
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x.a")); //$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
    	helpTest("SELECT X.A FROM (exec proc1('param1')) AS X", "SELECT X.A FROM (EXEC proc1('param1')) AS X", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testStoredQuery2SanityCheck(){
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        SPParameter parameter = new SPParameter(1, new Constant("param1")); //$NON-NLS-1$
        storedQuery.setParameter(parameter);
        From from = new From();
        SubqueryFromClause sfc = new SubqueryFromClause("x", storedQuery); //$NON-NLS-1$
        from.addClause(sfc);
        
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x.a")); //$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("exec proc1('param1')", "EXEC proc1('param1')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    /** 
     * Try nesting subquery in double parentheses - parsing fails.  'exec' is not handled as
     * robustly as other types of commands that can appear in a from clause subquery. 
     * 
     * deferred, not important enough 
     */
    public void DEFERRED_testStoredQuerySubqueryMultipleParens(){
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        SPParameter parameter = new SPParameter(1, new Constant("param1")); //$NON-NLS-1$
        storedQuery.setParameter(parameter);
        From from = new From();
        SubqueryFromClause sfc = new SubqueryFromClause("x", storedQuery); //$NON-NLS-1$
        from.addClause(sfc);
        
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x.a")); //$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        helpTest("SELECT X.A FROM ((exec proc1('param1'))) AS X", "SELECT X.A FROM (EXEC proc1('param1')) AS X", query); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    public void testErrorStatement(){
        RaiseErrorStatement errStmt = new RaiseErrorStatement(new Constant("Test only")); //$NON-NLS-1$
                 
        helpStmtTest("ERROR 'Test only';", "ERROR 'Test only';", //$NON-NLS-1$ //$NON-NLS-2$
            errStmt);           
    }
    
    public void testIfStatement(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement ifStmt = new DeclareStatement(a, shortType);

        ElementSymbol b = new ElementSymbol("b"); //$NON-NLS-1$
        Statement elseStmt = new DeclareStatement(b, shortType);
              
        Block ifBlock = new Block();
        ifBlock.addStatement(ifStmt); 
      
        Block elseBlock = new Block();
        elseBlock.addStatement(elseStmt);
    
        ElementSymbol c = new ElementSymbol("c");     //$NON-NLS-1$
        Criteria crit = new CompareCriteria(c, CompareCriteria.EQ, 
            new Constant(new Integer(5)));
            
        IfStatement stmt = new IfStatement(crit, ifBlock, elseBlock);
                        
        helpStmtTest("IF(c = 5) BEGIN DECLARE short a; END ELSE BEGIN DECLARE short b; END", //$NON-NLS-1$
             "IF(c = 5)"+"\n"+ "BEGIN"+"\n"+"DECLARE short a;"+"\n"+"END"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
             "ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short b;"+"\n"+"END", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
             stmt);     
    }    

    /*public void testIfStatement1(){
        ElementSymbol a = new ElementSymbol("a");
        String shortType = new String("short");
        Statement ifStmt = new DeclareStatement(a, shortType);

        ElementSymbol b = new ElementSymbol("b");
        Statement elseStmt = new DeclareStatement(b, shortType);
              
        Block ifBlock = new Block();
        ifBlock.addStatement(ifStmt); 
      
        Block elseBlock = new Block();
        elseBlock.addStatement(elseStmt);
    
        ElementSymbol c = new ElementSymbol("c");    
        Criteria crit = new CompareCriteria(c, CompareCriteria.EQ, 
            new Constant(new Integer(5)));
            
        IfStatement stmt = new IfStatement(crit, ifBlock, elseBlock);
                        
        helpStmtTest("IF(c = 5) BEGIN DECLARE short a; END ELSE ",
             "IF(c = 5)"+"\n"+ "BEGIN"+"\n"+"DECLARE short a;"+"\n"+"END"+"\n"+
             "ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short b;"+"\n"+"END",
             stmt);     
    }*/   
    
    public void testCriteriaSelector0(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("IS NULL CRITERIA ON (a)", "IS NULL CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector1(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_EQ);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("= CRITERIA ON (a)", "= CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector2(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("<> CRITERIA ON (a)", "<> CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector3(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_LT);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("< CRITERIA ON (a)", "< CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector4(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_GT);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("> CRITERIA ON (a)", "> CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector5(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_GE);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest(">= CRITERIA ON (a)", ">= CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector6(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_LE);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("<= CRITERIA ON (a)", "<= CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector7(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.LIKE);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("LIKE CRITERIA ON (a)", "LIKE CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    
    public void testCriteriaSelector8(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IN);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("IN CRITERIA ON (a)", "IN CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCriteriaSelector9(){
        //ElementSymbol a = new ElementSymbol("a");
        
        CriteriaSelector critSelector = new CriteriaSelector();
        //critSelector.setSelectorType(CriteriaSelector.IS_NULL);
        //critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("CRITERIA", "CRITERIA", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCriteriaSelector10(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.BETWEEN);
        critSelector.addElement(a);        
       
        helpCriteriaSelectorTest("BETWEEN CRITERIA ON (a)", "BETWEEN CRITERIA ON (a)", critSelector);     //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**HAS IS NULL CRITERIA ON (a)*/    
    public void testHasIsNullCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS IS NULL CRITERIA ON (a)", "HAS IS NULL CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
    
    /**HAS LIKE CRITERIA ON (a)*/    
    public void testHasLikeCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.LIKE);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS LIKE CRITERIA ON (a)", "HAS LIKE CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }  
            
    public void testHasEQCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_EQ);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS = CRITERIA ON (a)", "HAS = CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }    
    
    public void testHasNECriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS <> CRITERIA ON (a)", "HAS <> CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }    
    
    /**HAS IN CRITERIA ON (a)*/    
    public void testHasInCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IN);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS IN CRITERIA ON (a)", "HAS IN CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
     
    /**HAS COMPARE_LT CRITERIA ON (a)*/    
    public void testHasLTCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_LT);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS < CRITERIA ON (a)", "HAS < CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
    
    /**HAS COMPARE_LE CRITERIA ON (a)*/    
    public void testHasLECriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_LE);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS <= CRITERIA ON (a)", "HAS <= CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
    
    /**HAS COMPARE_GT CRITERIA ON (a)*/    
    public void testHasGTCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_GT);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS > CRITERIA ON (a)", "HAS > CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
       
    /**HAS COMPARE_GE CRITERIA ON (a)*/    
    public void testHasGECriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_GE);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS >= CRITERIA ON (a)", "HAS >= CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
         
    /**HAS BETWEEN CRITERIA ON (a)*/    
    public void testHasBetweenCriteria(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.BETWEEN);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        helpCriteriaTest("HAS BETWEEN CRITERIA ON (a)", "HAS BETWEEN CRITERIA ON (a)", //$NON-NLS-1$ //$NON-NLS-2$
            hasSelector);
    }   
         
    public void testTranslateCriteria(){
        ElementSymbol a = new ElementSymbol("a");              //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, 
            new Constant(new Integer(5)));        
        List critList = new ArrayList();
        critList.add(crit);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector.setElements(elements);
                        
        TranslateCriteria transCriteria = new TranslateCriteria(critSelector, critList);        
                          
        helpCriteriaTest("TRANSLATE IS NULL CRITERIA ON (a) WITH (a = 5)",  //$NON-NLS-1$
            "TRANSLATE IS NULL CRITERIA ON (a) WITH (a = 5)", //$NON-NLS-1$
            transCriteria);   
            
        //helpCriteriaTest("TRANSLATE IS NULL CRITERIA ON (a) USING transFuncEQ (a)",
        //"TRANSLATE IS NULL CRITERIA ON (a) USING transFuncEQ (a)",
        //transCriteria);
            
    }
    
    public void testAssignStatement(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
       
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        
        Expression expr = new Constant("aString"); //$NON-NLS-1$
        
        AssignmentStatement queryStmt = new AssignmentStatement(a, queryCmd);
        AssignmentStatement exprStmt = new AssignmentStatement(a, expr);
        
        helpStmtTest("a = SELECT a1 FROM g WHERE a2 = 5;", "a = SELECT a1 FROM g WHERE a2 = 5;", //$NON-NLS-1$ //$NON-NLS-2$
            queryStmt);
        
        helpStmtTest("a = 'aString';", "a = 'aString';", exprStmt);      //$NON-NLS-1$ //$NON-NLS-2$
    }
    
     public void testAssignStatement2(){
        Insert insert = new Insert();
        insert.setGroup(new GroupSymbol("g")); //$NON-NLS-1$
        List vars = new ArrayList();
        vars.add(new ElementSymbol("a")); //$NON-NLS-1$
                
        insert.setVariables(vars);
        List values = new ArrayList();
        values.add(new Reference(0));
        insert.setValues(values);
   
        //Command insertCmd = insert;       
        AssignmentStatement insertStmt = new AssignmentStatement(new ElementSymbol("b"), insert); //$NON-NLS-1$
                
        helpStmtTest("b = INSERT INTO g (a) VALUES (?);", "b = INSERT INTO g (a) VALUES (?);", //$NON-NLS-1$ //$NON-NLS-2$
            insertStmt);
           
    }
    
    public void testDeclareStatement(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        String type = new String("short"); //$NON-NLS-1$
        DeclareStatement stmt = new DeclareStatement(a, type);
    
        helpStmtTest("DECLARE short a;","DECLARE short a;", stmt); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDeclareStatementWithAssignment(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        String type = new String("short"); //$NON-NLS-1$
        DeclareStatement stmt = new DeclareStatement(a, type, new Constant(null));
    
        helpStmtTest("DECLARE short a = null;","DECLARE short a = null;", stmt); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDeclareStatementWithAssignment1(){
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        String type = new String("string"); //$NON-NLS-1$
        DeclareStatement stmt = new DeclareStatement(a, type, sampleQuery());
    
        helpStmtTest("DECLARE string a = SELECT a1 FROM g WHERE a2 = 5;","DECLARE string a = SELECT a1 FROM g WHERE a2 = 5;", stmt); //$NON-NLS-1$ //$NON-NLS-2$
    }
      
    public void testStatement() {
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        String type = new String("short"); //$NON-NLS-1$
        DeclareStatement declStmt = new DeclareStatement(a, type);
        Statement stmt = declStmt;
       
        helpStmtTest("DECLARE short a;", "DECLARE short a;", //$NON-NLS-1$ //$NON-NLS-2$
            stmt);
    }
        
    public void testBlock() {
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        String type = new String("short"); //$NON-NLS-1$
        DeclareStatement declStmt = new DeclareStatement(a, type);
        Statement stmt = declStmt;
        Block block = new Block(stmt);       
       
        helpBlockTest("BEGIN DECLARE short a; END", "BEGIN"+"\n"+"DECLARE short a;"+"\n"+"END", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            block);
    }
       
    public void testCommandStatement(){
        Query query = sampleQuery();
        
        Command sqlCmd = query;
        CommandStatement cmdStmt = new CommandStatement(sqlCmd);
   
        helpStmtTest("SELECT a1 FROM g WHERE a2 = 5;", "SELECT a1 FROM g WHERE a2 = 5;", //$NON-NLS-1$ //$NON-NLS-2$
        cmdStmt);       
    }

    /** 
     * @return
     */
    private Query sampleQuery() {
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);
           
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        return query;
    }
    
    public void testDynamicCommandStatement(){
        List symbols = new ArrayList();

        ElementSymbol a1 = new ElementSymbol("a1"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.STRING);
        symbols.add(a1);  
        
        DynamicCommand sqlCmd = new DynamicCommand();
        Expression sql = new Constant("SELECT a1 FROM g WHERE a2 = 5"); //$NON-NLS-1$
        
        sqlCmd.setSql(sql);
        sqlCmd.setAsColumns(symbols);
        sqlCmd.setAsClauseSet(true);
        
        sqlCmd.setIntoGroup(new GroupSymbol("#g")); //$NON-NLS-1$
        
        CommandStatement cmdStmt = new CommandStatement(sqlCmd);
   
        helpStmtTest("exec string 'SELECT a1 FROM g WHERE a2 = 5' as a1 string into #g;", "EXECUTE STRING 'SELECT a1 FROM g WHERE a2 = 5' AS a1 string INTO #g;", //$NON-NLS-1$ //$NON-NLS-2$
        cmdStmt);       
    }
    
    //sql is a variable, also uses the as, into, and update clauses
    public void testDynamicCommandStatement1(){
        List symbols = new ArrayList();
        
        ElementSymbol a1 = new ElementSymbol("a1"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.STRING);
        symbols.add(a1);  
        
        ElementSymbol a2 = new ElementSymbol("a2"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        symbols.add(a2);  
        
        DynamicCommand sqlCmd = new DynamicCommand();
        Expression sql = new ElementSymbol("z"); //$NON-NLS-1$
        
        sqlCmd.setSql(sql);
        sqlCmd.setAsColumns(symbols);
        sqlCmd.setAsClauseSet(true);
        
        sqlCmd.setIntoGroup(new GroupSymbol("#g")); //$NON-NLS-1$
        
        sqlCmd.setUpdatingModelCount(1);
        
        CommandStatement cmdStmt = new CommandStatement(sqlCmd);
   
        helpStmtTest("execute string z as a1 string, a2 integer into #g update 1;", "EXECUTE STRING z AS a1 string, a2 integer INTO #g UPDATE 1;", //$NON-NLS-1$ //$NON-NLS-2$
        cmdStmt);       
    }
    
    public void testDynamicCommandStatementWithUsing(){
        SetClauseList using = new SetClauseList();
        
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        using.addClause(a, new ElementSymbol("b"));  //$NON-NLS-1$
                
        DynamicCommand sqlCmd = new DynamicCommand();
        Expression sql = new ElementSymbol("z"); //$NON-NLS-1$
        
        sqlCmd.setSql(sql);
        sqlCmd.setUsing(using);
        
        CommandStatement cmdStmt = new CommandStatement(sqlCmd);
   
        helpStmtTest("execute string z using a=b;", "EXECUTE STRING z USING a = b;", //$NON-NLS-1$ //$NON-NLS-2$
        cmdStmt);       
    }

    //as clause should use short names
    public void testDynamicCommandStatement2(){
        helpException("create virtual procedure begin execute string z as variables.a1 string, a2 integer into #g; end"); //$NON-NLS-1$       
    }
    
    //using clause should use short names
    public void testDynamicCommandStatement3(){
        helpException("create virtual procedure begin execute string z as a1 string, a2 integer into #g using variables.x=variables.y; end", "Parsing error: Invalid simple identifier format: [variables.x]"); //$NON-NLS-1$ //$NON-NLS-2$       
    }
    
    //into clause requires as clause
    public void testDynamicCommandStatement4(){
        helpException("create virtual procedure begin execute string z into #g using x=variables.y; end"); //$NON-NLS-1$       
    }
    
    /** original test */
    public void testCreateUpdateProcedureCommand(){
        helpTestCreateUpdateProcedureCommandCase3025("CREATE PROCEDURE\nBEGIN\nDECLARE short var1;"+ //$NON-NLS-1$
           "IF(HAS IS NULL CRITERIA ON (a))\nBEGIN\nvar1 = SELECT a1 FROM g WHERE a2 = 5;\nEND\n"+ //$NON-NLS-1$
           "ELSE\nBEGIN\nDECLARE short var2;\nvar2 = SELECT b1 FROM g WHERE a2 = 5;\nEND\n" + //$NON-NLS-1$
           " END"); //$NON-NLS-1$
                  
    }

    /** test that a command in parens isn't parsed as a ScalarSubquery */
    public void testCreateUpdateProcedureCommandCase3025_1(){
 
        helpTestCreateUpdateProcedureCommandCase3025("CREATE PROCEDURE\nBEGIN\nDECLARE short var1;"+ //$NON-NLS-1$
         "IF(HAS IS NULL CRITERIA ON (a))\nBEGIN\nvar1 = (SELECT a1 FROM g WHERE a2 = 5);\nEND\n"+ //$NON-NLS-1$
         "ELSE\nBEGIN\nDECLARE short var2;\nvar2 = SELECT b1 FROM g WHERE a2 = 5;\nEND\n" + //$NON-NLS-1$
         " END"); //$NON-NLS-1$ 
                  
    }    

    /** test that a command in DOUBLE parens isn't parsed as a ScalarSubquery */
    public void testCreateUpdateProcedureCommandCase3025_2(){
        helpTestCreateUpdateProcedureCommandCase3025("CREATE PROCEDURE\nBEGIN\nDECLARE short var1;"+ //$NON-NLS-1$
           "IF(HAS IS NULL CRITERIA ON (a))\nBEGIN\nvar1 = ((SELECT a1 FROM g WHERE a2 = 5) );\nEND\n"+ //$NON-NLS-1$
           "ELSE\nBEGIN\nDECLARE short var2;\nvar2 = SELECT b1 FROM g WHERE a2 = 5;\nEND\n" + //$NON-NLS-1$
           " END"); //$NON-NLS-1$ 
    }     

    private void helpTestCreateUpdateProcedureCommandCase3025(String procedureString){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
    
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
          
        Block block = new Block();
        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
 
         helpTest(procedureString, "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ 
           "IF(HAS IS NULL CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        
    }

    /** test an expression in parentheses in an assignment statement */
    public void testCreateUpdateProcedureCommandCase3025_3(){
 
        String procedureString = "CREATE PROCEDURE\nBEGIN\nDECLARE short var1;"+ //$NON-NLS-1$
         "IF(HAS IS NULL CRITERIA ON (a))\nBEGIN\nvar1 = (concat('x', 'y') );\nEND\n"+ //$NON-NLS-1$
         "ELSE\nBEGIN\nDECLARE short var2;\nvar2 = SELECT b1 FROM g WHERE a2 = 5;\nEND\n" + //$NON-NLS-1$
         " END"; //$NON-NLS-1$
        
        helpTestCreateUpdateProcedureCommandCase3025_Expression(procedureString);
    }

    /** test an expression in parentheses in an assignment statement */
    public void testCreateUpdateProcedureCommandCase3025_4(){
 
        String procedureString = "CREATE PROCEDURE\nBEGIN\nDECLARE short var1;"+ //$NON-NLS-1$
         "IF(HAS IS NULL CRITERIA ON (a))\nBEGIN\nvar1 = ((concat('x', 'y') ));\nEND\n"+ //$NON-NLS-1$
         "ELSE\nBEGIN\nDECLARE short var2;\nvar2 = SELECT b1 FROM g WHERE a2 = 5;\nEND\n" + //$NON-NLS-1$
         " END"; //$NON-NLS-1$
        
        helpTestCreateUpdateProcedureCommandCase3025_Expression(procedureString);
    }

    /** test an expression without parentheses in an assignment statement */
    public void testCreateUpdateProcedureCommandCase3025_5(){
 
        String procedureString = "CREATE PROCEDURE\nBEGIN\nDECLARE short var1;"+ //$NON-NLS-1$
         "IF(HAS IS NULL CRITERIA ON (a))\nBEGIN\nvar1 = concat('x', 'y') ;\nEND\n"+ //$NON-NLS-1$
         "ELSE\nBEGIN\nDECLARE short var2;\nvar2 = SELECT b1 FROM g WHERE a2 = 5;\nEND\n" + //$NON-NLS-1$
         " END"; //$NON-NLS-1$
        
        helpTestCreateUpdateProcedureCommandCase3025_Expression(procedureString);
    }
    
    
    /** test an expression in parentheses in an assignment statement */
    private void helpTestCreateUpdateProcedureCommandCase3025_Expression(String procedureString){
        String expectedString = "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ 
        "IF(HAS IS NULL CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = concat('x', 'y');"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
        "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
                  
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        Expression[] args = new Expression[] {new Constant("x"), new Constant("y")}; //$NON-NLS-1$ //$NON-NLS-2$
        Function function = new Function("concat", args); //$NON-NLS-1$
        AssignmentStatement queryStmt = new AssignmentStatement(var1, function);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 

        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
    
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
          
        Block block = new Block();
        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
 
        helpTest(procedureString, expectedString, cmd);
    }    

    /**IF statement with has criteria */
    public void testCreateUpdateProcedureCommand1(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
   
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
     public void testCreateUpdateProcedureCommand0(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
   
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        //critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS CRITERIA) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS CRITERIA)"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /**IF statement with has LIKE criteria */
    public void testCreateUpdateProcedureCommand2(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
   
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.LIKE);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS LIKE CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS LIKE CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /**IF statement with has IN criteria */
    public void testCreateUpdateProcedureCommand3(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
   
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IN);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS IN CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS IN CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /**IF statement with has <> criteria */
    public void testCreateUpdateProcedureCommand4(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
   
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /**Has criteria in WHERE clause*/
    public void testCreateUpdateProcedureCommand5(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
   
        //element for has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);     
       
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
   
        CriteriaSelector critSelector2 = new CriteriaSelector();
        //critSelector2.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector2.setElements(elements);
        
        HasCriteria hasSelector2 = new HasCriteria();
        hasSelector2.setSelector(critSelector2);     
        //has criteria for else block
        elseQuery.setCriteria(hasSelector2);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
         
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
        
        CriteriaSelector critSelector1 = new CriteriaSelector();
        critSelector1.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector1.setElements(elements);
        
        HasCriteria hasSelector1 = new HasCriteria();
        hasSelector1.setSelector(critSelector1);
        
        IfStatement stmt = new IfStatement(hasSelector1, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE HAS CRITERIA ON (a); END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE HAS CRITERIA ON (a);"+"\n"+"END"+"\n"+"END", cmd);                                        //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
        
    /** Translate criteria (empty criteriaSelector in WHERE clause*/
    public void testCreateUpdateProcedureCommand7(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
   
        //element for has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);     
       
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
   
        Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, 
            new Constant(new Integer(5)));        
        List critList = new ArrayList();
        critList.add(crit);
        
        CriteriaSelector critSelector2 = new CriteriaSelector();
        //critSelector2.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector2.setElements(elements);
                        
        TranslateCriteria transCriteria = new TranslateCriteria(critSelector2, critList); 
        elseQuery.setCriteria(transCriteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
         
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
        
        CriteriaSelector critSelector1 = new CriteriaSelector();
        critSelector1.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector1.setElements(elements);
        
        HasCriteria hasSelector1 = new HasCriteria();
        hasSelector1.setSelector(critSelector1);
        
        IfStatement stmt = new IfStatement(hasSelector1, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA ON (a) WITH (a = 5); END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA ON (a) WITH (a = 5);"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /** Translate criteria (is null criteriaSelector in WHERE clause*/
    public void testCreateUpdateProcedureCommand9(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
   
        //element for has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);     
       
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
   
        Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, 
            new Constant(new Integer(5)));        
        List critList = new ArrayList();
        critList.add(crit);
        
        CriteriaSelector critSelector2 = new CriteriaSelector();
        critSelector2.setSelectorType(CriteriaSelector.IS_NULL);
        critSelector2.setElements(elements);
                        
        TranslateCriteria transCriteria = new TranslateCriteria(critSelector2, critList); 
        elseQuery.setCriteria(transCriteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
         
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
        
        CriteriaSelector critSelector1 = new CriteriaSelector();
        critSelector1.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector1.setElements(elements);
        
        HasCriteria hasSelector1 = new HasCriteria();
        hasSelector1.setSelector(critSelector1);
        
        IfStatement stmt = new IfStatement(hasSelector1, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE TRANSLATE IS NULL CRITERIA ON (a) WITH (a = 5); END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE TRANSLATE IS NULL CRITERIA ON (a) WITH (a = 5);"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
        /** Translate criteria ( only with WHERE clause) */
    public void testCreateUpdateProcedureCommand10(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
   
        //element for has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);     
       
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
   
        Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, 
            new Constant(new Integer(5)));        
        List critList = new ArrayList();
        critList.add(crit);
 
        TranslateCriteria transCriteria = new TranslateCriteria();
        CriteriaSelector critSelector2 = new CriteriaSelector();
        transCriteria.setTranslations(critList);
        transCriteria.setSelector(critSelector2);
        
        elseQuery.setCriteria(transCriteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
         
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
        
        CriteriaSelector critSelector1 = new CriteriaSelector();
        critSelector1.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector1.setElements(elements);
        
        HasCriteria hasSelector1 = new HasCriteria();
        hasSelector1.setSelector(critSelector1);
        
        IfStatement stmt = new IfStatement(hasSelector1, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA WITH (a = 5); END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA WITH (a = 5);"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /** Translate criteria ( only with WHERE clause) */
    public void testCreateUpdateProcedureCommand12(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
   
        //element for has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);    
       
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
   
        
        Criteria crit1 = new CompareCriteria(a, CompareCriteria.EQ, 
            new Constant(new Integer(5))); 
        ElementSymbol m = new ElementSymbol("m"); //$NON-NLS-1$
        Criteria crit2= new CompareCriteria(m, CompareCriteria.EQ, 
            new Constant(new Integer(6)));          
        List critList = new ArrayList();
        critList.add(crit1);
        critList.add(crit2);
 
        TranslateCriteria transCriteria = new TranslateCriteria();
        CriteriaSelector critSelector2 = new CriteriaSelector();
        transCriteria.setTranslations(critList);
        transCriteria.setSelector(critSelector2);
        
        elseQuery.setCriteria(transCriteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
         
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
        
        CriteriaSelector critSelector1 = new CriteriaSelector();
        critSelector1.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector1.setElements(elements);
        
        HasCriteria hasSelector1 = new HasCriteria();
        hasSelector1.setSelector(critSelector1);
        
        IfStatement stmt = new IfStatement(hasSelector1, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA WITH (a = 5, m = 6); END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA WITH (a = 5, m = 6);"+"\n"+"END"+"\n"+"END", cmd);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
                                          
    }
   
   /** Translate criteria (with only Criteria in WHERE clause) */
    public void testCreateUpdateProcedureCommand11(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
   
        //element for has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);     
       
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
   
        Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, 
            new Constant(new Integer(5)));        
        List critList = new ArrayList();
        critList.add(crit);
       
        CriteriaSelector critSelector = new CriteriaSelector();       
        TranslateCriteria transCrit = new TranslateCriteria();
        transCrit.setSelector(critSelector);
                
        elseQuery.setCriteria(transCrit);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
         
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
        
        CriteriaSelector critSelector1 = new CriteriaSelector();
        critSelector1.setSelectorType(CriteriaSelector.COMPARE_NE);
        critSelector1.setElements(elements);
        
        HasCriteria hasSelector1 = new HasCriteria();
        hasSelector1.setSelector(critSelector1);
        
        IfStatement stmt = new IfStatement(hasSelector1, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS <> CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS <> CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE TRANSLATE CRITERIA;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    /**IF statement with has criteria no on */
    public void testCreateUpdateProcedureCommand8(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        elseQuery.setFrom(from);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
         
        CriteriaSelector critSelector = new CriteriaSelector();
       
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS CRITERIA) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS CRITERIA)"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
     
    public void testSubquerySetCriteria0() { 
        //test wrap up command with subquerySetCriteria
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Expression expr = new ElementSymbol("b"); //$NON-NLS-1$
              
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
            
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        SubquerySetCriteria subCrit = new SubquerySetCriteria(expr, query);
       
        Query outer = new Query();
        outer.setSelect(select);
        outer.setFrom(from);
        outer.setCriteria(subCrit);
                     
        helpTest("SELECT a FROM db.g WHERE b IN (SELECT a FROM db.g WHERE a2 = 5)", //$NON-NLS-1$
            "SELECT a FROM db.g WHERE b IN (SELECT a FROM db.g WHERE a2 = 5)", //$NON-NLS-1$
             outer);        
    }     
               
    public void testSubquerySetCriteria1() { 

        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Expression expr = new ElementSymbol("b"); //$NON-NLS-1$
              
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
            
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        SubquerySetCriteria subCrit = new SubquerySetCriteria(expr, query);
        subCrit.setNegated(true);
        Query outer = new Query();
        outer.setSelect(select);
        outer.setFrom(from);
        outer.setCriteria(subCrit);
                     
        helpTest("SELECT a FROM db.g WHERE b NOT IN (SELECT a FROM db.g WHERE a2 = 5)", //$NON-NLS-1$
            "SELECT a FROM db.g WHERE b NOT IN (SELECT a FROM db.g WHERE a2 = 5)", //$NON-NLS-1$
             outer);        
    }                

    public void testSubquerySetCriteriaWithExec() { 
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Expression expr = new ElementSymbol("b"); //$NON-NLS-1$
              
        StoredProcedure exec = new StoredProcedure();
        exec.setProcedureName("m.sq1");               //$NON-NLS-1$
        SubquerySetCriteria subCrit = new SubquerySetCriteria(expr, exec);
       
        Query outer = new Query();
        outer.setSelect(select);
        outer.setFrom(from);
        outer.setCriteria(subCrit);
                     
        helpTest("SELECT a FROM db.g WHERE b IN (EXEC m.sq1())", //$NON-NLS-1$
            "SELECT a FROM db.g WHERE b IN (EXEC m.sq1())", //$NON-NLS-1$
             outer);        
    }          

    public void testSubquerySetCriteriaWithUnion() { 
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Expression expr = new ElementSymbol("b"); //$NON-NLS-1$

        Query u1 = new Query();
        Select u1s = new Select();
        u1s.addSymbol(new ElementSymbol("x1")); //$NON-NLS-1$
        u1.setSelect(u1s);
        From u1f = new From();
        u1f = new From();
        u1f.addClause(new UnaryFromClause(new GroupSymbol("db.g2"))); //$NON-NLS-1$
        u1.setFrom(u1f);

        Query u2 = new Query();
        Select u2s = new Select();
        u2s.addSymbol(new ElementSymbol("x2")); //$NON-NLS-1$
        u2.setSelect(u2s);
        From u2f = new From();
        u2f = new From();
        u2f.addClause(new UnaryFromClause(new GroupSymbol("db.g3"))); //$NON-NLS-1$
        u2.setFrom(u2f);

        SetQuery union = new SetQuery(Operation.UNION, true, u1, u2);        
                       
        SubquerySetCriteria subCrit = new SubquerySetCriteria(expr, union);
       
        Query outer = new Query();
        outer.setSelect(select);
        outer.setFrom(from);
        outer.setCriteria(subCrit);
                     
        helpTest("SELECT a FROM db.g WHERE b IN (SELECT x1 FROM db.g2 UNION ALL SELECT x2 FROM db.g3)", //$NON-NLS-1$
            "SELECT a FROM db.g WHERE b IN (SELECT x1 FROM db.g2 UNION ALL SELECT x2 FROM db.g3)", //$NON-NLS-1$
             outer);        
    }          
        
    public void testVariablesInExec(){
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        SPParameter parameter = new SPParameter(1, new ElementSymbol("param1")); //$NON-NLS-1$
        parameter.setParameterType(ParameterInfo.IN);
        storedQuery.setParameter(parameter);
        helpTest("Exec proc1(param1)", "EXEC proc1(param1)", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("execute proc1(param1)", "EXEC proc1(param1)", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }
                
    public void testExecSubquery(){
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addClause(new UnaryFromClause(new GroupSymbol("newModel2.Table1")));         //$NON-NLS-1$
        StoredProcedure subquery = new StoredProcedure();
        subquery.setProcedureName("NewVirtual.StoredQuery");
        from.addClause(new SubqueryFromClause("a", subquery)); //$NON-NLS-1$
        query.setFrom(from);
                        
        helpTest("SELECT * FROM newModel2.Table1, (EXEC NewVirtual.StoredQuery()) AS a",  //$NON-NLS-1$
            "SELECT * FROM newModel2.Table1, (EXEC NewVirtual.StoredQuery()) AS a", //$NON-NLS-1$
            query);
    }    
    
    public void testUnicode1() {
        try {
            byte[] data = { (byte)0xd0, (byte)0x9c, (byte)0xd0, (byte)0xbe, (byte)0xd1, (byte)0x81, (byte)0xd0, (byte)0xba, (byte)0xd0, (byte)0xb2, (byte)0xd0, (byte)0xb0};
    
            String string = new String(data, "UTF-8");  //$NON-NLS-1$
            String sql = "SELECT * FROM TestDocument.TestDocument WHERE Subject='" + string + "'";  //$NON-NLS-1$ //$NON-NLS-2$

            Query query = new Query();
            Select select = new Select();
            select.addSymbol(new AllSymbol());
            query.setSelect(select);
            From from = new From();
            from.addGroup(new GroupSymbol("TestDocument.TestDocument")); //$NON-NLS-1$
            query.setFrom(from);            
            CompareCriteria crit = new CompareCriteria(new ElementSymbol("Subject"), CompareCriteria.EQ, new Constant(string)); //$NON-NLS-1$
            query.setCriteria(crit);

            helpTest(sql, query.toString(), query);

        } catch(UnsupportedEncodingException e)   {
            fail(e.getMessage());                 
        }
    }

    public void testUnicode2() {
        String sql = "SELECT * FROM TestDocument.TestDocument WHERE Subject='\u0041\u005a'";  //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("TestDocument.TestDocument")); //$NON-NLS-1$
        query.setFrom(from);           
        CompareCriteria crit = new CompareCriteria(new ElementSymbol("Subject"), CompareCriteria.EQ, new Constant("AZ")); //$NON-NLS-1$ //$NON-NLS-2$
        query.setCriteria(crit);

        helpTest(sql, query.toString(), query);
    }
 
	public void testUnicode3() {
		String sql = "SELECT \"\u05e0\"";  //$NON-NLS-1$

		Query query = new Query();
		Select select = new Select();
		Constant c = new Constant("\u05e0"); //$NON-NLS-1$
		select.addSymbol(new ExpressionSymbol("expr", c)); //$NON-NLS-1$
		query.setSelect(select);

		helpTest(sql, query.toString(), query);
	}

	public void testUnicode4() {
		String sql = "SELECT \u05e0 FROM g";  //$NON-NLS-1$

		Query query = new Query();
		Select select = new Select();
		ElementSymbol e = new ElementSymbol("\u05e0"); //$NON-NLS-1$
		select.addSymbol(e);
		From from = new From();
		from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
		query.setSelect(select);
		query.setFrom(from);

		helpTest(sql, query.toString(), query);
	}
    
    public void testEscapedFunction1() {
        String sql = "SELECT * FROM a.thing WHERE e1 = {fn concat('a', 'b')}"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("a.thing")); //$NON-NLS-1$
        query.setFrom(from);           
        Function function = new Function("concat", new Expression[] { new Constant("a"), new Constant("b")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        CompareCriteria crit = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, function); //$NON-NLS-1$
        query.setCriteria(crit);

        helpTest(sql, 
            "SELECT * FROM a.thing WHERE e1 = concat('a', 'b')",  //$NON-NLS-1$
            query);        
    }

    public void testEscapedFunction2() {
        String sql = "SELECT * FROM a.thing WHERE e1 = {fn convert(5, string)}"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("a.thing")); //$NON-NLS-1$
        query.setFrom(from);           
        Function function = new Function("convert", new Expression[] { new Constant(new Integer(5)), new Constant("string")}); //$NON-NLS-1$ //$NON-NLS-2$
        CompareCriteria crit = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, function); //$NON-NLS-1$
        query.setCriteria(crit);

        helpTest(sql, 
            "SELECT * FROM a.thing WHERE e1 = convert(5, string)",  //$NON-NLS-1$
            query);        
    }

    public void testEscapedFunction3() {
        String sql = "SELECT * FROM a.thing WHERE e1 = {fn cast(5 as string)}"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("a.thing")); //$NON-NLS-1$
        query.setFrom(from);           
        Function function = new Function("cast", new Expression[] { new Constant(new Integer(5)), new Constant("string")}); //$NON-NLS-1$ //$NON-NLS-2$
        CompareCriteria crit = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, function); //$NON-NLS-1$
        query.setCriteria(crit);

        helpTest(sql, "SELECT * FROM a.thing WHERE e1 = cast(5 AS string)", query);         //$NON-NLS-1$
    }

    public void testEscapedFunction4() {
        String sql = "SELECT * FROM a.thing WHERE e1 = {fn concat({fn concat('a', 'b')}, 'c')}"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("a.thing")); //$NON-NLS-1$
        query.setFrom(from);           
        Function func1 = new Function("concat", new Expression[] { new Constant("a"), new Constant("b")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Function func2 = new Function("concat", new Expression[] { func1, new Constant("c")}); //$NON-NLS-1$ //$NON-NLS-2$
        CompareCriteria crit = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, func2); //$NON-NLS-1$
        query.setCriteria(crit);

        helpTest(sql, "SELECT * FROM a.thing WHERE e1 = concat(concat('a', 'b'), 'c')", query);         //$NON-NLS-1$
    }

    public void testFunctionWithUnderscore() {
        String sql = "SELECT yowza_yowza() FROM a.thing"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        Function func1 = new Function("yowza_yowza", new Expression[] { }); //$NON-NLS-1$
        ExpressionSymbol exprSymbol = new ExpressionSymbol("expr", func1); //$NON-NLS-1$
        select.addSymbol(exprSymbol);        
        query.setSelect(select);
        
        From from = new From();
        from.addGroup(new GroupSymbol("a.thing")); //$NON-NLS-1$
        query.setFrom(from);
        
        helpTest(sql, "SELECT yowza_yowza() FROM a.thing", query);         //$NON-NLS-1$
    }

    public void testManyInnerJoins1() {
        String sql = "SELECT * " + //$NON-NLS-1$
            "FROM SQL1.dbo.Customers INNER JOIN SQL1.dbo.Orders " + //$NON-NLS-1$
            "ON SQL1.dbo.Customers.CustomerID = SQL1.dbo.Orders.CustomerID " + //$NON-NLS-1$
            "INNER JOIN SQL1.dbo.order_details " + //$NON-NLS-1$
            "ON SQL1.dbo.Orders.OrderID = SQL1.dbo.order_details.OrderID";            //$NON-NLS-1$

        String sqlExpected = "SELECT * " + //$NON-NLS-1$
            "FROM (SQL1.dbo.Customers INNER JOIN SQL1.dbo.Orders " + //$NON-NLS-1$
            "ON SQL1.dbo.Customers.CustomerID = SQL1.dbo.Orders.CustomerID) " + //$NON-NLS-1$
            "INNER JOIN SQL1.dbo.order_details " + //$NON-NLS-1$
            "ON SQL1.dbo.Orders.OrderID = SQL1.dbo.order_details.OrderID";            //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        
        GroupSymbol g1 = new GroupSymbol("SQL1.dbo.Customers"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("SQL1.dbo.Orders"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("SQL1.dbo.order_details"); //$NON-NLS-1$
        
        ElementSymbol e1 = new ElementSymbol("SQL1.dbo.Customers.CustomerID"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("SQL1.dbo.Orders.CustomerID"); //$NON-NLS-1$
        ElementSymbol e3 = new ElementSymbol("SQL1.dbo.Orders.OrderID"); //$NON-NLS-1$
        ElementSymbol e4 = new ElementSymbol("SQL1.dbo.order_details.OrderID"); //$NON-NLS-1$
        
        List jcrits1 = new ArrayList();
        jcrits1.add(new CompareCriteria(e1, CompareCriteria.EQ, e2));
        List jcrits2 = new ArrayList();
        jcrits2.add(new CompareCriteria(e3, CompareCriteria.EQ, e4));

        JoinPredicate jp1 = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, jcrits1); 
        JoinPredicate jp2 = new JoinPredicate(jp1, new UnaryFromClause(g3), JoinType.JOIN_INNER, jcrits2);       
        
        from.addClause(jp2);
        query.setFrom(from);           

        helpTest(sql, sqlExpected, query);        
    }
  
    public void testManyInnerJoins2() {
        String sql = "SELECT * " + //$NON-NLS-1$
            "FROM A INNER JOIN (B RIGHT OUTER JOIN C ON b1 = c1) " + //$NON-NLS-1$
            "ON a1 = b1 " + //$NON-NLS-1$
            "INNER JOIN D " + //$NON-NLS-1$
            "ON a1 = d1";            //$NON-NLS-1$

        String sqlExpected = "SELECT * " + //$NON-NLS-1$
            "FROM (A INNER JOIN (B RIGHT OUTER JOIN C ON b1 = c1) " + //$NON-NLS-1$
            "ON a1 = b1) " + //$NON-NLS-1$
            "INNER JOIN D " + //$NON-NLS-1$
            "ON a1 = d1";            //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        
        UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("A")); //$NON-NLS-1$
        UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("B")); //$NON-NLS-1$
        UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("C")); //$NON-NLS-1$
        UnaryFromClause g4 = new UnaryFromClause(new GroupSymbol("D")); //$NON-NLS-1$
        
        ElementSymbol e1 = new ElementSymbol("a1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("b1"); //$NON-NLS-1$
        ElementSymbol e3 = new ElementSymbol("c1"); //$NON-NLS-1$
        ElementSymbol e4 = new ElementSymbol("d1"); //$NON-NLS-1$
        
        List jcrits1 = new ArrayList();
        jcrits1.add(new CompareCriteria(e1, CompareCriteria.EQ, e2));
        List jcrits2 = new ArrayList();
        jcrits2.add(new CompareCriteria(e2, CompareCriteria.EQ, e3));
        List jcrits3 = new ArrayList();
        jcrits3.add(new CompareCriteria(e1, CompareCriteria.EQ, e4));

        JoinPredicate jp1 = new JoinPredicate(g2, g3, JoinType.JOIN_RIGHT_OUTER, jcrits2); 
        JoinPredicate jp2 = new JoinPredicate(g1, jp1, JoinType.JOIN_INNER, jcrits1);       
        JoinPredicate jp3 = new JoinPredicate(jp2, g4, JoinType.JOIN_INNER, jcrits3);       
        
        from.addClause(jp3);
        query.setFrom(from);           

        helpTest(sql, sqlExpected, query);        
    }
  
    public void testManyInnerJoins3() {
        String sql = "SELECT * " + //$NON-NLS-1$
            "FROM A INNER JOIN " + //$NON-NLS-1$
            "(B RIGHT OUTER JOIN C ON b1 = c1 " + //$NON-NLS-1$
            "CROSS JOIN D) " + //$NON-NLS-1$
            "ON a1 = d1";            //$NON-NLS-1$

        String sqlExpected = "SELECT * " + //$NON-NLS-1$
            "FROM A INNER JOIN " + //$NON-NLS-1$
            "((B RIGHT OUTER JOIN C ON b1 = c1) " + //$NON-NLS-1$
            "CROSS JOIN D) " + //$NON-NLS-1$
            "ON a1 = d1";            //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        
        UnaryFromClause g1 = new UnaryFromClause(new GroupSymbol("A")); //$NON-NLS-1$
        UnaryFromClause g2 = new UnaryFromClause(new GroupSymbol("B")); //$NON-NLS-1$
        UnaryFromClause g3 = new UnaryFromClause(new GroupSymbol("C")); //$NON-NLS-1$
        UnaryFromClause g4 = new UnaryFromClause(new GroupSymbol("D")); //$NON-NLS-1$
        
        ElementSymbol e1 = new ElementSymbol("a1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("b1"); //$NON-NLS-1$
        ElementSymbol e3 = new ElementSymbol("c1"); //$NON-NLS-1$
        ElementSymbol e4 = new ElementSymbol("d1"); //$NON-NLS-1$
        
        List jcrits1 = new ArrayList();
        jcrits1.add(new CompareCriteria(e2, CompareCriteria.EQ, e3));
        List jcrits2 = new ArrayList();
        jcrits2.add(new CompareCriteria(e1, CompareCriteria.EQ, e4));

        JoinPredicate jp1 = new JoinPredicate(g2, g3, JoinType.JOIN_RIGHT_OUTER, jcrits1); 
        JoinPredicate jp2 = new JoinPredicate(jp1, g4, JoinType.JOIN_CROSS);       
        JoinPredicate jp3 = new JoinPredicate(g1, jp2, JoinType.JOIN_INNER, jcrits2);       
        
        from.addClause(jp3);
        query.setFrom(from);           

        helpTest(sql, sqlExpected, query);        
    }      
      
    public void testLoopStatement(){
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        Select select = new Select();
        ElementSymbol c1 = new ElementSymbol("c1", false); //$NON-NLS-1$
        select.addSymbol(c1);
        select.addSymbol(new ElementSymbol("c2", false));         //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        String intType = new String("integer"); //$NON-NLS-1$
        Statement dStmt = new DeclareStatement(x, intType);
        c1 = new ElementSymbol("mycursor.c1", true); //$NON-NLS-1$
        Statement assignmentStmt = new AssignmentStatement(x, c1);
        Block block = new Block();
        block.addStatement(dStmt); 
        block.addStatement(assignmentStmt);
        
        String cursor = "mycursor"; //$NON-NLS-1$
               
        LoopStatement loopStmt = new LoopStatement(block, query, cursor);
                        
        helpStmtTest("LOOP ON (SELECT c1, c2 FROM m.g) AS mycursor BEGIN DECLARE integer x; x=mycursor.c1; END", //$NON-NLS-1$
             "LOOP ON (SELECT c1, c2 FROM m.g) AS mycursor"+"\n"+ "BEGIN"+"\n"+"DECLARE integer x;"+"\n"+"x = mycursor.c1;" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
             +"\n"+"END", loopStmt);      //$NON-NLS-1$ //$NON-NLS-2$
    }  

    public void testLoopStatementWithOrderBy(){
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        Select select = new Select();
        ElementSymbol c1 = new ElementSymbol("c1", false); //$NON-NLS-1$
        select.addSymbol(c1);
        select.addSymbol(new ElementSymbol("c2", false));         //$NON-NLS-1$

        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(c1);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOrderBy(orderBy);
        
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        String intType = new String("integer"); //$NON-NLS-1$
        Statement dStmt = new DeclareStatement(x, intType);
        c1 = new ElementSymbol("mycursor.c1", true); //$NON-NLS-1$
        Statement assignmentStmt = new AssignmentStatement(x, c1);
        Block block = new Block();
        block.addStatement(dStmt); 
        block.addStatement(assignmentStmt);
        
        String cursor = "mycursor"; //$NON-NLS-1$
               
        LoopStatement loopStmt = new LoopStatement(block, query, cursor);
                        
        helpStmtTest("LOOP ON (SELECT c1, c2 FROM m.g ORDER BY c1) AS mycursor BEGIN DECLARE integer x; x=mycursor.c1; END", //$NON-NLS-1$
             "LOOP ON (SELECT c1, c2 FROM m.g ORDER BY c1) AS mycursor"+"\n"+ "BEGIN"+"\n"+"DECLARE integer x;"+"\n"+"x = mycursor.c1;" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
             +"\n"+"END", loopStmt);      //$NON-NLS-1$ //$NON-NLS-2$
    }  
    
    public void testWhileStatement(){
        ElementSymbol x = new ElementSymbol("x", false); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { x, new Constant(new Integer(1)) }); //$NON-NLS-1$
        Statement assignmentStmt = new AssignmentStatement(x, f);
        Block block = new Block();
        block.addStatement(assignmentStmt);
        Criteria crit = new CompareCriteria(x, CompareCriteria.LT, 
                    new Constant(new Integer(100)));
        WhileStatement whileStmt = new WhileStatement(crit, block);
        helpStmtTest("WHILE (x < 100) BEGIN x=x+1; END", //$NON-NLS-1$
                     "WHILE(x < 100)"+"\n"+ "BEGIN"+"\n"+"x = (x + 1);" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
                     +"\n"+"END", whileStmt); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testBreakStatement(){
        Statement breakStmt = new BreakStatement();
        helpStmtTest("break;", "BREAK;", breakStmt); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testContinueStatement(){
        Statement contStmt = new ContinueStatement();
        helpStmtTest("continue;", "CONTINUE;", contStmt); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testVirtualProcedure(){        
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        String intType = new String("integer"); //$NON-NLS-1$
        Statement dStmt = new DeclareStatement(x, intType);
        
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        Select select = new Select();
        ElementSymbol c1 = new ElementSymbol("c1", false); //$NON-NLS-1$
        select.addSymbol(c1);
        select.addSymbol(new ElementSymbol("c2", false));         //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        x = new ElementSymbol("x"); //$NON-NLS-1$
        c1 = new ElementSymbol("mycursor.c1", true); //$NON-NLS-1$
        Statement assignmentStmt = new AssignmentStatement(x, c1);
        Block block = new Block(); 
        block.addStatement(assignmentStmt);
        
        Block ifBlock = new Block();
        Statement continueStmt = new ContinueStatement();
        ifBlock.addStatement(continueStmt);
        Criteria crit = new CompareCriteria(x, CompareCriteria.GT, 
        new Constant(new Integer(5)));
        IfStatement ifStmt = new IfStatement(crit, ifBlock);
        block.addStatement(ifStmt); 
        
        String cursor = "mycursor";                //$NON-NLS-1$
        LoopStatement loopStmt = new LoopStatement(block, query, cursor);
        
        block = new Block();        
        block.addStatement(dStmt);
        block.addStatement(loopStmt);
        CommandStatement cmdStmt = new CommandStatement(query);
        block.addStatement(cmdStmt);
        
        CreateUpdateProcedureCommand virtualProcedureCommand = new CreateUpdateProcedureCommand();
        virtualProcedureCommand.setBlock(block);
        virtualProcedureCommand.setUpdateProcedure(false);
        
        helpTest("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT c1, c2 FROM m.g) AS mycursor BEGIN x=mycursor.c1; IF(x > 5) BEGIN CONTINUE; END END SELECT c1, c2 FROM m.g; END", //$NON-NLS-1$
        "CREATE VIRTUAL PROCEDURE\nBEGIN\nDECLARE integer x;\n" //$NON-NLS-1$
        + "LOOP ON (SELECT c1, c2 FROM m.g) AS mycursor\nBEGIN\n" //$NON-NLS-1$
        + "x = mycursor.c1;\nIF(x > 5)\nBEGIN\nCONTINUE;\nEND\nEND\n" //$NON-NLS-1$
        + "SELECT c1, c2 FROM m.g;\nEND", virtualProcedureCommand); //$NON-NLS-1$

    }
    
    public void testScalarSubqueryExpressionInSelect(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        s2.addSymbol(new ExpressionSymbol("expr", new ScalarSubquery(q1))); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT e1, (SELECT e1 FROM m.g1) FROM m.g2",  //$NON-NLS-1$
                 "SELECT e1, (SELECT e1 FROM m.g1) FROM m.g2", //$NON-NLS-1$
                 q2);
    }    

    public void testScalarSubqueryExpressionInSelect2(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ExpressionSymbol("expr", new ScalarSubquery(q1))); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT (SELECT e1 FROM m.g1) FROM m.g2",  //$NON-NLS-1$
                 "SELECT (SELECT e1 FROM m.g1) FROM m.g2", //$NON-NLS-1$
                 q2);
    }   

    public void testScalarSubqueryExpressionInSelect3(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ExpressionSymbol("expr", new ScalarSubquery(q1))); //$NON-NLS-1$
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT (SELECT e1 FROM m.g1), e1 FROM m.g2",  //$NON-NLS-1$
                 "SELECT (SELECT e1 FROM m.g1), e1 FROM m.g2", //$NON-NLS-1$
                 q2);
    }   

    public void testScalarSubqueryExpressionWithAlias(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        s2.addSymbol(new AliasSymbol("X", new ExpressionSymbol("expr", new ScalarSubquery(q1)))); //$NON-NLS-1$ //$NON-NLS-2$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT e1, (SELECT e1 FROM m.g1) as X FROM m.g2",  //$NON-NLS-1$
                 "SELECT e1, (SELECT e1 FROM m.g1) AS X FROM m.g2", //$NON-NLS-1$
                 q2);
    }   

    public void testScalarSubqueryExpressionInComplexExpression() throws QueryParserException {
        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
       
        s2.addSymbol(new AliasSymbol("X", new ExpressionSymbol("expr", QueryParser.getQueryParser().parseExpression("(SELECT e1 FROM m.g1) + 2")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT e1, ((SELECT e1 FROM m.g1) + 2) as X FROM m.g2",  //$NON-NLS-1$
                 "SELECT e1, ((SELECT e1 FROM m.g1) + 2) AS X FROM m.g2", //$NON-NLS-1$
                 q2);
    }

    public void testScalarSubqueryExpressionInComplexExpression2() throws QueryParserException{
        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        
        s2.addSymbol(new AliasSymbol("X", new ExpressionSymbol("expr", QueryParser.getQueryParser().parseExpression("3 + (SELECT e1 FROM m.g1)")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT e1, (3 + (SELECT e1 FROM m.g1)) as X FROM m.g2",  //$NON-NLS-1$
                 "SELECT e1, (3 + (SELECT e1 FROM m.g1)) AS X FROM m.g2", //$NON-NLS-1$
                 q2);
    } 

    public void testScalarSubqueryExpressionInComplexExpression3() throws QueryParserException{
        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        
        s2.addSymbol(new AliasSymbol("X", new ExpressionSymbol("expr", QueryParser.getQueryParser().parseExpression("(SELECT e1 FROM m.g1) + (SELECT e3 FROM m.g3)")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT e1, ((SELECT e1 FROM m.g1) + (SELECT e3 FROM m.g3)) as X FROM m.g2",  //$NON-NLS-1$
                 "SELECT e1, ((SELECT e1 FROM m.g1) + (SELECT e3 FROM m.g3)) AS X FROM m.g2", //$NON-NLS-1$
                 q2);
    }

    public void testScalarSubqueryExpressionInFunction() throws QueryParserException{
        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        
        s2.addSymbol(new AliasSymbol("X", new ExpressionSymbol("expr", QueryParser.getQueryParser().getQueryParser().parseExpression("length((SELECT e1 FROM m.g1))")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        helpTest("SELECT e1, length((SELECT e1 FROM m.g1)) as X FROM m.g2",  //$NON-NLS-1$
                 "SELECT e1, length((SELECT e1 FROM m.g1)) AS X FROM m.g2", //$NON-NLS-1$
                 q2);
    } 
    
    public void testBadScalarSubqueryExpression() {
        helpException("SELECT e1, length(SELECT e1 FROM m.g1) as X FROM m.g2"); //$NON-NLS-1$
    }

    public void testExistsPredicateCriteria(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Criteria existsCrit = new ExistsCriteria(q1);
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);
        q2.setCriteria(existsCrit);

        helpTest("SELECT e1 FROM m.g2 WHERE Exists (SELECT e1 FROM m.g1)",  //$NON-NLS-1$
                 "SELECT e1 FROM m.g2 WHERE EXISTS (SELECT e1 FROM m.g1)", //$NON-NLS-1$
                 q2);            
    }
    
    public void testAnyQuantifierSubqueryComparePredicate(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Criteria left = new SubqueryCompareCriteria(new ElementSymbol("e3"), q1, SubqueryCompareCriteria.GE, SubqueryCompareCriteria.ANY); //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);
        q2.setCriteria(left);

        helpTest("SELECT e1 FROM m.g2 WHERE e3 >= ANY (SELECT e1 FROM m.g1)",  //$NON-NLS-1$
                 "SELECT e1 FROM m.g2 WHERE e3 >= ANY (SELECT e1 FROM m.g1)", //$NON-NLS-1$
                 q2);            

    }    

    public void testSomeQuantifierSubqueryComparePredicate(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Criteria left = new SubqueryCompareCriteria(new ElementSymbol("e3"), q1, SubqueryCompareCriteria.GT, SubqueryCompareCriteria.SOME); //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);
        q2.setCriteria(left);

        helpTest("SELECT e1 FROM m.g2 WHERE e3 > some (SELECT e1 FROM m.g1)",  //$NON-NLS-1$
                 "SELECT e1 FROM m.g2 WHERE e3 > SOME (SELECT e1 FROM m.g1)", //$NON-NLS-1$
                 q2);            

    }  

    public void testAllQuantifierSubqueryComparePredicate(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Criteria left = new SubqueryCompareCriteria(new ElementSymbol("e3"), q1, SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL); //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);
        q2.setCriteria(left);

        helpTest("SELECT e1 FROM m.g2 WHERE e3 = all (SELECT e1 FROM m.g1)",  //$NON-NLS-1$
                 "SELECT e1 FROM m.g2 WHERE e3 = ALL (SELECT e1 FROM m.g1)", //$NON-NLS-1$
                 q2);            

    } 
    
    public void testScalarSubqueryComparePredicate(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Criteria left = new CompareCriteria(new ElementSymbol("e3"), SubqueryCompareCriteria.LT, new ScalarSubquery(q1)); //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);
        q2.setCriteria(left);

        helpTest("SELECT e1 FROM m.g2 WHERE e3 < (SELECT e1 FROM m.g1)",  //$NON-NLS-1$
                 "SELECT e1 FROM m.g2 WHERE e3 < (SELECT e1 FROM m.g1)", //$NON-NLS-1$
                 q2);            

    }

    public void testSelectInto(){
        GroupSymbol g = new GroupSymbol("m.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        Select select = new Select();
        ElementSymbol c1 = new ElementSymbol("c1", false); //$NON-NLS-1$
        select.addSymbol(c1);
        select.addSymbol(new ElementSymbol("c2", false));   //$NON-NLS-1$
        
        Into into = new Into(new GroupSymbol("#temp")); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        q.setInto(into);
        helpTest("SELECT c1, c2 INTO #temp FROM m.g",  //$NON-NLS-1$
                 "SELECT c1, c2 INTO #temp FROM m.g", //$NON-NLS-1$
                 q);  
    }
    
    public void testCaseExpression1() {
        CaseExpression expr = TestCaseExpression.example(4);
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("z")); //$NON-NLS-1$
        // The parser hard-codes the name "expr"
        select.addSymbol(new ExpressionSymbol("expr", expr)); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        
        String query = new StringBuffer("SELECT y, z, ") //$NON-NLS-1$
            .append("CASE x") //$NON-NLS-1$
            .append(" WHEN 'a' THEN 0") //$NON-NLS-1$
            .append(" WHEN 'b' THEN 1") //$NON-NLS-1$
            .append(" WHEN 'c' THEN 2") //$NON-NLS-1$
            .append(" WHEN 'd' THEN 3") //$NON-NLS-1$
            .append(" ELSE 9999") //$NON-NLS-1$
            .append(" END") //$NON-NLS-1$
            .append(" FROM m.g").toString(); //$NON-NLS-1$
        
        helpTest(query, query, q);
    }
    
    public void testCaseExpression2() {
        CaseExpression expr = TestCaseExpression.example(4);
        expr.setElseExpression(null);
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("z")); //$NON-NLS-1$
        // The parser hard-codes the name "expr"
        select.addSymbol(new ExpressionSymbol("expr", expr)); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        
        String query = new StringBuffer("SELECT y, z, ") //$NON-NLS-1$
            .append("CASE x") //$NON-NLS-1$
            .append(" WHEN 'a' THEN 0") //$NON-NLS-1$
            .append(" WHEN 'b' THEN 1") //$NON-NLS-1$
            .append(" WHEN 'c' THEN 2") //$NON-NLS-1$
            .append(" WHEN 'd' THEN 3") //$NON-NLS-1$
            .append(" END") //$NON-NLS-1$
            .append(" FROM m.g").toString(); //$NON-NLS-1$
       
        helpTest(query, query, q);
    }
    
    public void testCaseExpression3() {
        SearchedCaseExpression expr = TestSearchedCaseExpression.example2(4);
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria where = new CompareCriteria(new ElementSymbol("z"), CompareCriteria.EQ, expr); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        q.setCriteria(where);
        
        String query = new StringBuffer("SELECT y FROM m.g ") //$NON-NLS-1$
            .append("WHERE z = CASE") //$NON-NLS-1$
            .append(" WHEN x = 'a' THEN 0") //$NON-NLS-1$
            .append(" WHEN x = 'b' THEN 1") //$NON-NLS-1$
            .append(" WHEN x = 'c' THEN 2") //$NON-NLS-1$
            .append(" WHEN x = 'd' THEN 3") //$NON-NLS-1$
            .append(" ELSE 9999") //$NON-NLS-1$
            .append(" END").toString(); //$NON-NLS-1$
        helpTest(query, query, q);
    }
    
    public void testSearchedCaseExpression1() {
        SearchedCaseExpression expr = TestSearchedCaseExpression.example(4);
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("z")); //$NON-NLS-1$
        // The parser hard-codes the name "expr"
        select.addSymbol(new ExpressionSymbol("expr", expr)); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        
        String query = new StringBuffer("SELECT y, z, ") //$NON-NLS-1$
            .append("CASE") //$NON-NLS-1$
            .append(" WHEN x = 0 THEN 0") //$NON-NLS-1$
            .append(" WHEN x = 1 THEN 1") //$NON-NLS-1$
            .append(" WHEN x = 2 THEN 2") //$NON-NLS-1$
            .append(" WHEN x = 3 THEN 3") //$NON-NLS-1$
            .append(" ELSE 9999") //$NON-NLS-1$
            .append(" END") //$NON-NLS-1$
            .append(" FROM m.g").toString(); //$NON-NLS-1$
        helpTest(query, query, q);
    }
    
    public void testSearchedCaseExpression2() {
        SearchedCaseExpression expr = TestSearchedCaseExpression.example(4);
        expr.setElseExpression(null);
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("z")); //$NON-NLS-1$
        // The parser hard-codes the name "expr"
        select.addSymbol(new ExpressionSymbol("expr", expr)); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        
        String query = new StringBuffer("SELECT y, z, ") //$NON-NLS-1$
            .append("CASE") //$NON-NLS-1$
            .append(" WHEN x = 0 THEN 0") //$NON-NLS-1$
            .append(" WHEN x = 1 THEN 1") //$NON-NLS-1$
            .append(" WHEN x = 2 THEN 2") //$NON-NLS-1$
            .append(" WHEN x = 3 THEN 3") //$NON-NLS-1$
            .append(" END") //$NON-NLS-1$
            .append(" FROM m.g").toString(); //$NON-NLS-1$
        helpTest(query, query, q);
    }
    
    public void testSearchedCaseExpression3() {
        SearchedCaseExpression expr = TestSearchedCaseExpression.example(4);
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria where = new CompareCriteria(new ElementSymbol("z"), CompareCriteria.EQ, expr); //$NON-NLS-1$
        Query q = new Query();
        q.setSelect(select);
        q.setFrom(from);
        q.setCriteria(where);
        
        String query = new StringBuffer("SELECT y FROM m.g ") //$NON-NLS-1$
            .append("WHERE z = CASE") //$NON-NLS-1$
            .append(" WHEN x = 0 THEN 0") //$NON-NLS-1$
            .append(" WHEN x = 1 THEN 1") //$NON-NLS-1$
            .append(" WHEN x = 2 THEN 2") //$NON-NLS-1$
            .append(" WHEN x = 3 THEN 3") //$NON-NLS-1$
            .append(" ELSE 9999") //$NON-NLS-1$
            .append(" END").toString(); //$NON-NLS-1$
        helpTest(query, query, q);
    }

    public void testAndOrPrecedence_1575() {
        Select s = new Select();
        s.addSymbol(new AllSymbol());
        From f = new From();
        f.addGroup(new GroupSymbol("m.g1")); //$NON-NLS-1$
        CompareCriteria c1 = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(0))); //$NON-NLS-1$
        CompareCriteria c2 = new CompareCriteria(new ElementSymbol("e2"), CompareCriteria.EQ, new Constant(new Integer(1))); //$NON-NLS-1$
        CompareCriteria c3 = new CompareCriteria(new ElementSymbol("e3"), CompareCriteria.EQ, new Constant(new Integer(3))); //$NON-NLS-1$
        CompoundCriteria cc1 = new CompoundCriteria(CompoundCriteria.AND, c2, c3);        
        CompoundCriteria cc2 = new CompoundCriteria(CompoundCriteria.OR, c1, cc1);
        Query q = new Query();
        q.setSelect(s);
        q.setFrom(f);
        q.setCriteria(cc2);
        
        helpTest("SELECT * FROM m.g1 WHERE e1=0 OR e2=1 AND e3=3", //$NON-NLS-1$
        "SELECT * FROM m.g1 WHERE (e1 = 0) OR ((e2 = 1) AND (e3 = 3))", q);                          //$NON-NLS-1$
    }

    public void testAndOrPrecedence2_1575() {
        Select s = new Select();
        s.addSymbol(new AllSymbol());
        From f = new From();
        f.addGroup(new GroupSymbol("m.g1")); //$NON-NLS-1$
        CompareCriteria c1 = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(0))); //$NON-NLS-1$
        CompareCriteria c2 = new CompareCriteria(new ElementSymbol("e2"), CompareCriteria.EQ, new Constant(new Integer(1))); //$NON-NLS-1$
        CompareCriteria c3 = new CompareCriteria(new ElementSymbol("e3"), CompareCriteria.EQ, new Constant(new Integer(3))); //$NON-NLS-1$
        CompoundCriteria cc1 = new CompoundCriteria(CompoundCriteria.AND, c1, c2);        
        CompoundCriteria cc2 = new CompoundCriteria(CompoundCriteria.OR, cc1, c3);
        Query q = new Query();
        q.setSelect(s);
        q.setFrom(f);
        q.setCriteria(cc2);
        
        helpTest("SELECT * FROM m.g1 WHERE e1=0 AND e2=1 OR e3=3", //$NON-NLS-1$
        "SELECT * FROM m.g1 WHERE ((e1 = 0) AND (e2 = 1)) OR (e3 = 3)", q);                          //$NON-NLS-1$
    }

    /** 
     * 
     * @since 4.2
     */
    private void helpTestCompoundNonJoinCriteria(String sqlPred, PredicateCriteria predCrit) {
        Select s = new Select();
        s.addSymbol(new AllSymbol());
        From f = new From();
        
        CompareCriteria c1 = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(0))); //$NON-NLS-1$
        CompoundCriteria cc1 = new CompoundCriteria(CompoundCriteria.AND, c1, predCrit);
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(new GroupSymbol("m.g1")), new UnaryFromClause(new GroupSymbol("m.g2")), JoinType.JOIN_INNER, cc1); //$NON-NLS-1$ //$NON-NLS-2$
        f.addClause(jp);
        
        Query q = new Query();
        q.setSelect(s);
        q.setFrom(f);
        
        helpTest("SELECT * FROM m.g1 JOIN m.g2 ON e1=0 AND " + sqlPred, //$NON-NLS-1$
        "SELECT * FROM m.g1 INNER JOIN m.g2 ON e1 = 0 AND " + sqlPred, q); //$NON-NLS-1$

    }


    public void testCompoundNonJoinCriteriaInFromWithComparisonCriteria() {        
        CompareCriteria c2 = new CompareCriteria(new ElementSymbol("e2"), CompareCriteria.EQ, new Constant(new Integer(1))); //$NON-NLS-1$
        helpTestCompoundNonJoinCriteria("e2 = 1", c2);     //$NON-NLS-1$
    }
    
    public void testCompoundNonJoinCriteriaInFromWithIsNull() {        
        helpTestCompoundNonJoinCriteria("e2 IS NULL", new IsNullCriteria(new ElementSymbol("e2")));     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCompoundNonJoinCriteriaInFromUWithIN() {        
        Collection values = new ArrayList();
        values.add(new Constant(new Integer(0)));
        values.add(new Constant(new Integer(1)));
        PredicateCriteria crit = new SetCriteria(new ElementSymbol("e2"), values); //$NON-NLS-1$
        helpTestCompoundNonJoinCriteria("e2 IN (0, 1)", crit);     //$NON-NLS-1$
    }

    public void testCompoundNonJoinCriteriaInFromUWithLIKE() {        
        PredicateCriteria crit = new MatchCriteria(new ElementSymbol("e2"), new Constant("%")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestCompoundNonJoinCriteria("e2 LIKE '%'", crit);     //$NON-NLS-1$
    }

    public void testCompoundNonJoinCriteria_defect15167_1() throws Exception {   
        QueryParser.getQueryParser().parseCommand("SELECT A.alert_id, A.primary_entity_name, A.primary_entity_level_code, A.alert_description, A.create_date, A.alert_risk_score, S.scenario_name, A.alert_status_code, A.process_id, A.actual_values_text, S.SCENARIO_CATEGORY_DESC, A.primary_entity_number, A.scenario_id, A.primary_entity_key FROM (FSK_ALERT AS A LEFT OUTER JOIN FSK_SCENARIO AS S ON A.scenario_id = S.scenario_id) INNER JOIN FSC_ACCOUNT_DIM AS C ON A.primary_entity_key = C.ACCOUNT_KEY  AND ((S.current_ind = 'Y') OR (S.current_ind IS NULL)) WHERE (A.primary_entity_level_code = 'ACC') AND (C.ACCOUNT_KEY = 23923) AND (A.logical_delete_ind = 'N') OPTION PLANONLY"); //$NON-NLS-1$
    }

    public void testCompoundNonJoinCriteria_defect15167_2() throws Exception {   
        QueryParser.getQueryParser().parseCommand("SELECT A.alert_id, A.primary_entity_name, A.primary_entity_level_code, A.alert_description, A.create_date, A.alert_risk_score, S.scenario_name, A.alert_status_code, A.process_id, A.actual_values_text, S.SCENARIO_CATEGORY_DESC, A.primary_entity_number, A.scenario_id, A.primary_entity_key FROM (FSK_ALERT AS A LEFT OUTER JOIN FSK_SCENARIO AS S ON A.scenario_id = S.scenario_id) INNER JOIN FSC_ACCOUNT_DIM AS C ON A.primary_entity_key = C.ACCOUNT_KEY  AND (S.current_ind = 'Y' OR S.current_ind IS NULL) WHERE (A.primary_entity_level_code = 'ACC') AND (C.ACCOUNT_KEY = 23923) AND (A.logical_delete_ind = 'N') OPTION PLANONLY"); //$NON-NLS-1$
    }

    public void testCompoundNonJoinCriteria_defect15167_3() throws Exception {   
        QueryParser.getQueryParser().parseCommand("SELECT A.alert_id, A.primary_entity_name, A.primary_entity_level_code, A.alert_description, A.create_date, A.alert_risk_score, S.scenario_name, A.alert_status_code, A.process_id, A.actual_values_text, S.SCENARIO_CATEGORY_DESC, A.primary_entity_number, A.scenario_id, A.primary_entity_key FROM (FSK_ALERT AS A LEFT OUTER JOIN FSK_SCENARIO AS S ON A.scenario_id = S.scenario_id) INNER JOIN FSC_ACCOUNT_DIM AS C ON (A.primary_entity_key = C.ACCOUNT_KEY AND (S.current_ind = 'Y' OR S.current_ind IS NULL)) WHERE (A.primary_entity_level_code = 'ACC') AND (C.ACCOUNT_KEY = 23923) AND (A.logical_delete_ind = 'N') OPTION PLANONLY"); //$NON-NLS-1$
    }

    public void testCompoundNonJoinCriteria_defect15167_4() throws Exception {   
        QueryParser.getQueryParser().parseCommand("SELECT A.alert_id, A.primary_entity_name, A.primary_entity_level_code, A.alert_description, A.create_date, A.alert_risk_score, S.scenario_name, A.alert_status_code, A.process_id, A.actual_values_text, S.SCENARIO_CATEGORY_DESC, A.primary_entity_number, A.scenario_id, A.primary_entity_key FROM (FSK_ALERT AS A LEFT OUTER JOIN FSK_SCENARIO AS S ON A.scenario_id = S.scenario_id) INNER JOIN FSC_ACCOUNT_DIM AS C ON (A.primary_entity_key = C.ACCOUNT_KEY AND S.current_ind = 'Y' OR S.current_ind IS NULL) WHERE (A.primary_entity_level_code = 'ACC') AND (C.ACCOUNT_KEY = 23923) AND (A.logical_delete_ind = 'N') OPTION PLANONLY"); //$NON-NLS-1$
    }
    
    public void testFunctionInGroupBy() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT SUM(s), elem+1 FROM m.g GROUP BY elem+1"); //$NON-NLS-1$
    }

    public void testCaseInGroupBy() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT SUM(elem+1), CASE elem WHEN 0 THEN 1 ELSE 2 END AS c FROM m.g GROUP BY CASE elem WHEN 0 THEN 1 ELSE 2 END"); //$NON-NLS-1$
    }

    public void testNationCharString() throws Exception {
        Query query = (Query) QueryParser.getQueryParser().parseCommand("SELECT N'blah' FROM m.g"); //$NON-NLS-1$
        Select select = query.getSelect();
        ExpressionSymbol s = (ExpressionSymbol) select.getSymbol(0);
        Constant c = (Constant) s.getExpression();
        assertEquals(c, new Constant("blah")); //$NON-NLS-1$
    }

    public void testNationCharString2() throws Exception {
        Query query = (Query) QueryParser.getQueryParser().parseCommand("SELECT DISTINCT TABLE_QUALIFIER, NULL AS TABLE_OWNER, NULL AS TABLE_NAME, NULL AS TABLE_TYPE, NULL AS REMARKS FROM ATIODBCSystem.OA_TABLES  WHERE TABLE_QUALIFIER LIKE N'%'  ESCAPE '\\'  ORDER BY TABLE_QUALIFIER  "); //$NON-NLS-1$
        MatchCriteria matchCrit = (MatchCriteria) query.getCriteria();
        Constant c = (Constant) matchCrit.getRightExpression();
        assertEquals(c, new Constant("%")); //$NON-NLS-1$
    }
    
    public void testScalarSubquery() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT (SELECT 1) FROM x"); //$NON-NLS-1$
    }

    public void testElementInDoubleQuotes() throws Exception {
        GroupSymbol g = new GroupSymbol("x"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        ElementSymbol e =  new ElementSymbol("foo"); //$NON-NLS-1$ 
		Select select = new Select();
		select.addSymbol(e);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT \"foo\" FROM x",  //$NON-NLS-1$
				 "SELECT foo FROM x",  //$NON-NLS-1$
				 query, info);                
    }
    
    public void testElementInDoubleQuotes_Insert() throws Exception {
        GroupSymbol g = new GroupSymbol("x"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        ElementSymbol e =  new ElementSymbol("foo"); //$NON-NLS-1$ 

        Insert query = new Insert(g, new ArrayList(), new ArrayList());
        query.addVariable(e);
        query.addValue(new Constant("bar", String.class)); //$NON-NLS-1$
        
        helpTest("insert into x (\"foo\") values ('bar')",  //$NON-NLS-1$
                 "INSERT INTO x (foo) VALUES ('bar')",  //$NON-NLS-1$
                 query, info);                
    }
    
    public void testElementInDoubleQuotes_Update() throws Exception {
        GroupSymbol g = new GroupSymbol("x"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        ElementSymbol e =  new ElementSymbol("foo"); //$NON-NLS-1$ 
        Update query = new Update();
        query.setGroup(g);
        query.addChange(e, new Constant("bar", String.class)); //$NON-NLS-1$
        
        helpTest("update x set \"foo\"='bar'",  //$NON-NLS-1$
                 "UPDATE x SET foo = 'bar'",  //$NON-NLS-1$
                 query, info);                
    }  
    
    public void testElementInDoubleQuotes_delete() throws Exception {
        GroupSymbol g = new GroupSymbol("x"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        ElementSymbol e =  new ElementSymbol("foo"); //$NON-NLS-1$ 
        CompareCriteria c = new CompareCriteria(e, CompareCriteria.EQ, new Constant("bar", String.class)); //$NON-NLS-1$
        Delete query = new Delete(g,c);
        
        helpTest("delete from x where \"foo\"='bar'",  //$NON-NLS-1$
                 "DELETE FROM x WHERE foo = 'bar'",  //$NON-NLS-1$
                 query, info);                
    }    
    
    public void testAliasInDoubleQuotes() throws Exception {        
        GroupSymbol g = new GroupSymbol("x"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        AliasSymbol as = new AliasSymbol("fooAlias", new ElementSymbol("fooKey")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
		helpTest("SELECT fooKey AS \"fooAlias\" FROM x",  //$NON-NLS-1$
				 "SELECT fooKey AS fooAlias FROM x",  //$NON-NLS-1$
				 query, info);		
    }
    
    public void testAliasInDoubleQuotesWithQuotedGroup() throws Exception {
        
        GroupSymbol g = new GroupSymbol("x.y.z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        AliasSymbol as = new AliasSymbol("fooAlias", new ElementSymbol("fooKey")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);
		
		ElementSymbol a = new ElementSymbol("x.y.z.id");         //$NON-NLS-1$
		Constant c = new Constant(new Integer(10));
        Criteria crit = new CompareCriteria(a, CompareCriteria.EQ, c);		

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		
		helpTest("SELECT fooKey AS \"fooAlias\" FROM \"x.y\".z where x.\"y.z\".id = 10",  //$NON-NLS-1$
		         "SELECT fooKey AS fooAlias FROM x.y.z WHERE x.y.z.id = 10",  //$NON-NLS-1$
				 query, info);		        
    }

    public void testSingleQuotedConstant() throws Exception {        
        
        GroupSymbol g = new GroupSymbol("x.y.z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
                
        Constant as = new Constant("fooString"); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(new ExpressionSymbol("expr", as)); //$NON-NLS-1$
		
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
				        
		helpTest("SELECT 'fooString' FROM \"x.y.z\"",  //$NON-NLS-1$
		        "SELECT 'fooString' FROM x.y.z",  //$NON-NLS-1$
				 query, info);        
    }

    public void testAliasInSingleQuotes() throws Exception {
        
        GroupSymbol g = new GroupSymbol("x.y.z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        AliasSymbol as = new AliasSymbol("fooAlias", new ElementSymbol("fooKey")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
        helpTest("SELECT fooKey 'fooAlias' FROM x.\"y\".z", //$NON-NLS-1$
                "SELECT fooKey AS fooAlias FROM x.y.z", //$NON-NLS-1$
                query, info); 
    }
    
    public void testAliasInSingleQuotes2() throws Exception {

        GroupSymbol g = new GroupSymbol("x.y.z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        AliasSymbol as = new AliasSymbol("fooAlias", new ElementSymbol("fooKey")); //$NON-NLS-1$ //$NON-NLS-2$
		Select select = new Select();
		select.addSymbol(as);

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
        helpTest("SELECT \"fooKey\" AS 'fooAlias' FROM x.y.z", //$NON-NLS-1$
                "SELECT fooKey AS fooAlias FROM x.y.z", //$NON-NLS-1$
                query, info); 		
    }
    
    public void testFullyQualifiedElementWithSlashes() throws Exception {
        GroupSymbol g = new GroupSymbol("x.y.z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
		Select select = new Select();
		select.addSymbol(new ElementSymbol("x.y.z.fooKey")); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		
        helpTest("SELECT \"x/y\".z.fooKey FROM \"x/y\".z", //$NON-NLS-1$
                "SELECT x.y.z.fooKey FROM x.y.z", //$NON-NLS-1$
                query); 		
    }    
    
    public void testFullyQualifiedVaribleWithSlashes() throws Exception {
        GroupSymbol g = new GroupSymbol("x.y.z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
		Select select = new Select();
		select.addSymbol(new ElementSymbol("x.y.z.fooKey")); //$NON-NLS-1$
		AliasSymbol alias = new AliasSymbol("key", new ElementSymbol("x.y.z.key2")); //$NON-NLS-1$ //$NON-NLS-2$
		select.addSymbol(alias);
		
		Criteria crit = new CompareCriteria(new ElementSymbol("x.y.z.fooKey"), SubqueryCompareCriteria.GT, new Constant(new Integer(10))); //$NON-NLS-1$
		
		OrderBy orderby = new OrderBy();
		orderby.addVariable(new ElementSymbol("x.y.z.fooKey"), true); //$NON-NLS-1$
	
		GroupBy groupby = new GroupBy();
		groupby.addSymbol(new ElementSymbol("key")); //$NON-NLS-1$
		
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(crit);
		query.setOrderBy(orderby);
		query.setGroupBy(groupby);
		
        helpTest("SELECT x/y.z.fooKey, x/y.z.key2 as 'key' FROM x/y.z where x/y.z.fooKey > 10 group by key order by x/y.z.fooKey", //$NON-NLS-1$
                "SELECT x.y.z.fooKey, x.y.z.key2 AS key FROM x.y.z WHERE x.y.z.fooKey > 10 GROUP BY key ORDER BY x.y.z.fooKey", //$NON-NLS-1$
                query); 		
    } 

    /** QUERY Tool Format*/
    public void testQueryWithQuotes_MSQuery() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT \"PART_COLOR\", \"PART_ID\", \"PART_NAME\", \"PART_WEIGHT\" FROM \"VirtualParts/base\".\"Parts\""); //$NON-NLS-1$
    }
     
    /** MS Query Format **/
    public void testQueryWithQuotes_MSQuery2() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT Core.ModelType.Value AS 'ModelType', DtcBase.Metamodels.DisplayName AS 'MetaModel', DtcBase.Models.Name AS 'ModelName', DtcBase.Models.Version, Core.ModelAnnotation.PrimaryMetamodelUri, DtcBase.Models.TransactionId AS 'ModelID'"); //$NON-NLS-1$
    }

    /** MS Access Format**/
    public void testQueryWithQuotes_MSAccess() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT \"PART_COLOR\" ,\"PART_ID\" ,\"PART_NAME\" ,\"PART_WEIGHT\"  FROM \"parts_oracle/DEV_RRAMESH\".\"PARTS\""); //$NON-NLS-1$
    }

    /** BO Business View Manager**/
    public void testQueryWithQuotes_BODesigner() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT DISTINCT \"PARTS\".\"PART_NAME\" FROM   \"parts_oracle/DEV_RRAMESH\".\"PARTS\" \"PARTS\""); //$NON-NLS-1$
    }

    /** Crystal Reports **/
    public void testQueryWithQuotes_CrystalReports() throws Exception {
        QueryParser.getQueryParser().parseCommand("SELECT \"Oracle_PARTS\".\"PART_COLOR\", \"Oracle_PARTS\".\"PART_ID\", \"Oracle_PARTS\".\"PART_NAME\", \"Oracle_PARTS\".\"PART_WEIGHT\", \"SQL_PARTS\".\"PART_COLOR\", \"SQL_PARTS\".\"PART_ID\", \"SQL_PARTS\".\"PART_NAME\", \"SQL_PARTS\".\"PART_WEIGHT\" FROM   \"parts_oracle/DEV_RRAMESH\".\"PARTS\" \"Oracle_PARTS\", \"parts_sqlserver/dv_rreddy/dv_rreddy\".\"PARTS\" \"SQL_PARTS\" WHERE  (\"Oracle_PARTS\".\"PART_ID\"=\"SQL_PARTS\".\"PART_ID\")"); //$NON-NLS-1$
    }

    public void testOrderByWithNumbers_InQuotes() throws Exception {       
        GroupSymbol g = new GroupSymbol("z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
		Select select = new Select();
		select.addSymbol(new ElementSymbol("x")); //$NON-NLS-1$
		select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$

		OrderBy orderby = new OrderBy();
		orderby.addVariable(new ElementSymbol("1"), true); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setOrderBy(orderby);
		
		helpTest("SELECT x, y from z order by \"1\"", "SELECT x, y FROM z ORDER BY 1", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testOrderByWithNumbers_AsInt() throws Exception {
        GroupSymbol g = new GroupSymbol("z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
		Select select = new Select();
		select.addSymbol(new ElementSymbol("x")); //$NON-NLS-1$
		select.addSymbol(new ElementSymbol("y")); //$NON-NLS-1$

		OrderBy orderby = new OrderBy();
		orderby.addVariable(new ElementSymbol("1"), true); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setOrderBy(orderby);        
                
		helpTest("SELECT x, y FROM z order by 1", "SELECT x, y FROM z ORDER BY 1", query); //$NON-NLS-1$ //$NON-NLS-2$        
    }
   
    public void testOrderByWithNumbers_AsNegitiveInt() throws Exception {
        try {
        QueryParser.getQueryParser().parseCommand("SELECT x, y FROM z order by -1"); //$NON-NLS-1$
        fail("order by should not have negitive values"); //$NON-NLS-1$
        }catch(Exception e) {
            // this is expected.
        }
    } 
    
    public void testOrderByWithNumbers_Expression() throws Exception {
        GroupSymbol g = new GroupSymbol("z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        Function f = new Function("+", new Expression[] {new ElementSymbol("x"), new ElementSymbol("y")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);
		
		OrderBy orderby = new OrderBy();
		orderby.addVariable(new ElementSymbol("1"), true); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setOrderBy(orderby);           
        
        helpTest("SELECT x+y FROM z order by 1", "SELECT (x + y) FROM z ORDER BY 1", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testOrderByWithNumbers_ScalarFunction() throws Exception {
        GroupSymbol g = new GroupSymbol("z"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        
        Function f = new Function("concat", new Expression[] {new ElementSymbol("x", false), new Constant("5")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionSymbol es = new ExpressionSymbol("expr", f); //$NON-NLS-1$
		Select select = new Select();
		select.addSymbol(es);
		
		OrderBy orderby = new OrderBy();
		orderby.addVariable(new ElementSymbol("1"), true); //$NON-NLS-1$

		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		query.setOrderBy(orderby);          
                
        helpTest("SELECT concat(x, \"5\") FROM z order by 1", "SELECT concat(x, '5') FROM z ORDER BY 1", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testEmptyAndNullInputsGiveSameErrorMessage() throws Exception {
        String emptyMessage = null;
        try {
            QueryParser.getQueryParser().parseCommand(""); //$NON-NLS-1$
            fail("Expected exception for parsing empty string"); //$NON-NLS-1$
        } catch(MetaMatrixException e) { 
            emptyMessage = e.getMessage();
        }       

        String nullMessage = null;
        try {
            QueryParser.getQueryParser().parseCommand(null);
            fail("Expected exception for parsing null string"); //$NON-NLS-1$
        } catch(MetaMatrixException e) { 
            nullMessage = e.getMessage();
        }     
        
        assertTrue("Expected same message for empty and null cases", emptyMessage.equals(nullMessage)); //$NON-NLS-1$
    }

    public void testCase3281NamedVariable() {
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setDisplayNamedParameters(true);
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        SPParameter parameter = new SPParameter(1, new Constant("paramValue1")); //$NON-NLS-1$ 
        parameter.setName("param1"); //$NON-NLS-1$
        parameter.setParameterType(ParameterInfo.IN);
        storedQuery.setParameter(parameter);
        helpTest("Exec proc1(param1 = 'paramValue1')", "EXEC proc1(param1 = 'paramValue1')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("execute proc1(param1 = 'paramValue1')", "EXEC proc1(param1 = 'paramValue1')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCase3281NamedVariables() {
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setDisplayNamedParameters(true);
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        SPParameter param1 = new SPParameter(1, new Constant("paramValue1")); //$NON-NLS-1$ 
        param1.setName("param1"); //$NON-NLS-1$
        param1.setParameterType(ParameterInfo.IN);
        storedQuery.setParameter(param1);
        SPParameter param2 = new SPParameter(2, new Constant("paramValue2")); //$NON-NLS-1$ 
        param2.setName("param2"); //$NON-NLS-1$
        param2.setParameterType(ParameterInfo.IN);
        storedQuery.setParameter(param2);
        helpTest("Exec proc1(param1 = 'paramValue1', param2 = 'paramValue2')", "EXEC proc1(param1 = 'paramValue1', param2 = 'paramValue2')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("execute proc1(param1 = 'paramValue1', param2 = 'paramValue2')", "EXEC proc1(param1 = 'paramValue1', param2 = 'paramValue2')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCase3281QuotedNamedVariableFails2() {
        try {
            QueryParser.getQueryParser().parseCommand("Exec proc1('param1' = 'paramValue1')"); //$NON-NLS-1$
            fail("Named parameter name cannot be quoted"); //$NON-NLS-1$
        }catch(QueryParserException e) {
            // this is expected.
            //e.printStackTrace();
            //assertEquals("Unable to parse named parameter name: 'param1'", e.getMessage()); //$NON-NLS-1$
        }
    }

    /** Test what happens if the name of a parameter is a reserved word.  It must be quoted (double-ticks). */
    public void testCase3281NamedVariablesReservedWords() {
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setDisplayNamedParameters(true);
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        SPParameter param1 = new SPParameter(1, new Constant("paramValue1")); //$NON-NLS-1$ 
        param1.setName("in"); //$NON-NLS-1$ //<---RESERVED WORD
        param1.setParameterType(ParameterInfo.IN);
        storedQuery.setParameter(param1);
        SPParameter param2 = new SPParameter(2, new Constant("paramValue2")); //$NON-NLS-1$ 
        param2.setName("in2"); //$NON-NLS-1$
        param2.setParameterType(ParameterInfo.IN);
        storedQuery.setParameter(param2);
        helpTest("Exec proc1(\"in\" = 'paramValue1', in2 = 'paramValue2')", "EXEC proc1(\"in\" = 'paramValue1', in2 = 'paramValue2')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("execute proc1(\"in\" = 'paramValue1', in2 = 'paramValue2')", "EXEC proc1(\"in\" = 'paramValue1', in2 = 'paramValue2')", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    public void testExceptionMessageWithLocation() {
        try {
            QueryParser.getQueryParser().parseCommand("SELECT FROM"); //$NON-NLS-1$
        } catch(QueryParserException e) {
            assertTrue(e.getMessage().startsWith("Parsing error: Encountered \"FROM\" at line 1, column 8.")); //$NON-NLS-1$
        }
    }
    
    public void testExceptionMessageWithoutLocation() {
        try {
            QueryParser.getQueryParser().parseCommand("SELECT COUNT(*) FROM a WHERE COUNT(*) > 1"); //$NON-NLS-1$
        } catch(QueryParserException e) {
            assertTrue(e.getMessage().startsWith("Parsing error: Aggregate expressions are allowed only as top level functions in the SELECT and HAVING clauses.")); //$NON-NLS-1$
        }        
    }
    
    public void testLimit() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new Object[] {new AllSymbol()}));
        From from = new From(Arrays.asList(new Object[] {new UnaryFromClause(new GroupSymbol("a"))})); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(0)), new Constant(new Integer(100))));
        helpTest("Select * from a limit 100", "SELECT * FROM a LIMIT 100", query); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("Select * from a limit 0, 100", "SELECT * FROM a LIMIT 0, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLimitWithOffset() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new Object[] {new AllSymbol()}));
        From from = new From(Arrays.asList(new Object[] {new UnaryFromClause(new GroupSymbol("a"))})); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Constant(new Integer(100))));
        helpTest("Select * from a limit 50,100", "SELECT * FROM a LIMIT 50, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLimitWithReferences1() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new Object[] {new AllSymbol()}));
        From from = new From(Arrays.asList(new Object[] {new UnaryFromClause(new GroupSymbol("a"))})); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), new Constant(new Integer(100))));
        helpTest("Select * from a limit ?,100", "SELECT * FROM a LIMIT ?, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLimitWithReferences2() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new Object[] {new AllSymbol()}));
        From from = new From(Arrays.asList(new Object[] {new UnaryFromClause(new GroupSymbol("a"))})); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Reference(0)));
        helpTest("Select * from a limit 50,?", "SELECT * FROM a LIMIT 50, ?", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLimitWithReferences3() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new Object[] {new AllSymbol()}));
        From from = new From(Arrays.asList(new Object[] {new UnaryFromClause(new GroupSymbol("a"))})); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), new Reference(1)));
        helpTest("Select * from a limit ?,?", "SELECT * FROM a LIMIT ?, ?", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testEmptyOuterJoinCriteria() {
        helpException("select a from b left outer join c on ()"); //$NON-NLS-1$
    }
    
    public void testCreateTempTable1() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        List columns = new ArrayList();
        ElementSymbol column = new ElementSymbol("c1");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
        columns.add(column);
        column = new ElementSymbol("c2");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BYTE);
        columns.add(column);
        create.setColumns(columns);
        helpTest("Create local TEMPORARY table tempTable (c1 boolean, c2 byte)", "CREATE LOCAL TEMPORARY TABLE tempTable (c1 boolean, c2 byte)", create); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCreateTempTable2() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        List columns = new ArrayList();
        ElementSymbol column = new ElementSymbol("c1");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
        columns.add(column);
        column = new ElementSymbol("c2");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BYTE);
        columns.add(column);
        create.setColumns(columns);
        helpTest("Create local TEMPORARY table tempTable(c1 boolean, c2 byte)", "CREATE LOCAL TEMPORARY TABLE tempTable (c1 boolean, c2 byte)", create); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCreateTempTable3() {
        helpException("Create TEMPORARY table tempTable (c1 boolean, c2 byte)"); //$NON-NLS-1$ 
    }
    
    public void testCreateTempTable4() {
        helpException("Create table tempTable (c1 boolean, c2 byte)"); //$NON-NLS-1$ 
    }
    
    public void testCreateTempTable5() {
        helpException("Create  local TEMPORARY table tempTable (c1 boolean primary, c2 byte)"); //$NON-NLS-1$ 
    }
    
    public void testCreateTempTable6() {
        helpException("Create  local TEMPORARY table tempTable (c1 varchar, c2 byte)"); //$NON-NLS-1$ 
    }
    
    public void testCreateTempTable7() {
        helpException("Create local TEMPORARY table tempTable (c1.x boolean, c2 byte)" ,"Parsing error: Invalid simple identifier format: [c1.x]"); //$NON-NLS-1$ //$NON-NLS-2$ 
    }
    
    public void testDropTable() {
        Drop drop = new Drop();
        drop.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        helpTest("DROP table tempTable", "DROP TABLE tempTable", drop); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testEscapedOuterJoin() {
        String sql = "SELECT * FROM {oj A LEFT OUTER JOIN B ON (A.x=B.x)}"; //$NON-NLS-1$
        String expected = "SELECT * FROM A LEFT OUTER JOIN B ON A.x = B.x"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        query.setSelect(select);
        select.addSymbol(new AllSymbol());
        From from = new From();
        query.setFrom(from);
        Criteria compareCriteria = new CompareCriteria(new ElementSymbol("A.x"), CompareCriteria.EQ, new ElementSymbol("B.x")); //$NON-NLS-1$ //$NON-NLS-2$
        FromClause f1 = new UnaryFromClause(new GroupSymbol("A")); //$NON-NLS-1$
        FromClause f2 = new UnaryFromClause(new GroupSymbol("B")); //$NON-NLS-1$
        JoinPredicate jp = new JoinPredicate(f1, f2, JoinType.JOIN_LEFT_OUTER, Arrays.asList(new Object[] {compareCriteria}));
        from.addClause(jp);
        
        helpTest(sql, expected, query);
    } 
    
    public void testBadAlias() {
        String sql = "select a as a.x from foo"; //$NON-NLS-1$
        
        helpException(sql, "Parsing error: Invalid alias format: [a.x]"); //$NON-NLS-1$
    }
    
    public void testBadFunctionName() {
        String sql = "select a.x()"; //$NON-NLS-1$
        
        helpException(sql, "Parsing error: Invalid function name: [a.x]"); //$NON-NLS-1$
    }
    
    public void testUnionJoin() {
        String sql = "select * from pm1.g1 union join pm1.g2 where g1.e1 = 1"; //$NON-NLS-1$
        String expected = "SELECT * FROM pm1.g1 UNION JOIN pm1.g2 WHERE g1.e1 = 1"; //$NON-NLS-1$
        
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        
        From from = new From();
        from.addClause(new JoinPredicate(new UnaryFromClause(new GroupSymbol("pm1.g1")), new UnaryFromClause(new GroupSymbol("pm1.g2")), JoinType.JOIN_UNION)); //$NON-NLS-1$ //$NON-NLS-2$
        
        Criteria crit = new CompareCriteria(new ElementSymbol("g1.e1"), CompareCriteria.EQ, new Constant(new Integer(1))); //$NON-NLS-1$
        
        Query command = new Query(select, from, crit, null, null);
        helpTest(sql, expected, command);
    }
    
    public void testUnionJoin1() {
        String sql = "select * from pm1.g1 union all join pm1.g2 where g1.e1 = 1"; //$NON-NLS-1$
        
        helpException(sql);
    }
    
    public void testIfElseWithoutBeginEnd() {
        String sql = "CREATE PROCEDURE BEGIN IF (x > 1) select 1; IF (x > 1) select 1; ELSE select 1; END"; //$NON-NLS-1$
        String expected = "CREATE PROCEDURE\nBEGIN\nIF(x > 1)\nBEGIN\nSELECT 1;\nEND\nIF(x > 1)\nBEGIN\nSELECT 1;\nEND\nELSE\nBEGIN\nSELECT 1;\nEND\nEND"; //$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(new Select(Arrays.asList(new Object[] {new ExpressionSymbol("expr", new Constant(new Integer(1)))}))); //$NON-NLS-1$
        CommandStatement commandStmt = new CommandStatement(query);
        CompareCriteria criteria = new CompareCriteria(new ElementSymbol("x"), CompareCriteria.GT, new Constant(new Integer(1))); //$NON-NLS-1$
        Block block = new Block();
        block.addStatement(commandStmt);
        IfStatement ifStmt = new IfStatement(criteria, block);
        IfStatement ifStmt1 = (IfStatement)ifStmt.clone();
        Block block2 = new Block();
        block2.addStatement(commandStmt);
        ifStmt1.setElseBlock(block2);
        Block block3 = new Block();
        block3.addStatement(ifStmt);
        block3.addStatement(ifStmt1);
        CreateUpdateProcedureCommand command = new CreateUpdateProcedureCommand(block3);
        
        helpTest(sql, expected, command);
    }
    
    public void testBadCreate() {
        helpException("create insert"); //$NON-NLS-1$
    }
    
    public void testCommandWithSemicolon() throws Exception {
        helpTest("select * from pm1.g1;", "SELECT * FROM pm1.g1", QueryParser.getQueryParser().parseCommand("select * from pm1.g1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testLOBTypes() throws Exception {
        Function convert = new Function("convert", new Expression[] {new Constant(null), new Constant("blob")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function convert1 = new Function("convert", new Expression[] {new Constant(null), new Constant("clob")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function convert2 = new Function("convert", new Expression[] {new Constant(null), new Constant("xml")}); //$NON-NLS-1$ //$NON-NLS-2$
        Select select = new Select(Arrays.asList(new Object[] {new ExpressionSymbol("expr", convert), new ExpressionSymbol("expr1", convert1), new ExpressionSymbol("expr2", convert2)})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Query query = new Query();
        query.setSelect(select);
        
        helpTest("select convert(null, blob), convert(null, clob), convert(null, xml)", "SELECT convert(null, blob), convert(null, clob), convert(null, xml)", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInsertWithoutColumns() {
        Insert insert = new Insert();
        insert.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        insert.addValue(new Constant("a")); //$NON-NLS-1$
        insert.addValue(new Constant("b")); //$NON-NLS-1$
        helpTest("INSERT INTO m.g VALUES ('a', 'b')",  //$NON-NLS-1$
                 "INSERT INTO m.g VALUES ('a', 'b')",  //$NON-NLS-1$
                 insert);
    }
}
