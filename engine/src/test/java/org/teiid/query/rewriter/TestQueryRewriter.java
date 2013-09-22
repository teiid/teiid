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

package org.teiid.query.rewriter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.metadata.Table;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;


@SuppressWarnings("nls")
public class TestQueryRewriter {

    private static final String TRUE_STR = "1 = 1"; //$NON-NLS-1$
    private static final String FALSE_STR = "1 = 0"; //$NON-NLS-1$

    // ################################## TEST HELPERS ################################
    
    private Criteria parseCriteria(String critStr, QueryMetadataInterface metadata) {
        try {
            Criteria crit = QueryParser.getQueryParser().parseCriteria(critStr);
            
            // resolve against metadata
            QueryResolver.resolveCriteria(crit, metadata);
            
            return crit;
        } catch(TeiidException e) {
            throw new RuntimeException(e);
        }   
    }
    
    private Criteria helpTestRewriteCriteria(String original, String expected) {
        try {
            return helpTestRewriteCriteria(original, expected, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } 
    }   
    
    private Criteria helpTestRewriteCriteria(String original, String expected, boolean rewrite) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria expectedCrit = parseCriteria(expected, metadata);
        if (rewrite) {
            QueryResolver.resolveCriteria(expectedCrit, metadata);
            expectedCrit = QueryRewriter.rewriteCriteria(expectedCrit, null, metadata);
        }
        return helpTestRewriteCriteria(original, expectedCrit, metadata);
    }
    
    private Map<ElementSymbol, Integer> elements;
    private List<List<? extends Object>> tuples;
    
    @Before public void setUp() {
    	elements = null;
    	tuples = new ArrayList<List<? extends Object>>();
    }

    private Criteria helpTestRewriteCriteria(String original, Criteria expectedCrit, QueryMetadataInterface metadata) {
        Criteria origCrit = parseCriteria(original, metadata);
        
        Criteria actual = null;
        // rewrite
        try { 
        	ArrayList<Boolean> booleanVals = new ArrayList<Boolean>(tuples.size());
        	for (List<?> tuple : tuples) {
            	booleanVals.add(new Evaluator(elements, null, null).evaluate(origCrit, tuple));
			}
            actual = QueryRewriter.rewriteCriteria(origCrit, null, metadata);
            assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
            for (int i = 0; i < tuples.size(); i++) {
            	assertEquals(tuples.get(i).toString(), booleanVals.get(i), new Evaluator(elements, null, null).evaluate(actual, tuples.get(i)));
			}
        } catch(TeiidException e) { 
        	throw new RuntimeException(e);
        }
        return actual;
    }  
    
    private Expression helpTestRewriteExpression(String original, String expected, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException {
    	Expression actualExp = QueryParser.getQueryParser().parseExpression(original);
    	ResolverVisitor.resolveLanguageObject(actualExp, metadata);
    	CommandContext context = new CommandContext();
    	context.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
    	actualExp = QueryRewriter.rewriteExpression(actualExp, context, metadata);
    	if (expected != null) {
	    	Expression expectedExp = QueryParser.getQueryParser().parseExpression(expected);
	    	ResolverVisitor.resolveLanguageObject(expectedExp, metadata);
	    	assertEquals(expectedExp, actualExp);
    	}
    	return actualExp;
    }
    
	private String getRewritenProcedure(String procedure, String userUpdateStr, Table.TriggerEvent procedureType) throws TeiidComponentException, TeiidProcessingException {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleUpdateProc(procedureType, procedure);

        return getRewritenProcedure(userUpdateStr, metadata);
	}

	private String getRewritenProcedure(String userUpdateStr,
			QueryMetadataInterface metadata) throws TeiidComponentException,
			QueryMetadataException, TeiidProcessingException {
		ProcedureContainer userCommand = (ProcedureContainer)QueryParser.getQueryParser().parseCommand(userUpdateStr); 
        QueryResolver.resolveCommand(userCommand, metadata);
        Command proc = QueryResolver.expandCommand(userCommand, metadata, null);
		QueryRewriter.rewrite(userCommand, metadata, null);
		Command result = QueryRewriter.rewrite(proc, metadata, null);
		return result.toString();
	}
	
    static Command helpTestRewriteCommand(String original, String expected) { 
        try {
            return helpTestRewriteCommand(original, expected, RealMetadataFactory.example1Cached());
        } catch(TeiidException e) { 
            throw new TeiidRuntimeException(e);
        }
    }
    
    public static Command helpTestRewriteCommand(String original, String expected, QueryMetadataInterface metadata) throws TeiidException { 
        return helpTestRewriteCommand(original, expected, metadata, null);
    }
    
    public static Command helpTestRewriteCommand(String original, String expected, QueryMetadataInterface metadata, CommandContext cc) throws TeiidException {
    	Command command = QueryParser.getQueryParser().parseCommand(original);            
        QueryResolver.resolveCommand(command, metadata);
        Command rewriteCommand = QueryRewriter.rewrite(command, metadata, cc);
        assertEquals("Rewritten command was not expected", expected, rewriteCommand.toString()); //$NON-NLS-1$
        return rewriteCommand;
    }
    
    @Test public void testRewriteUnknown() {
        helpTestRewriteCriteria("pm1.g1.e1 = '1' and '1' = convert(null, string)", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteUnknown1() {
        helpTestRewriteCriteria("pm1.g1.e1 = '1' or '1' = convert(null, string)", "pm1.g1.e1 = '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteUnknown2() {
        helpTestRewriteCriteria("not('1' = convert(null, string))", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteUnknown3() {
        helpTestRewriteCriteria("pm1.g1.e1 like convert(null, string))", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteUnknown4() {
        helpTestRewriteCriteria("null in ('a', 'b', 'c')", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteUnknown5() {
        helpTestRewriteCriteria("(null <> null) and 1 = 0", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteUnknown6() {
        helpTestRewriteCriteria("not(pm1.g1.e1 = '1' and '1' = convert(null, string))", "pm1.g1.e1 <> '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    @Test public void testRewriteUnknown7() {
        helpTestRewriteCriteria("not(pm1.g1.e1 = '1' or '1' = convert(null, string))", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteUnknown8() {
        helpTestRewriteCriteria("pm1.g1.e1 in (2, null)", "pm1.g1.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$ 
    }
    
    @Test public void testRewriteInCriteriaWithRepeats() {
        helpTestRewriteCriteria("pm1.g1.e1 in ('1', '1', '2')", "pm1.g1.e1 IN ('1', '2')"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteInCriteriaWithSingleValue() {
        helpTestRewriteCriteria("pm1.g1.e1 in ('1')", "pm1.g1.e1 = '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteInCriteriaWithSingleValue1() {
        helpTestRewriteCriteria("pm1.g1.e1 not in ('1')", "pm1.g1.e1 != '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteInCriteriaWithConvert() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) not in ('x')", "pm1.g1.e2 IS NOT NULL"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteInCriteriaWithNoValues() throws Exception {
    	ElementSymbol e1 = new ElementSymbol("e1");
    	e1.setGroupSymbol(new GroupSymbol("g1"));
    	SetCriteria crit = new SetCriteria(e1, Collections.EMPTY_LIST); //$NON-NLS-1$
        
        Criteria actual = QueryRewriter.rewriteCriteria(crit, null, null);
        
        assertEquals(QueryRewriter.FALSE_CRITERIA, actual);
        
        crit.setNegated(true);
        
        actual = QueryRewriter.rewriteCriteria(crit, null, null);
        
        assertEquals(QueryRewriter.TRUE_CRITERIA, actual);
    }
        
    @Test public void testRewriteBetweenCriteria1() {
        helpTestRewriteCriteria("pm1.g1.e1 BETWEEN 1000 AND 2000", "(pm1.g1.e1 >= '1000') AND (pm1.g1.e1 <= '2000')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteBetweenCriteria2() {
        helpTestRewriteCriteria("pm1.g1.e1 NOT BETWEEN 1000 AND 2000", "(pm1.g1.e1 < '1000') OR (pm1.g1.e1 > '2000')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCrit1() {
        helpTestRewriteCriteria("concat('a','b') = 'ab'", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCrit2() {
        helpTestRewriteCriteria("'x' = pm1.g1.e1", "(pm1.g1.e1 = 'x')"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCrit3() {
        helpTestRewriteCriteria("pm1.g1.e1 = convert('a', string)", "pm1.g1.e1 = 'a'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCrit4() {
        helpTestRewriteCriteria("pm1.g1.e1 = CONVERT('a', string)", "pm1.g1.e1 = 'a'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCrit5() {
        helpTestRewriteCriteria("pm1.g1.e1 in ('a')", "pm1.g1.e1 = 'a'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit6() {
        helpTestRewriteCriteria("1 = convert(pm1.g1.e1,integer) + 10", "pm1.g1.e1 = '-9'"); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    @Test public void testRewriteCrit7() {
        helpTestRewriteCriteria("((pm1.g1.e1 = 1) and (pm1.g1.e1 = 1))", "pm1.g1.e1 = '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteMatchCritEscapeChar1() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE 'x_' ESCAPE '\\'", "pm1.g1.e1 LIKE 'x_'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteMatchCritEscapeChar2() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE '#%x' ESCAPE '#'", "pm1.g1.e1 LIKE '#%x' ESCAPE '#'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteMatchCritEscapeChar3() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE '#%x'", "pm1.g1.e1 LIKE '#%x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteMatchCritEscapeChar4() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE pm1.g1.e1 ESCAPE '#'", "pm1.g1.e1 LIKE pm1.g1.e1 ESCAPE '#'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteMatchCritEscapeChar5() throws Exception {
        MatchCriteria mcrit = new MatchCriteria(new ElementSymbol("pm1.g1.e1"), new Constant(null, DataTypeManager.DefaultDataClasses.STRING), '#'); //$NON-NLS-1$
        Criteria expected = QueryRewriter.UNKNOWN_CRITERIA; 
                
        Object actual = QueryRewriter.rewriteCriteria(mcrit, null, null); 
        assertEquals("Did not get expected rewritten criteria", expected, actual); //$NON-NLS-1$
    }
    
    @Test public void testRewriteMatchCrit1() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE 'x' ESCAPE '\\'", "pm1.g1.e1 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteMatchCrit2() {
        helpTestRewriteCriteria("pm1.g1.e1 NOT LIKE 'x'", "pm1.g1.e1 <> 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteMatchCrit3() {
        helpTestRewriteCriteria("pm1.g1.e1 NOT LIKE '%'", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritTimestampCreate1() {
        helpTestRewriteCriteria("timestampCreate(pm3.g1.e2, pm3.g1.e3) = {ts'2004-11-23 09:25:00'}", "(pm3.g1.e2 = {d'2004-11-23'}) AND (pm3.g1.e3 = {t'09:25:00'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritTimestampCreate2() {
        helpTestRewriteCriteria("{ts'2004-11-23 09:25:00'} = timestampCreate(pm3.g1.e2, pm3.g1.e3)", "(pm3.g1.e2 = {d'2004-11-23'}) AND (pm3.g1.e3 = {t'09:25:00'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritSwap1() {
        helpTestRewriteCriteria("'x' = pm1.g1.e1", "pm1.g1.e1 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritSwap2() {
        helpTestRewriteCriteria("'x' <> pm1.g1.e1", "pm1.g1.e1 <> 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritSwap3() {
        helpTestRewriteCriteria("'x' < pm1.g1.e1", "pm1.g1.e1 > 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritSwap4() {
        helpTestRewriteCriteria("'x' <= pm1.g1.e1", "pm1.g1.e1 >= 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritSwap5() {
        helpTestRewriteCriteria("'x' > pm1.g1.e1", "pm1.g1.e1 < 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritSwap6() {
        helpTestRewriteCriteria("'x' >= pm1.g1.e1", "pm1.g1.e1 <= 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_op1() {
        helpTestRewriteCriteria("pm1.g1.e2 + 5 = 10", "pm1.g1.e2 = 5"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_op2() {
        helpTestRewriteCriteria("pm1.g1.e2 - 5 = 10", "pm1.g1.e2 = 15"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_op3() {
        helpTestRewriteCriteria("pm1.g1.e2 * 5 = 10", "pm1.g1.e2 = 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_op4() {
        helpTestRewriteCriteria("pm1.g1.e2 / 5 = 10", "pm1.g1.e2 = 50"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_signFlip1() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 > 10", "pm1.g1.e2 < -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_signFlip2() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 >= 10", "pm1.g1.e2 <= -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritExpr_signFlip3() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 < 10", "pm1.g1.e2 > -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_signFlip4() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 <= 10", "pm1.g1.e2 >= -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritExpr_backwards1() {
        helpTestRewriteCriteria("5 + pm1.g1.e2 <= 10", "pm1.g1.e2 <= 5"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_backwards2() {
        helpTestRewriteCriteria("-5 * pm1.g1.e2 <= 10", "pm1.g1.e2 >= -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritExpr_unhandled1() {
        helpTestRewriteCriteria("5 / pm1.g1.e2 <= 10", "5 / pm1.g1.e2 <= 10"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCritExpr_unhandled2() {
        helpTestRewriteCriteria("5 - pm1.g1.e2 <= 10", "5 - pm1.g1.e2 <= 10"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseDate() {
        helpTestRewriteCriteria("PARSEDATE(pm3.g1.e1, 'yyyyMMdd') = {d'2003-05-01'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '20030501'" );         //$NON-NLS-1$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseDate1() {
        helpTestRewriteCriteria("PARSEDATE(pm3.g1.e1, 'yyyyMM') = {d'2003-05-01'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '200305'" );         //$NON-NLS-1$
    }

    @Ignore(value="we're no longer considering parsedate directly")
    @Test public void testRewriteCrit_parseDate2() {
        helpTestRewriteCriteria("PARSEDATE(pm3.g1.e1, 'yyyyMM') = {d'2003-05-02'}", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }
    
    @Ignore(value="Should be moved to the validator")
    @Test public void testRewriteCrit_invalidParseDate() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        Criteria origCrit = parseCriteria("PARSEDATE(pm3.g1.e1, '''') = {d'2003-05-01'}", metadata); //$NON-NLS-1$
        
        try { 
            QueryRewriter.rewriteCriteria(origCrit, null, null);
            fail("Expected failure"); //$NON-NLS-1$
        } catch(TeiidException e) { 
            assertEquals("Error Code:ERR.015.001.0003 Message:Error simplifying criteria: PARSEDATE(pm3.g1.e1, '''') = {d'2003-05-01'}", e.getMessage());     //$NON-NLS-1$
        }
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseTime() {
        helpTestRewriteCriteria("PARSETIME(pm3.g1.e1, 'HH mm ss') = {t'13:25:04'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '13 25 04'" );         //$NON-NLS-1$
    }

    @Test public void testRewriteCrit_parseTimestamp() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') = {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseTimestamp1() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') = {ts'2003-01-01 00:25:00.0'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '2003 01 25'" );         //$NON-NLS-1$
    }
    
    @Test public void testRewriteCrit_parseTimestamp2() {
        helpTestRewriteCriteria("PARSETimestamp(CONVERT(pm3.g1.e2, string), 'yyyy-MM-dd') = {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }

    @Test public void testRewriteCrit_parseTimestamp3() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') <> {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "pm3.g1.e1 is not null" );         //$NON-NLS-1$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseTimestamp4() {
        helpTestRewriteCriteria("PARSETimestamp(CONVERT(pm3.g1.e2, string), 'yyyy-MM-dd') = {ts'2003-05-01 00:00:00.0'}", //$NON-NLS-1$
                                "pm3.g1.e2 = {d'2003-05-01'}" );         //$NON-NLS-1$
    }
    
    @Test public void testRewriteCrit_parseTimestamp_notEquality() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') > {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') > {ts'2003-05-01 13:25:04.5'}" );         //$NON-NLS-1$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseTimestamp_decompose() {
        helpTestRewriteCriteria("PARSETIMESTAMP(CONCAT(FORMATDATE(pm3.g1.e2, 'yyyyMMdd'), FORMATTIME(pm3.g1.e3, 'HHmmss')), 'yyyyMMddHHmmss') = PARSETIMESTAMP('19690920183045', 'yyyyMMddHHmmss')", //$NON-NLS-1$
        "(pm3.g1.e2 = {d'1969-09-20'}) AND (pm3.g1.e3 = {t'18:30:45'})" );         //$NON-NLS-1$
    }
    
    @Test public void testRewriteCrit_timestampCreate_decompose() {
        helpTestRewriteCriteria("timestampCreate(pm3.g1.e2, pm3.g1.e3) = PARSETIMESTAMP('19690920183045', 'yyyyMMddHHmmss')", //$NON-NLS-1$
        "(pm3.g1.e2 = {d'1969-09-20'}) AND (pm3.g1.e3 = {t'18:30:45'})" );         //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseInteger() {
        helpTestRewriteCriteria("parseInteger(pm1.g1.e1, '#,##0') = 1234", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234'" );         //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseLong() {
        helpTestRewriteCriteria("parseLong(pm1.g1.e1, '#,##0') = convert(1234, long)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234'" );         //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseBigInteger() {
        helpTestRewriteCriteria("parseBigInteger(pm1.g1.e1, '#,##0') = convert(1234, biginteger)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234'" );         //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseFloat() {
        helpTestRewriteCriteria("parseFloat(pm1.g1.e1, '#,##0.###') = convert(1234.123, float)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234.123'" );         //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseDouble() {
        helpTestRewriteCriteria("parseDouble(pm1.g1.e1, '$#,##0.00') = convert(1234.5, double)", //$NON-NLS-1$
                                "pm1.g1.e1 = '$1,234.50'" );         //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCrit_parseBigDecimal() {
        helpTestRewriteCriteria("parseBigDecimal(pm1.g1.e1, '#,##0.###') = convert(1234.1234, bigdecimal)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234.123'" );         //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatDate() {
        helpTestRewriteCriteria("formatDate(pm3.g1.e2, 'yyyyMMdd') = '20030501'", //$NON-NLS-1$
                                "pm3.g1.e2 = {d'2003-05-01'}" );         //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatTime() {
        helpTestRewriteCriteria("formatTime(pm3.g1.e3, 'HH mm ss') = '13 25 04'", //$NON-NLS-1$
                                "pm3.g1.e3 = {t'13:25:04'}" );         //$NON-NLS-1$
    }

    @Test public void testRewriteCrit_formatTimestamp() {
        helpTestRewriteCriteria("formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss') = '05 01, 1974 - 07:00:00'", //$NON-NLS-1$
                                "formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss') = '05 01, 1974 - 07:00:00'" );         //$NON-NLS-1$
    }
    
    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatTimestamp1() {
        helpTestRewriteCriteria("formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss.S') = '05 01, 1974 - 07:00:00.0'", //$NON-NLS-1$
                                "pm3.g1.e4 = {ts'1974-05-01 07:00:00.0'}" );         //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatInteger() {
        helpTestRewriteCriteria("formatInteger(pm1.g1.e2, '#,##0') = '1,234'", //$NON-NLS-1$
                                "pm1.g1.e2 = 1234" );         //$NON-NLS-1$
    }
    
    @Test public void testRewriteCrit_formatInteger1() throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        helpTestRewriteCriteria("formatInteger(pm1.g1.e2, '#5') = '105'", //$NON-NLS-1$
                                "formatbigdecimal(convert(pm1.g1.e2, bigdecimal), '#5') = '105'", true );         //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatLong() {
        helpTestRewriteCriteria("formatLong(convert(pm1.g1.e2, long), '#,##0') = '1,234,567,890,123'", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }
    
    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatLong1() {
        helpTestRewriteCriteria("formatLong(convert(pm1.g1.e2, long), '#,##0') = '1,234,567,890'", //$NON-NLS-1$
                                "pm1.g1.e2 = 1234567890" );         //$NON-NLS-1$
    }
    
    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatTimestampInvert() { 
        String original = "formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss.S') = ?"; //$NON-NLS-1$ 
        String expected = "pm3.g1.e4 = parseTimestamp(?, 'MM dd, yyyy - HH:mm:ss.S')"; //$NON-NLS-1$ 
         
        helpTestRewriteCriteria(original, expected); 
    } 
     
    @Test public void testRewriteCrit_plusInvert() { 
        String original = "pm1.g1.e2 + 1.1 = ?"; //$NON-NLS-1$ 
        String expected = "convert(pm1.g1.e2, bigdecimal) = ? - 1.1"; //$NON-NLS-1$ 
         
        helpTestRewriteCriteria(original, expected);
    } 

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatBigInteger() throws Exception {
        String original = "formatBigInteger(convert(pm1.g1.e2, biginteger), '#,##0') = '1,234,567,890'"; //$NON-NLS-1$
        String expected = "pm1.g1.e2 = 1234567890"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        Criteria expectedCrit = parseCriteria(expected, metadata);
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null);
        assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatFloat() throws Exception {
        String original = "formatFloat(convert(pm1.g1.e4, float), '#,##0.###') = '1,234.123'"; //$NON-NLS-1$
        String expected = "pm1.g1.e4 = 1234.123046875"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null);
        assertEquals("Did not rewrite correctly: ", expected, actual.toString()); //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatDouble() throws Exception {
        String original = "formatDouble(convert(pm1.g1.e4, double), '$#,##0.00') = '$1,234.50'"; //$NON-NLS-1$
        String expected = "pm1.g1.e4 = '1234.5'"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        Criteria expectedCrit = parseCriteria(expected, metadata);
        ((CompareCriteria)expectedCrit).setRightExpression(new Constant(new Double(1234.5)));
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null);
        assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
    }

    @Ignore(value="Cannot deterime if the format is narrowing")
    @Test public void testRewriteCrit_formatBigDecimal() throws Exception {
        String original = "formatBigDecimal(convert(pm1.g1.e4, bigdecimal), '#,##0.###') = '1,234.5'"; //$NON-NLS-1$
        String expected = "pm1.g1.e4 = 1234.5"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        Criteria expectedCrit = parseCriteria(expected, metadata);
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null);
        assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
    }
    
    @Test public void testRewriteCritTimestampDiffDate1() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_DAY, {d'2003-05-15'}, {d'2003-05-17'} ) = 2", TRUE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteCritTimestampDiffDate2() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_DAY, {d'2003-06-02'}, {d'2003-05-17'} ) = -16", TRUE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteCritTimestampDiffDate3() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_QUARTER, {d'2002-01-25'}, {d'2003-06-01'} ) = 5", TRUE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteCritTimestampDiffTime1() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_HOUR, {t'03:04:45'}, {t'05:05:36'} ) = 2", TRUE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteCritTimestampDiffTime1_ignorecase() {
        helpTestRewriteCriteria("timestampdiff(SQL_tsi_HOUR, {t'03:04:45'}, {t'05:05:36'} ) = 2", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteOr1() {
        helpTestRewriteCriteria("(5 = 5) OR (0 = 1)", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteOr2() {
        helpTestRewriteCriteria("(0 = 1) OR (5 = 5)", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteOr3() {
        helpTestRewriteCriteria("(1 = 1) OR (5 = 5)", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteOr4() {
        helpTestRewriteCriteria("(0 = 1) OR (4 = 5)", FALSE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteOr5() {
        helpTestRewriteCriteria("(0 = 1) OR (4 = 5) OR (pm1.g1.e1 = 'x')", "(pm1.g1.e1 = 'x')");         //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteOr6() {
        helpTestRewriteCriteria("(0 = 1) OR (4 = 5) OR (pm1.g1.e1 = 'x') OR (pm1.g1.e1 = 'y')", "pm1.g1.e1 IN ('x', 'y')");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteOr7() {
        helpTestRewriteCriteria("(pm1.g1.e1 = 'x') OR (pm1.g1.e1 = 'y')", "pm1.g1.e1 IN ('x', 'y')");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteAnd1() {
        helpTestRewriteCriteria("(5 = 5) AND (0 = 1)", FALSE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteAnd2() {
        helpTestRewriteCriteria("(0 = 1) AND (5 = 5)", FALSE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteAnd3() {
        helpTestRewriteCriteria("(1 = 1) AND (5 = 5)", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteAnd4() {
        helpTestRewriteCriteria("(0 = 1) AND (4 = 5)", FALSE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteAnd5() { 
        helpTestRewriteCriteria("(1 = 1) AND (5 = 5) AND (pm1.g1.e1 = 'x')", "(pm1.g1.e1 = 'x')");             //$NON-NLS-1$ //$NON-NLS-2$
    }
 
    @Test public void testRewriteAnd6() { 
        helpTestRewriteCriteria("(1 = 1) AND (5 = 5) AND (pm1.g1.e1 = 'x')", "(pm1.g1.e1 = 'x')");             //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteAnd7() {
        helpTestRewriteCriteria("(pm1.g1.e1 = 'x') AND (lower(pm1.g1.e1) = 'y')", "(pm1.g1.e1 = 'x') AND (lcase(pm1.g1.e1) = 'y')");     //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    @Test public void testRewriteMixed1() {
        helpTestRewriteCriteria("((1=1) AND (1=1)) OR ((1=1) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed2() {
        helpTestRewriteCriteria("((1=2) AND (1=1)) OR ((1=1) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed3() {
        helpTestRewriteCriteria("((1=1) AND (1=2)) OR ((1=1) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed4() {
        helpTestRewriteCriteria("((1=1) AND (1=1)) OR ((1=2) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed5() {
        helpTestRewriteCriteria("((1=1) AND (1=1)) OR ((1=1) AND (1=2))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed6() {
        helpTestRewriteCriteria("((1=2) AND (1=1)) OR ((1=2) AND (1=1))", FALSE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed7() {
        helpTestRewriteCriteria("((1=1) AND (1=2)) OR ((1=1) AND (1=2))", FALSE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed8() {
        helpTestRewriteCriteria("((1=2) AND (1=2)) OR ((1=2) AND (1=2))", FALSE_STR); //$NON-NLS-1$
    }
    
    @Test public void testRewriteMixed9() {
        helpTestRewriteCriteria("((1=1) OR (1=1)) AND ((1=1) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed10() {
        helpTestRewriteCriteria("((1=2) OR (1=1)) AND ((1=1) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed11() {
        helpTestRewriteCriteria("((1=1) OR (1=2)) AND ((1=1) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed12() {
        helpTestRewriteCriteria("((1=1) OR (1=1)) AND ((1=2) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed13() {
        helpTestRewriteCriteria("((1=1) OR (1=1)) AND ((1=1) OR (1=2))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed14() {
        helpTestRewriteCriteria("((1=2) OR (1=1)) AND ((1=2) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed15() {
        helpTestRewriteCriteria("((1=1) OR (1=2)) AND ((1=1) OR (1=2))", TRUE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteMixed16() {
        helpTestRewriteCriteria("((1=2) OR (1=2)) AND ((1=2) OR (1=2))", FALSE_STR); //$NON-NLS-1$
    }

    @Test public void testRewriteNot1() {
        helpTestRewriteCriteria("NOT (1=1)", FALSE_STR);     //$NON-NLS-1$
    }   

    @Test public void testRewriteNot2() {
        helpTestRewriteCriteria("NOT (1=2)", TRUE_STR);     //$NON-NLS-1$
    }   
    
    @Test public void testRewriteNot3() {
        helpTestRewriteCriteria("NOT (pm1.g1.e1='x')", "pm1.g1.e1 <> 'x'");     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteDefect1() {
        helpTestRewriteCriteria("(('DE' = 'LN') AND (null > '2002-01-01')) OR (('DE' = 'DE') AND (pm1.g1.e1 > '9000000'))", "(pm1.g1.e1 > '9000000')");         //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteQueryCriteriaAlwaysTrue() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE 0 = 0", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1"); //$NON-NLS-1$
    }
    
    @Test public void testSubquery1() {
        helpTestRewriteCommand("SELECT e1 FROM (SELECT e1 FROM pm1.g1 WHERE (1 - 1) = (0 + 0)) AS x", //$NON-NLS-1$
                                "SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS x"); //$NON-NLS-1$
    }

    @Test public void testExistsSubquery() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE EXISTS (SELECT e1 FROM pm1.g2 LIMIT 1)"); //$NON-NLS-1$
    }

    @Test public void testCompareSubqueryANY() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE '3' = ANY (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE '3' IN (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }

    @Test public void testCompareSubquery() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE '3' = SOME (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE '3' IN (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }
    
    @Test public void testCompareSubqueryUnknown() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE null = SOME (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE null IN (SELECT e1 FROM pm1.g2 LIMIT 1)"); //$NON-NLS-1$
    }

    @Test public void testINClauseSubquery() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE '3' IN (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE '3' IN (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }

    @Test public void testRewriteXMLCriteria1() {
        helpTestRewriteCriteria("context(pm1.g1.e1, pm1.g1.e1) = convert(5, string)", "context(pm1.g1.e1, pm1.g1.e1) = '5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteXMLCriteria2() {
        helpTestRewriteCriteria("context(pm1.g1.e1, convert(5, string)) = 2+3", "context(pm1.g1.e1, '5') = '5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    //base test.  no change is expected
    @Test public void testRewriteLookupFunction1() {
        String criteria = "lookup('pm1.g1','e1', 'e2', 1) = 'ab'"; //$NON-NLS-1$
        CompareCriteria expected = (CompareCriteria)parseCriteria(criteria, RealMetadataFactory.example1Cached()); 
        helpTestRewriteCriteria(criteria, expected, RealMetadataFactory.example1Cached());
    }
    
    @Test public void testRewriteLookupFunction1b() {
        helpTestRewriteCriteria("lookup('pm1.g1','e1', 'e2', pm1.g1.e2) = 'ab'", "lookup('pm1.g1','e1', 'e2', pm1.g1.e2) = 'ab'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** defect 11630 1 should still get rewritten as '1'*/
    @Test public void testRewriteLookupFunctionCompoundCriteria() {
        String criteria = "LOOKUP('pm1.g1','e1', 'e2', 1) IS NULL AND pm1.g1.e1='1'"; //$NON-NLS-1$
        CompoundCriteria expected = (CompoundCriteria)parseCriteria(criteria, RealMetadataFactory.example1Cached()); 
        helpTestRewriteCriteria("LOOKUP('pm1.g1','e1', 'e2', 1) IS NULL AND pm1.g1.e1=1", expected, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ 
    }

    @Test public void testSelectWithNoFrom() {
        helpTestRewriteCommand("SELECT 5", "SELECT 5"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    //defect 9822
    @Test public void testStoredProcedure_9822() throws Exception {

        QueryParser parser = new QueryParser();
        Command command = parser.parseCommand("exec pm1.sp4(5)");             //$NON-NLS-1$
        
        // resolve
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        QueryResolver.resolveCommand(command, metadata);
        
        // rewrite
        Command rewriteCommand = QueryRewriter.rewrite(command, metadata, null);
        
        Collection<SPParameter> parameters = ((StoredProcedure)rewriteCommand).getParameters();

        for (SPParameter param : parameters) {
            if(param.getParameterType() == ParameterInfo.IN || param.getParameterType() == ParameterInfo.INOUT){
                assertTrue(param.getExpression() instanceof Constant);
            }
        }  
    }
    
    @Test public void testRewriteFunctionThrowsEvaluationError() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria("5 / 0 = 5", metadata); //$NON-NLS-1$
        
        // rewrite
        try { 
            QueryRewriter.rewriteCriteria(origCrit, null, metadata);
            fail("Expected QueryValidatorException due to divide by 0"); //$NON-NLS-1$
        } catch(TeiidException e) {
        	// looks like message is being wrapped with another exception with same message
            assertEquals("TEIID30328 Unable to evaluate (5 / 0): TEIID30384 Error while evaluating function /", e.getMessage());  //$NON-NLS-1$
        }       
    }
    
    @Test public void testRewriteConvertThrowsEvaluationError() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria("convert('x', integer) = 0", metadata); //$NON-NLS-1$
        
        // rewrite
        try { 
            QueryRewriter.rewriteCriteria(origCrit, null, metadata);
            fail("Expected QueryValidatorException due to invalid string"); //$NON-NLS-1$
        } catch(TeiidException e) {
            assertEquals("TEIID30328 Unable to evaluate convert('x', integer): TEIID30384 Error while evaluating function convert", e.getMessage()); //$NON-NLS-1$
        }       
    }
    
    @Test public void testRewriteCase1954() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) = '3'", "pm1.g1.e2 = 3"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCase1954a() {
        helpTestRewriteCriteria("cast(pm1.g1.e2 as string) = '3'", "pm1.g1.e2 = 3"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCase1954b() throws Exception{
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached(); 

        CompareCriteria expected = new CompareCriteria();
        ElementSymbol leftElement = new ElementSymbol("pm1.g1.e4"); //$NON-NLS-1$
        Constant constant = new Constant(new Double(3.0), DataTypeManager.DefaultDataClasses.DOUBLE);
        expected.setLeftExpression(leftElement);
        expected.setRightExpression(constant);
        // resolve against metadata
        QueryResolver.resolveCriteria(expected, metadata);
        
        helpTestRewriteCriteria("convert(pm1.g1.e4, string) = '3.0'", expected, metadata); //$NON-NLS-1$ 
    }    

    @Test public void testRewriteCase1954c() {
        helpTestRewriteCriteria("convert(pm1.g1.e1, string) = 'x'", "pm1.g1.e1 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteCase1954d() {
        helpTestRewriteCriteria("convert(pm1.g1.e1, timestamp) = {ts '2005-01-03 00:00:00.0'}", "pm1.g1.e1 = '2005-01-03 00:00:00.0'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCase1954e() {
        helpTestRewriteCriteria("convert(pm1.g1.e4, integer) = 2", "convert(pm1.g1.e4, integer) = 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** Check that this fails, x is not convertable to an int */
    @Test public void testRewriteCase1954f() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) = 'x'", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /** Check that this returns true, x is not convertable to an int */
    @Test public void testRewriteCase1954f1() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) != 'x'", "pm1.g1.e2 is not null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCase1954Set() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) in ('2', '3')", "pm1.g1.e2 IN (2,3)"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    @Test public void testRewriteCase1954SetA() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) in ('2', 'x')", "pm1.g1.e2 = 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    @Test public void testRewriteCase1954SetB() {
        helpTestRewriteCriteria("cast(pm1.g1.e2 as string) in ('2', '3')", "pm1.g1.e2 IN (2,3)"); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    @Test public void testRewriteCase1954SetC() {
        helpTestRewriteCriteria("concat(pm1.g1.e2, 'string') in ('2', '3')", "concat(convert(pm1.g1.e2, string), 'string') in ('2', '3')"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    @Test public void testRewriteCase1954SetD() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) in ('2', pm1.g1.e1)", "convert(pm1.g1.e2, string) in ('2', pm1.g1.e1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }      
    
    // First WHEN always true, so rewrite as THEN expression
    @Test public void testRewriteCaseExpr1() {
        helpTestRewriteCriteria("case when 0=0 then 1 else 2 end = 1", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCaseExpr1a() {
        helpTestRewriteCriteria("case when pm1.g1.e1 = 'a' then 3 when 0=0 then 1 when 1=1 then 4 else 2 end = 1", "CASE WHEN pm1.g1.e1 = 'a' THEN 3 WHEN 1 = 1 THEN 1 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN always false, so rewrite as ELSE expression
    @Test public void testRewriteCaseExpr2() {
        helpTestRewriteCriteria("case when 0=1 then 1 else 2 end = 1", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN can't be rewritten, so no changes
    @Test public void testRewriteCaseExpr3() {
        helpTestRewriteCriteria("case when 0 = pm1.g1.e2 then 1 else 2 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 1 ELSE 2 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCaseExpr4() {
        helpTestRewriteCriteria("lookup('pm1.g1', 'e2', 'e1', case when 1=1 then pm1.g1.e1 end) = 0", "lookup('pm1.g1', 'e2', 'e1', pm1.g1.e1) = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN always false, so remove it
    @Test public void testRewriteCaseExpr5() {
        helpTestRewriteCriteria("case when 0=1 then 1 when 0 = pm1.g1.e2 then 2 else 3 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 2 ELSE 3 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCaseExprForCase5413aFrom502() {
        helpTestRewriteCriteria("pm1.g2.e1 = case when 0 = pm1.g1.e2 then 2 else 2 end", "pm1.g2.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCaseExprForCase5413bFrom502() {
        helpTestRewriteCriteria("case when 0 = pm1.g1.e2 then null else null end IS NULL", TRUE_STR); //$NON-NLS-1$ 
    }
    
    @Test public void testRewriteConstantAgg2() throws Exception {
    	helpTestRewriteCommand("select count(2) from pm1.g1 group by e1", "SELECT COUNT(2) FROM pm1.g1 GROUP BY e1");
    }
    
    @Test public void testRewriteCaseExprForCase5413a() {
        helpTestRewriteCriteria("pm1.g2.e1 = case when 0 = pm1.g1.e2 then 2 else 2 end", "pm1.g2.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteCaseExprForCase5413b() {
        helpTestRewriteCriteria("case when 0 = pm1.g1.e2 then null else null end IS NULL", TRUE_STR); //$NON-NLS-1$ 
    }

    // First WHEN always true, so rewrite as THEN expression
    @Test public void testRewriteSearchedCaseExpr1() {
        helpTestRewriteCriteria("case 0 when 0 then 1 else 2 end = 1", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN always false, so rewrite as ELSE expression
    @Test public void testRewriteSearchedCaseExpr2() {
        helpTestRewriteCriteria("case 0 when 1 then 1 else 2 end = 1", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteSearchedCaseExpr3() {
        helpTestRewriteCriteria("case 0 when pm1.g1.e2 then 1 else 2 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 1 ELSE 2 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteSearchedCaseExpr4() {
        String criteria = "lookup('pm1.g1', 'e2', 'e1', '2') = 0"; //$NON-NLS-1$
        CompareCriteria expected = (CompareCriteria)parseCriteria(criteria, RealMetadataFactory.example1Cached()); 
        helpTestRewriteCriteria("lookup('pm1.g1', 'e2', 'e1', case 0 when 1 then pm1.g1.e1 else 2 end) = 0", expected, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    // First WHEN always false, so remove it
    @Test public void testRewriteSearchedCaseExpr5() {
        helpTestRewriteCriteria("case 0 when 1 then 1 when pm1.g1.e2 then 2 else 3 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 2 ELSE 3 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDefect16879_1(){
    	helpTestRewriteCommand("SELECT decodestring(e1, 'a, b') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testDefect16879_2(){
    	helpTestRewriteCommand("SELECT decodestring(e1, 'a, b, c, d') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' WHEN e1 = 'c' THEN 'd' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testDefect16879_3(){
    	helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b') FROM pm1.g1", "SELECT convert(CASE WHEN e1 = 'a' THEN 'b' ELSE e1 END, integer) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testDefect16879_4(){
    	helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b, c, d') FROM pm1.g1", "SELECT convert(CASE WHEN e1 = 'a' THEN 'b' WHEN e1 = 'c' THEN 'd' ELSE e1 END, integer) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testDefect16879_5(){
        helpTestRewriteCommand("SELECT decodeinteger(e1, 'null, b, c, d') FROM pm1.g1", "SELECT convert(CASE WHEN e1 IS NULL THEN 'b' WHEN e1 = 'c' THEN 'd' ELSE e1 END, integer) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDefect16879_6(){
        helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b, null, d') FROM pm1.g1", "SELECT convert(CASE WHEN e1 = 'a' THEN 'b' WHEN e1 IS NULL THEN 'd' ELSE e1 END, integer) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testDefect16879_7(){
        helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b, null, d, e') FROM pm1.g1", "SELECT convert(CASE WHEN e1 = 'a' THEN 'b' WHEN e1 IS NULL THEN 'd' ELSE 'e' END, integer) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testCaseExpressionThatResolvesToNull() {
        String sqlBefore = "SELECT CASE 'x' WHEN 'Old Inventory System' THEN NULL WHEN 'New Inventory System' THEN NULL END"; //$NON-NLS-1$
        String sqlAfter = "SELECT null"; //$NON-NLS-1$

        Command cmd = helpTestRewriteCommand( sqlBefore, sqlAfter );
        
        Expression es = cmd.getProjectedSymbols().get(0);
        assertEquals( DataTypeManager.DefaultDataClasses.STRING, es.getType() );
    }

    @Test public void testRewriteExec() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("exec pm1.sq2(session_id())");             //$NON-NLS-1$
        
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        
        CommandContext context = new CommandContext();
        context.setConnectionID("1");
        Command rewriteCommand = QueryRewriter.rewrite(command, RealMetadataFactory.example1Cached(), context);
        
        assertEquals("EXEC pm1.sq2('1')", rewriteCommand.toString()); //$NON-NLS-1$
    }

    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteNestedFunctions() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 where convert(parsedate(e1, 'yyyy-MM-dd'), string) = '2006-07-01'", "SELECT e1 FROM pm1.g1 WHERE e1 = '2006-07-01'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Ignore(value="It's not generally possible to invert a narrowing conversion")
    @Test public void testRewriteWithReference() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 where parsetimestamp(e1, 'yyyy-MM-dd') != ?", "SELECT e1 FROM pm1.g1 WHERE e1 <> formattimestamp(?, 'yyyy-MM-dd')"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewiteJoinCriteria() {
        helpTestRewriteCommand("SELECT pm1.g1.e1 FROM pm1.g1 inner join pm1.g2 on (pm1.g1.e1 = null)", "SELECT pm1.g1.e1 FROM pm1.g1 INNER JOIN pm1.g2 ON 1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewiteCompoundCriteria() {
        helpTestRewriteCriteria("(pm1.g1.e1 = 1 and pm1.g1.e2 = 2) and (pm1.g1.e3 = 1 and pm1.g1.e4 = 2.0e0)", "(pm1.g1.e1 = '1') AND (pm1.g1.e2 = 2) AND (pm1.g1.e3 = TRUE) AND (pm1.g1.e4 = 2.0e0)"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteWhile1() throws Exception {
        
        String procedure = "FOR EACH ROW\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "while (1 = 0)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select vm1.g1.e1 from vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userQuery = "Insert into vm1.g1 (e1, e2) values ('String', 1)"; //$NON-NLS-1$
        
        String rewritProc = "FOR EACH ROW\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN ATOMIC\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$
        
        String procReturned = this.getRewritenProcedure(procedure, userQuery, 
                Table.TriggerEvent.INSERT);
                
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    /**
     * Tests that VariableSubstitutionVisitor does not cause an NPE on count(*)
     */
    @Test public void testRewriteProcedureWithCount() throws Exception {
        
        String procedure = "FOR EACH ROW\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select count(*) from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userQuery = "Insert into vm1.g1 (e1, e2) values ('String', 1)"; //$NON-NLS-1$
        
        String rewritProc = "FOR EACH ROW\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN ATOMIC\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "SELECT COUNT(*) FROM pm1.g1;\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$
        
        String procReturned = this.getRewritenProcedure(procedure, userQuery, 
                Table.TriggerEvent.INSERT);
                
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    @Test public void testRemoveEmptyLoop() {
        String procedure1 = "CREATE virtual PROCEDURE  "; //$NON-NLS-1$
        procedure1 += "BEGIN\n"; //$NON-NLS-1$
        procedure1 += "loop on (select e1 from pm1.g1) as myCursor\n"; //$NON-NLS-1$
        procedure1 += "begin\n"; //$NON-NLS-1$
        procedure1 += "end\n"; //$NON-NLS-1$
        procedure1 += "select e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure1 += "END"; //$NON-NLS-1$
        
        String expected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nSELECT e1 FROM pm1.g1;\nEND"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure1, expected);
    }
    
    @Test public void testRetainAtomic() {
        String procedure1 = "CREATE virtual PROCEDURE "; //$NON-NLS-1$
        procedure1 += "if (true)\n"; //$NON-NLS-1$
        procedure1 += "begin atomic\n"; //$NON-NLS-1$
        procedure1 += "select e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure1 += "end\n"; //$NON-NLS-1$
        
        String expected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nBEGIN ATOMIC\nSELECT e1 FROM pm1.g1;\nEND\nEND"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure1, expected);
    }
    
    @Test public void testExceptionHandling() {
        String procedure1 = "CREATE virtual PROCEDURE begin "; //$NON-NLS-1$
        procedure1 += "select 1/0;\n"; //$NON-NLS-1$
        procedure1 += "exception e\n"; //$NON-NLS-1$
        procedure1 += "end\n"; //$NON-NLS-1$
        
        String expected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nRAISE 'org.teiid.api.exception.query.ExpressionEvaluationException: TEIID30328 Unable to evaluate (1 / 0): TEIID30384 Error while evaluating function /';\nEXCEPTION e\nEND"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure1, expected);
    }
    
    @Test public void testRewriteDeclare() {
        String procedure1 = "CREATE virtual PROCEDURE  "; //$NON-NLS-1$
        procedure1 += "BEGIN\n"; //$NON-NLS-1$
        procedure1 += "declare integer x = 1 + 1;\n"; //$NON-NLS-1$
        procedure1 += "END"; //$NON-NLS-1$
        
        String expected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nDECLARE integer x = 2;\nEND"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure1, expected);
    }
      
    @Test public void testRewriteUnionJoin() {
        String sql = "select pm1.g1.e1 from pm1.g1 union join pm1.g2 where g1.e1 = 1"; //$NON-NLS-1$
        String expected = "SELECT pm1.g1.e1 FROM pm1.g1 FULL OUTER JOIN pm1.g2 ON 1 = 0 WHERE g1.e1 = '1'"; //$NON-NLS-1$
                
        helpTestRewriteCommand(sql, expected);        
    }

    @Test public void testRewriteNonNullDependentFunction() {
        helpTestRewriteCriteria("pm1.g1.e1 = concat(null, pm1.g1.e2)", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteInWithNull() {
        helpTestRewriteCriteria("convert(null, string) in (pm1.g1.e1, pm1.g1.e2)", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteXMLCriteriaCases5630And5640() {
        helpTestRewriteCommand("select * from xmltest.doc1 where node1 = null", "SELECT * FROM xmltest.doc1 WHERE node1 = null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCorrelatedSubqueryInHaving() {
        String sql = "select pm1.g1.e1 from pm1.g1 group by pm1.g1.e1 having pm1.g1.e1 in (select pm1.g1.e1 from pm1.g2)"; //$NON-NLS-1$
        String expected = "SELECT pm1.g1.e1 FROM pm1.g1 GROUP BY pm1.g1.e1 HAVING pm1.g1.e1 IN (SELECT pm1.g1.e1 FROM pm1.g2)"; //$NON-NLS-1$
                
        Query query = (Query)helpTestRewriteCommand(sql, expected);
        
        List<Reference> refs = new LinkedList<Reference>();
        
        CorrelatedReferenceCollectorVisitor.collectReferences(query, Arrays.asList(new GroupSymbol("pm1.g1")), refs);//$NON-NLS-1$
        
        assertEquals(1, refs.size());
    }
    
    @Test public void testRewriteSelectInto() {
        String sql = "select distinct pm1.g1.e1 into #temp from pm1.g1"; //$NON-NLS-1$
        String expected = "INSERT INTO #temp (e1) SELECT DISTINCT pm1.g1.e1 FROM pm1.g1"; //$NON-NLS-1$
                
        helpTestRewriteCommand(sql, expected);        
    }
    
    /**
     * Accounts for type change with duplicate names
     */
    @Test public void testRewriteSelectInto1() {
        String sql = "select distinct e2, e2, e3, e4 into pm1.g1 from pm1.g2"; //$NON-NLS-1$
        String expected = "INSERT INTO pm1.g1 (e1, e2, e3, e4) SELECT convert(X.e2, string) AS e1, X.e2_0 AS e2, X.e3, X.e4 FROM (SELECT DISTINCT e2, e2 AS e2_0, e3, e4 FROM pm1.g2) AS X"; //$NON-NLS-1$
                
        helpTestRewriteCommand(sql, expected);        
    }
    
    @Test public void testUnionQueryNullInOneBranch() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT e1, null FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER });
    }
    
    @Test public void testUnionQueryNullInOneBranch2() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT e1, e2 FROM pm1.g2 UNION ALL SELECT e1, null FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER });
    }

    @Test public void testUnionQueryNullInOneBranch3() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, null FROM pm1.g1 UNION ALL SELECT e1, null FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER });
    }

    @Test public void testUnionQueryNullInAllBranches() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, null FROM pm1.g1 UNION ALL SELECT e1, null FROM pm1.g2 UNION ALL SELECT e1, null FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
    }
    
    @Test public void testUnionQueryWithTypeConversion() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1 FROM pm1.g1 UNION ALL SELECT e2 FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING});
    }

    private void verifyProjectedTypesOnUnionBranches(String unionQuery, Class<?>[] types) throws TeiidComponentException, TeiidProcessingException {
        SetQuery union = (SetQuery)QueryParser.getQueryParser().parseCommand(unionQuery);
        QueryResolver.resolveCommand(union, RealMetadataFactory.example1Cached());
        
        union = (SetQuery)QueryRewriter.rewrite(union, RealMetadataFactory.example1Cached(), null);
        
        for (QueryCommand query : union.getQueryCommands()) {
            List<Expression> projSymbols = query.getProjectedSymbols();
            for(int i=0; i<projSymbols.size(); i++) {
                assertEquals("Found type mismatch at column " + i, types[i], projSymbols.get(i).getType()); //$NON-NLS-1$
            }                
        }
    }
        
    /**
     * The rewrite creates inline view to do the type conversion.
     * 
     * It also ensures that all project symbols are uniquely named in the inline view
     */
    @Test public void testSelectIntoWithOrderByAndTypeConversion() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE\n"; //$NON-NLS-1$
        procedure += "BEGIN\n";       //$NON-NLS-1$
        procedure += "CREATE local temporary table temp (x string, y integer, z integer);\n";       //$NON-NLS-1$
        procedure += "Select pm1.g1.e2, 1 as x, 2 as x into temp from pm1.g1 order by pm1.g1.e2 limit 1;\n"; //$NON-NLS-1$
        procedure += "Select x from temp;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure, "CREATE VIRTUAL PROCEDURE\nBEGIN\nCREATE LOCAL TEMPORARY TABLE temp (x string, y integer, z integer);\nINSERT INTO temp (x, y, z) SELECT convert(X.e2, string) AS x, X.x AS y, X.x_0 AS z FROM (SELECT pm1.g1.e2, 1 AS x, 2 AS x_0 FROM pm1.g1 ORDER BY pm1.g1.e2 LIMIT 1) AS X;\nSELECT x FROM temp;\nEND"); //$NON-NLS-1$
    }
    
    @Test public void testRewriteNot() {
        helpTestRewriteCriteria("not(not(pm1.g1.e1 = 1 + 1))", "pm1.g1.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteQueryWithNoFrom() {
        String sql = "select 1 as a order by a"; //$NON-NLS-1$
        
        helpTestRewriteCommand(sql, "SELECT 1 AS a"); //$NON-NLS-1$
    }
        
    /**
     * Case 4814
     */
    @Test public void testVirtualRightOuterJoinSwap() throws Exception {
        String sql = "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM (select * from BQT1.smalla) sa RIGHT OUTER JOIN (select BQT1.mediumb.intkey from BQT1.mediumb) mb ON sa.IntKey = mb.IntKey"; //$NON-NLS-1$
        helpTestRewriteCommand(sql, "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM (SELECT BQT1.mediumb.intkey FROM BQT1.mediumb) AS mb LEFT OUTER JOIN (SELECT * FROM BQT1.smalla) AS sa ON sa.IntKey = mb.IntKey", RealMetadataFactory.exampleBQTCached()); //$NON-NLS-1$
    }
    
    /**
     * Case 4814
     */
    @Test public void testVirtualRightOuterJoinSwap1() throws Exception {
        String sql = "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM ((select * from BQT1.smalla) sa inner join BQT1.smallb on sa.intkey = smallb.intkey) RIGHT OUTER JOIN (select BQT1.mediumb.intkey from BQT1.mediumb) mb ON sa.IntKey = mb.IntKey"; //$NON-NLS-1$
        helpTestRewriteCommand(sql, "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM (SELECT BQT1.mediumb.intkey FROM BQT1.mediumb) AS mb LEFT OUTER JOIN ((SELECT * FROM BQT1.smalla) AS sa INNER JOIN BQT1.smallb ON sa.intkey = smallb.intkey) ON sa.IntKey = mb.IntKey", RealMetadataFactory.exampleBQTCached()); //$NON-NLS-1$
    }
    
    @Test public void testRewriteConcat2() {
        helpTestRewriteCriteria("sys.concat2('a','b') = 'ab'", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteConcat2_1() {
        helpTestRewriteCriteria("concat2(null, null) is null", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteConcat2_2() throws Exception {
        helpTestRewriteCriteria("concat2(pm1.g1.e1, null) = 'xyz'", "pm1.g1.e1 = 'xyz'", true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteConcat2_4() throws Exception {
        helpTestRewriteCriteria("concat2('a', pm1.g1.e1) = 'xyz'", "concat2('a', pm1.g1.e1) = 'xyz'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteFromUnixTime() throws Exception {
    	TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
    	try {
    		helpTestRewriteCriteria("from_unixtime(pm1.g1.e2) = '1992-12-01 07:00:00'", "timestampadd(SQL_TSI_SECOND, pm1.g1.e2, {ts'1969-12-31 18:00:00.0'}) = {ts'1992-12-01 07:00:00.0'}"); //$NON-NLS-1$ //$NON-NLS-2$
    	} finally {
    		TimestampWithTimezone.resetCalendar(null);
    	}
    }
    
    @Test public void testRewriteNullIf() throws Exception {
    	helpTestRewriteCriteria("nullif(pm1.g1.e2, pm1.g1.e4) = 1", "CASE WHEN pm1.g1.e2 = pm1.g1.e4 THEN convert(null, double) ELSE pm1.g1.e2 END = 1.0", true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCoalesce() throws Exception {
    	helpTestRewriteCriteria("coalesce(convert(pm1.g1.e2, double), pm1.g1.e4) = 1", "ifnull(convert(pm1.g1.e2, double), pm1.g1.e4) = 1", true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testAggregateWithBetweenInCaseInSelect() {
    	String sqlBefore = "SELECT MAX(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1"; //$NON-NLS-1$
    	String sqlAfter  = "SELECT MAX(CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END) FROM pm1.g1"; //$NON-NLS-1$

    	helpTestRewriteCommand( sqlBefore, sqlAfter );
    }
    
    @Test public void testBetweenInCaseInSelect() {
    	String sqlBefore = "SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1"; //$NON-NLS-1$
    	String sqlAfter = "SELECT CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END FROM pm1.g1"; //$NON-NLS-1$
    	
    	helpTestRewriteCommand( sqlBefore, sqlAfter );
    }
    
    @Test public void testBetweenInCase() {
    	String sqlBefore = "SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END"; //$NON-NLS-1$
    	String sqlAfter = "SELECT * FROM pm1.g1 WHERE convert(e3, integer) = CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END"; //$NON-NLS-1$
    	
    	helpTestRewriteCommand( sqlBefore, sqlAfter );
    }
    
    @Test public void testRewriteNullHandling() {
    	String original = "pm1.g1.e1 like '%'"; //$NON-NLS-1$
    	String expected = "pm1.g1.e1 is not null"; //$NON-NLS-1$
    	addTestData();
    	
    	helpTestRewriteCriteria(original, expected);
    }

	@SuppressWarnings("unchecked")
	private void addTestData() {
		this.elements = new HashMap<ElementSymbol, Integer>();
    	elements.put(new ElementSymbol("pm1.g1.e1"), 0);
    	elements.put(new ElementSymbol("pm1.g1.e2"), 1);
    	elements.put(new ElementSymbol("pm1.g1.e3"), 2);
    	for (String s : Arrays.asList("a", null, "*")) {
			for (Integer i : Arrays.asList(1, null, 6)) {
	    		for (Boolean b : Arrays.asList(true, false, null)) {
    		    	tuples.add(Arrays.asList(s, i, b));
    			}
    		}
		}
	}
    
    @Test public void testRewriteNullHandling1() {
    	String original = "not(pm1.g1.e1 like '%' or pm1.g1.e1 = '1')"; //$NON-NLS-1$
    	String expected = "1 = 0"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling2() {
    	String original = "not(pm1.g1.e1 like '%' and pm1.g1.e1 = '1')"; //$NON-NLS-1$
    	String expected = "pm1.g1.e1 <> '1'"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling3() {
    	String original = "pm1.g1.e1 like '%' or pm1.g1.e1 = '1'"; //$NON-NLS-1$
    	String expected = "(pm1.g1.e1 IS NOT NULL) OR (pm1.g1.e1 = '1')"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling4() {
    	String original = "not((pm1.g1.e1 like '%' or pm1.g1.e3 = true) and pm1.g1.e2 < 5)"; //$NON-NLS-1$
    	String expected = "pm1.g1.e2 >= 5"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling4a() {
    	String original = "not(not((pm1.g1.e1 like '%' or pm1.g1.e3 = true) and pm1.g1.e2 < 5))"; //$NON-NLS-1$
    	String expected = "((pm1.g1.e1 IS NOT NULL) OR (pm1.g1.e3 = TRUE)) AND (pm1.g1.e2 < 5)"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling5() {
    	String original = "not((pm1.g1.e1 not like '%' or pm1.g1.e3 = true) and pm1.g1.e2 < 5)"; //$NON-NLS-1$
    	String expected = "((pm1.g1.e1 IS NOT NULL) AND (pm1.g1.e3 <> TRUE)) OR (pm1.g1.e2 >= 5)"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling6() {
    	String original = "not((pm1.g1.e1 not like '%' and pm1.g1.e3 = true) or pm1.g1.e2 < 5)"; //$NON-NLS-1$
    	String expected = "((pm1.g1.e1 IS NOT NULL) OR (pm1.g1.e3 <> TRUE)) AND (pm1.g1.e2 >= 5)"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling7() {
    	String original = "not(not(pm1.g1.e1 not like '%' and pm1.g1.e3 = true) or pm1.g1.e2 < 5)"; //$NON-NLS-1$
    	String expected = "1 = 0"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteNullHandling7a() {
    	String original = "not(not(pm1.g1.e1 like '*%' and pm1.g1.e3 = true) or pm1.g1.e2 < 5)"; //$NON-NLS-1$
    	String expected = "(pm1.g1.e1 LIKE '*%') AND (pm1.g1.e3 = TRUE) AND (pm1.g1.e2 >= 5)"; //$NON-NLS-1$
    	addTestData();
    	helpTestRewriteCriteria(original, expected);
    }
    
    @Test public void testRewriteChar() {
    	String original = "convert(pm1.g1.e1, char) = '100'"; //$NON-NLS-1$
    	String expected = "1 = 0"; //$NON-NLS-1$
    	
    	helpTestRewriteCriteria(original, expected);
    }
    
    /**
     * Test ensures that '22.0' is a valid long via bigdecimal
     */
    @Test public void testRewriteBigDecimal() {
    	String original = "convert(BQT1.SmallA.LongNum, bigdecimal) = '22.0'"; //$NON-NLS-1$
    	CompareCriteria crit = new CompareCriteria(new ElementSymbol("BQT1.SmallA.LongNum"), CompareCriteria.EQ, new Constant(new Long(22))); //$NON-NLS-1$
    	helpTestRewriteCriteria(original, crit, RealMetadataFactory.exampleBQTCached()); 
    }

    /**
     * Test ensures that we will not attempt to invert the widening conversion
     */
    @Test public void testRewriteWideningIn() {
    	String original = "convert(BQT1.SmallA.TimestampValue, time) in ({t'10:00:00'}, {t'11:00:00'})"; //$NON-NLS-1$
    	helpTestRewriteCriteria(original, parseCriteria("convert(BQT1.SmallA.TimestampValue, time) in ({t'10:00:00'}, {t'11:00:00'})", RealMetadataFactory.exampleBQTCached()), RealMetadataFactory.exampleBQTCached()); //$NON-NLS-1$ 
    }
    
    @Test public void testRewriteParseDate() {
    	String original = "parsedate(BQT1.SmallA.stringkey, 'yymmdd') = {d'1970-01-01'}"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteCriteria(original, parseCriteria("convert(parsetimestamp(BQT1.SmallA.stringkey, 'yymmdd'), date) = {d'1970-01-01'}", metadata), metadata); //$NON-NLS-1$
    }

    @Test public void testRewriteFormatTime() {
    	String original = "formattime(BQT1.SmallA.timevalue, 'hh:mm') = '08:02'"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteCriteria(original, parseCriteria("formattimestamp(convert(BQT1.SmallA.timevalue, timestamp), 'hh:mm') = '08:02'", metadata), metadata); //$NON-NLS-1$
    }
    
    @Test public void testRewriteTimestampAdd() {
    	String original = "timestampadd(SQL_TSI_SECOND, 1, BQT1.SmallA.timevalue) = {t'08:02:00'}"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteCriteria(original, parseCriteria("convert(timestampadd(SQL_TSI_SECOND, 1, convert(BQT1.SmallA.timevalue, timestamp)), time) = {t'08:02:00'}", metadata), metadata); //$NON-NLS-1$
    }
    
    @Test public void testRewriteXmlElement() throws Exception {
    	String original = "xmlserialize(document xmlelement(name a, xmlattributes('b' as c)) as string)"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteExpression(original, "'<a c=\"b\"></a>'", metadata);
    }
    
    @Test public void testRewriteXmlElement1() throws Exception {
    	String original = "xmlelement(name a, xmlattributes(1+1 as c), BQT1.SmallA.timevalue)"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteExpression(original, "XMLELEMENT(NAME a, XMLATTRIBUTES(2 AS c), BQT1.SmallA.timevalue)", metadata);
    }
    
    @Test public void testRewriteXmlSerialize() throws Exception {
    	String original = "xmlserialize(document xmlelement(name a, xmlattributes('b' as c)) as string)"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteExpression(original, "'<a c=\"b\"></a>'", metadata);
    }
    
    @Test public void testRewriteXmlTable() throws Exception {
    	String original = "select * from xmltable('/' passing 1 + 1 as a columns x string default curdate()) as x"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteCommand(original, "SELECT * FROM XMLTABLE('/' PASSING 2 AS a COLUMNS x string DEFAULT convert(curdate(), string)) AS x", metadata);
    }
    
    @Test public void testRewriteQueryString() throws Exception {
    	String original = "querystring('path', 'value' as \"&x\", ' & ' as y, null as z)"; //$NON-NLS-1$
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	helpTestRewriteExpression(original, "'path?%26x=value&y=%20%26%20'", metadata);
    }
    
    @Test public void testRewriteExpressionCriteria() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e3", "pm1.g1.e3 = true");
    }
    
    @Test public void testRewritePredicateOptimization() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 in (1, 2, 3) and pm1.g1.e2 in (2, 3, 4)", "pm1.g1.e2 in (2, 3)");
    }
    
    @Test public void testRewritePredicateOptimization1() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 < 5 and pm1.g1.e2 = 2", "pm1.g1.e2 = 2");
    }
    
    @Test public void testRewritePredicateOptimization2() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 < 5 and pm1.g1.e2 = 6", "1 = 0");
    }
    
    @Test public void testRewritePredicateOptimization2a() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 < 5 and pm1.g1.e2 = 2", "pm1.g1.e2 = 2");
    }

    @Test public void testRewritePredicateOptimization3() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 in (1, 2) and pm1.g1.e2 = 6", "1 = 0");
    }
    
    @Test public void testRewritePredicateOptimization4() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 in (1, 2) and pm1.g1.e2 is null", "1 = 0");
    }
    
    @Test public void testRewritePredicateOptimization5() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 <> 5 and pm1.g1.e2 in (2, 3, 5)", "pm1.g1.e2 in (2, 3)");
    }
    
    @Test public void testRewritePredicateOptimization6() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 = 5 and pm1.g1.e2 in (5, 6)", "pm1.g1.e2 = 5");
    }
    
    @Test public void testRewritePredicateOptimization6a() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 in (5, 6) and pm1.g1.e2 = 5", "pm1.g1.e2 = 5");
    }

    @Ignore("TODO")
    @Test public void testRewritePredicateOptimization7() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 > 5 and pm1.g1.e2 < 2", "1 = 0");
    }
    
    @Test public void testRewritePredicateOptimization8() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 = 2 and pm1.g1.e2 > 1", "pm1.g1.e2 = 2");
    }
    
    @Test public void testRewritePredicateOptimization8a() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 in (0, 2) and pm1.g1.e2 > 1", "pm1.g1.e2 = 2");
    }
    
    @Test public void testRewritePredicateOptimization9() throws Exception {
    	helpTestRewriteCriteria("not(pm1.g1.e2 = 2 and pm1.g1.e2 = 3)", "(pm1.g1.e2 <> 2) OR (pm1.g1.e2 <> 3)");
    }
    
    @Test public void testRewritePredicateOptimizationOr() throws Exception {
    	helpTestRewriteCriteria("pm1.g1.e2 in (5, 6) or pm1.g1.e2 = 2", "pm1.g1.e2 IN (2, 5, 6)");
    }

    @Test public void testRewriteCritSubqueryNegate() {
        helpTestRewriteCriteria("not(pm1.g1.e1 > SOME (select 'a' from pm1.g2))", "pm1.g1.e1 <= ALL (SELECT 'a' FROM pm1.g2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritSubqueryFalse() {
        helpTestRewriteCriteria("exists(select 1 from pm1.g1 where 1=0)", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritSubqueryFalse1() {
        helpTestRewriteCriteria("not(pm1.g1.e1 > SOME (select 'a' from pm1.g1 where 1=0))", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritSubqueryFalse2() {
        helpTestRewriteCriteria("pm1.g1.e1 < ALL (select 'a' from pm1.g1 where 1=0)", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteCritSubqueryFalse3() {
        helpTestRewriteCriteria("pm1.g1.e1 not in (select 'a' from pm1.g1 where 1=0)", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
	@Test public void testUDFParse() throws Exception {     
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		String sql = "parsedate_(pm1.g1.e1) = {d'2001-01-01'}";
        helpTestRewriteCriteria(sql, parseCriteria(sql, metadata), metadata);  		
	} 
	
    @Test public void testRewriteNestedConvert() throws Exception {
        helpTestRewriteExpression("cast(cast(pm1.g1.e3 as integer) as long)", "cast(pm1.g1.e3 as long)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteNestedConvert1() throws Exception {
        helpTestRewriteExpression("cast(cast(pm1.g1.e3 as integer) as string)", "convert(convert(pm1.g1.e3, integer), string)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteNestedConvert2() throws Exception {
        helpTestRewriteExpression("cast(cast(pm1.g1.e3 as string) as clob)", "convert(convert(pm1.g1.e3, string), clob)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteConstantAgg() throws Exception {
    	helpTestRewriteCommand("select max(1) from pm1.g1 group by e1", "SELECT 1 FROM pm1.g1 GROUP BY e1");
    }
    
    @Test public void testRewriteTrim() throws Exception {
    	helpTestRewriteExpression("trim(pm1.g1.e1)", "rtrim(ltrim(pm1.g1.e1))", RealMetadataFactory.example1Cached());
    }

    @Test public void testRewriteTrim1() throws Exception {
    	helpTestRewriteExpression("trim(leading from pm1.g1.e1)", "ltrim(pm1.g1.e1)", RealMetadataFactory.example1Cached());
    }
    
    @Test public void testRewriteXmlSerialize1() throws Exception {
    	helpTestRewriteExpression("xmlserialize(DOCUMENT cast (pm1.g1.e1 as xml) as clob version '2.0')", "XMLSERIALIZE(DOCUMENT convert(pm1.g1.e1, xml) AS clob VERSION '2.0' INCLUDING XMLDECLARATION)", RealMetadataFactory.example1Cached());
    }
    
    @Test public void testRewriteMerge() throws Exception {
		String ddl = "CREATE foreign table x (y string primary key)";

		QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");
		
    	helpTestRewriteCommand("merge into x (y) values (1)", "CREATE VIRTUAL PROCEDURE\nBEGIN ATOMIC\nDECLARE integer VARIABLES.ROWS_UPDATED = 0;\nLOOP ON (SELECT X.expr1 AS y FROM (SELECT '1' AS expr1) AS X) AS X1\nBEGIN\nIF(EXISTS (SELECT 1 FROM x WHERE y = X1.y LIMIT 1))\nBEGIN\nEND\nELSE\nBEGIN\nINSERT INTO x (y) VALUES (X1.y);\nEND\nVARIABLES.ROWS_UPDATED = (VARIABLES.ROWS_UPDATED + 1);\nEND\nSELECT VARIABLES.ROWS_UPDATED;\nEND", metadata);
    }

}
