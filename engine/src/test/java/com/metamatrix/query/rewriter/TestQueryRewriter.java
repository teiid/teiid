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

package com.metamatrix.query.rewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ContextProperties;

public class TestQueryRewriter extends TestCase {

    private static final String TRUE_STR = "1 = 1"; //$NON-NLS-1$
    private static final String FALSE_STR = "1 = 0"; //$NON-NLS-1$

    // ################################## FRAMEWORK ################################
    
    public TestQueryRewriter(String name) { 
        super(name);
    }

    // ################################## TEST HELPERS ################################
    
    private Criteria parseCriteria(String critStr, QueryMetadataInterface metadata) {
        try {
            Criteria crit = QueryParser.getQueryParser().parseCriteria(critStr);
            
            // resolve against metadata
            QueryResolver.resolveCriteria(crit, metadata);
            
            return crit;
        } catch(MetaMatrixException e) {
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
    
    private Criteria helpTestRewriteCriteria(String original, String expected, boolean rewrite) throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException, QueryValidatorException {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria expectedCrit = parseCriteria(expected, metadata);
        if (rewrite) {
            QueryResolver.resolveCriteria(expectedCrit, metadata);
            expectedCrit = QueryRewriter.rewriteCriteria(expectedCrit, null, null, metadata);
        }
        return helpTestRewriteCriteria(original, expectedCrit, metadata);
    }

    private Criteria helpTestRewriteCriteria(String original, Criteria expectedCrit, QueryMetadataInterface metadata) {
        Criteria origCrit = parseCriteria(original, metadata);
        
        Criteria actual = null;
        // rewrite
        try { 
            actual = QueryRewriter.rewriteCriteria(origCrit, null, null, null);
            assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
        } catch(QueryValidatorException e) { 
            e.printStackTrace();
            fail("Exception during rewriting (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        }
        return actual;
    }    
    
	private String getReWrittenProcedure(String procedure, String userUpdateStr, String procedureType) {
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(procedureType, procedure);

        try {
            Command userCommand = QueryParser.getQueryParser().parseCommand(userUpdateStr);       
            QueryResolver.resolveCommand(userCommand, metadata);
    		QueryRewriter.rewrite(userCommand, null, metadata, null);
    		return userCommand.getSubCommands().get(0).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	private void helpFailUpdateProcedure(String procedure, String userUpdateStr, String procedureType) {
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(procedureType, procedure);

        Command userCommand = null;       
        try {
            QueryParser parser = new QueryParser();
            userCommand = parser.parseCommand(userUpdateStr);
            QueryResolver.resolveCommand(userCommand, metadata);            
        } catch(MetaMatrixException e) {
            e.printStackTrace();
			fail("Exception during parsing/resolution (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }

		QueryValidatorException exception = null;
        try {		
			QueryRewriter.rewrite(userCommand, null, metadata, null);
        } catch(QueryValidatorException e) {
        	exception = e;
        }

		assertNotNull("Expected a QueryValidatorException but got none.", exception); //$NON-NLS-1$
	}

    private Command helpTestRewriteCommand(String original, String expected) { 
        try {
            return helpTestRewriteCommand(original, expected, FakeMetadataFactory.example1Cached());
        } catch(MetaMatrixException e) { 
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    private Command helpTestRewriteCommand(String original, String expected, QueryMetadataInterface metadata) throws MetaMatrixException { 
        Command command = QueryParser.getQueryParser().parseCommand(original);            
        QueryResolver.resolveCommand(command, metadata);
        Command rewriteCommand = QueryRewriter.rewrite(command, null, metadata, null);
        assertEquals("Rewritten command was not expected", expected, rewriteCommand.toString()); //$NON-NLS-1$
        return rewriteCommand;
    }
    
    public void testRewriteUnknown() {
        helpTestRewriteCriteria("pm1.g1.e1 = '1' and '1' = convert(null, string)", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteUnknown1() {
        helpTestRewriteCriteria("pm1.g1.e1 = '1' or '1' = convert(null, string)", "pm1.g1.e1 = '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteUnknown2() {
        helpTestRewriteCriteria("not('1' = convert(null, string))", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteUnknown3() {
        helpTestRewriteCriteria("pm1.g1.e1 like convert(null, string))", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteUnknown4() {
        helpTestRewriteCriteria("null in ('a', 'b', 'c')", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteUnknown5() {
        helpTestRewriteCriteria("(null <> null) and 1 = 0", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteUnknown6() {
        helpTestRewriteCriteria("not(pm1.g1.e1 = '1' and '1' = convert(null, string))", "NOT ((pm1.g1.e1 = '1') and (NULL <> NULL))"); //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    public void testRewriteUnknown7() {
        helpTestRewriteCriteria("not(pm1.g1.e1 = '1' or '1' = convert(null, string))", "NOT ((pm1.g1.e1 = '1') or (NULL <> NULL))"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteUnknown8() {
        helpTestRewriteCriteria("pm1.g1.e1 in (2, null)", "pm1.g1.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$ 
    }
    
    public void testRewriteInCriteriaWithRepeats() {
        helpTestRewriteCriteria("pm1.g1.e1 in ('1', '1', '2')", "pm1.g1.e1 IN ('1', '2')"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteInCriteriaWithSingleValue() {
        helpTestRewriteCriteria("pm1.g1.e1 in ('1')", "pm1.g1.e1 = '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteInCriteriaWithSingleValue1() {
        helpTestRewriteCriteria("pm1.g1.e1 not in ('1')", "pm1.g1.e1 != '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteInCriteriaWithNoValues() throws Exception {
        Criteria crit = new SetCriteria(new ElementSymbol("e1"), Collections.EMPTY_LIST); //$NON-NLS-1$
        
        Criteria actual = QueryRewriter.rewriteCriteria(crit, null, null, null);
        
        assertEquals(QueryRewriter.FALSE_CRITERIA, actual);
    }
        
    public void testRewriteBetweenCriteria1() {
        helpTestRewriteCriteria("pm1.g1.e1 BETWEEN 1000 AND 2000", "(pm1.g1.e1 >= '1000') AND (pm1.g1.e1 <= '2000')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteBetweenCriteria2() {
        helpTestRewriteCriteria("pm1.g1.e1 NOT BETWEEN 1000 AND 2000", "(pm1.g1.e1 < '1000') OR (pm1.g1.e1 > '2000')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCrit1() {
        helpTestRewriteCriteria("concat('a','b') = 'ab'", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCrit2() {
        helpTestRewriteCriteria("'x' = pm1.g1.e1", "(pm1.g1.e1 = 'x')"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCrit3() {
        helpTestRewriteCriteria("pm1.g1.e1 = convert('a', string)", "pm1.g1.e1 = 'a'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCrit4() {
        helpTestRewriteCriteria("pm1.g1.e1 = CONVERT('a', string)", "pm1.g1.e1 = 'a'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCrit5() {
        helpTestRewriteCriteria("pm1.g1.e1 in ('a')", "pm1.g1.e1 = 'a'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCrit6() {
        helpTestRewriteCriteria("1 = convert(pm1.g1.e1,integer) + 10", "pm1.g1.e1 = '-9'"); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    public void testRewriteCrit7() {
        helpTestRewriteCriteria("((pm1.g1.e1 = 1) and (pm1.g1.e1 = 1))", "pm1.g1.e1 = '1'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteMatchCritEscapeChar1() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE 'x_' ESCAPE '\\'", "pm1.g1.e1 LIKE 'x_'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteMatchCritEscapeChar2() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE '#%x' ESCAPE '#'", "pm1.g1.e1 LIKE '#%x' ESCAPE '#'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteMatchCritEscapeChar3() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE '#%x'", "pm1.g1.e1 LIKE '#%x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteMatchCritEscapeChar4() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE pm1.g1.e1 ESCAPE '#'", "pm1.g1.e1 LIKE pm1.g1.e1 ESCAPE '#'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteMatchCritEscapeChar5() throws Exception {
        MatchCriteria mcrit = new MatchCriteria(new ElementSymbol("pm1.g1.e1"), new Constant(null, DataTypeManager.DefaultDataClasses.STRING), '#'); //$NON-NLS-1$
        Criteria expected = QueryRewriter.UNKNOWN_CRITERIA; 
                
        Object actual = QueryRewriter.rewriteCriteria(mcrit, null, null, null); 
        assertEquals("Did not get expected rewritten criteria", expected, actual); //$NON-NLS-1$
    }
    
    public void testRewriteMatchCrit1() {
        helpTestRewriteCriteria("pm1.g1.e1 LIKE 'x' ESCAPE '\\'", "pm1.g1.e1 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteMatchCrit2() {
        helpTestRewriteCriteria("pm1.g1.e1 NOT LIKE 'x'", "pm1.g1.e1 <> 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteMatchCrit3() {
        helpTestRewriteCriteria("pm1.g1.e1 NOT LIKE '%'", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCritTimestampCreate1() {
        helpTestRewriteCriteria("timestampCreate(pm3.g1.e2, pm3.g1.e3) = {ts'2004-11-23 09:25:00'}", "(pm3.g1.e2 = {d'2004-11-23'}) AND (pm3.g1.e3 = {t'09:25:00'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritTimestampCreate2() {
        helpTestRewriteCriteria("{ts'2004-11-23 09:25:00'} = timestampCreate(pm3.g1.e2, pm3.g1.e3)", "(pm3.g1.e2 = {d'2004-11-23'}) AND (pm3.g1.e3 = {t'09:25:00'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritSwap1() {
        helpTestRewriteCriteria("'x' = pm1.g1.e1", "pm1.g1.e1 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritSwap2() {
        helpTestRewriteCriteria("'x' <> pm1.g1.e1", "pm1.g1.e1 <> 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritSwap3() {
        helpTestRewriteCriteria("'x' < pm1.g1.e1", "pm1.g1.e1 > 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritSwap4() {
        helpTestRewriteCriteria("'x' <= pm1.g1.e1", "pm1.g1.e1 >= 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritSwap5() {
        helpTestRewriteCriteria("'x' > pm1.g1.e1", "pm1.g1.e1 < 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritSwap6() {
        helpTestRewriteCriteria("'x' >= pm1.g1.e1", "pm1.g1.e1 <= 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_op1() {
        helpTestRewriteCriteria("pm1.g1.e2 + 5 = 10", "pm1.g1.e2 = 5"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_op2() {
        helpTestRewriteCriteria("pm1.g1.e2 - 5 = 10", "pm1.g1.e2 = 15"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_op3() {
        helpTestRewriteCriteria("pm1.g1.e2 * 5 = 10", "pm1.g1.e2 = 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_op4() {
        helpTestRewriteCriteria("pm1.g1.e2 / 5 = 10", "pm1.g1.e2 = 50"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_signFlip1() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 > 10", "pm1.g1.e2 < -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_signFlip2() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 >= 10", "pm1.g1.e2 <= -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCritExpr_signFlip3() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 < 10", "pm1.g1.e2 > -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_signFlip4() {
        helpTestRewriteCriteria("pm1.g1.e2 * -5 <= 10", "pm1.g1.e2 >= -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCritExpr_backwards1() {
        helpTestRewriteCriteria("5 + pm1.g1.e2 <= 10", "pm1.g1.e2 <= 5"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_backwards2() {
        helpTestRewriteCriteria("-5 * pm1.g1.e2 <= 10", "pm1.g1.e2 >= -2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCritExpr_unhandled1() {
        helpTestRewriteCriteria("5 / pm1.g1.e2 <= 10", "5 / pm1.g1.e2 <= 10"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCritExpr_unhandled2() {
        helpTestRewriteCriteria("5 - pm1.g1.e2 <= 10", "5 - pm1.g1.e2 <= 10"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCrit_parseDate() {
        helpTestRewriteCriteria("PARSEDATE(pm3.g1.e1, 'yyyyMMdd') = {d'2003-05-01'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '20030501'" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseDate1() {
        helpTestRewriteCriteria("PARSEDATE(pm3.g1.e1, 'yyyyMM') = {d'2003-05-01'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '200305'" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseDate2() {
        helpTestRewriteCriteria("PARSEDATE(pm3.g1.e1, 'yyyyMM') = {d'2003-05-02'}", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_invalidParseDate() {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1Cached();
        Criteria origCrit = parseCriteria("PARSEDATE(pm3.g1.e1, '''') = {d'2003-05-01'}", metadata); //$NON-NLS-1$
        
        try { 
            QueryRewriter.rewriteCriteria(origCrit, null, null, null);
            fail("Expected failure"); //$NON-NLS-1$
        } catch(QueryValidatorException e) { 
            assertEquals("Error Code:ERR.015.001.0003 Message:Error simplifying criteria: PARSEDATE(pm3.g1.e1, '''') = {d'2003-05-01'}", e.getMessage());     //$NON-NLS-1$
        }
    }
    
    public void testRewriteCrit_parseTime() {
        helpTestRewriteCriteria("PARSETIME(pm3.g1.e1, 'HH mm ss') = {t'13:25:04'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '13 25 04'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseTimestamp() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') = {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseTimestamp1() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') = {ts'2003-01-01 00:25:00.0'}", //$NON-NLS-1$
                                "pm3.g1.e1 = '2003 01 25'" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseTimestamp2() {
        helpTestRewriteCriteria("PARSETimestamp(CONVERT(pm3.g1.e2, string), 'yyyy-MM-dd') = {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseTimestamp3() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') <> {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "1 = 1" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseTimestamp4() {
        helpTestRewriteCriteria("PARSETimestamp(CONVERT(pm3.g1.e2, string), 'yyyy-MM-dd') = {ts'2003-05-01 00:00:00.0'}", //$NON-NLS-1$
                                "pm3.g1.e2 = {d'2003-05-01'}" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseTimestamp_notEquality() {
        helpTestRewriteCriteria("PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') > {ts'2003-05-01 13:25:04.5'}", //$NON-NLS-1$
                                "PARSETimestamp(pm3.g1.e1, 'yyyy dd mm') > {ts'2003-05-01 13:25:04.5'}" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_parseTimestamp_decompose() {
        helpTestRewriteCriteria("PARSETIMESTAMP(CONCAT(FORMATDATE(pm3.g1.e2, 'yyyyMMdd'), FORMATTIME(pm3.g1.e3, 'HHmmss')), 'yyyyMMddHHmmss') = PARSETIMESTAMP('19690920183045', 'yyyyMMddHHmmss')", //$NON-NLS-1$
        "(pm3.g1.e2 = {d'1969-09-20'}) AND (pm3.g1.e3 = {t'18:30:45'})" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_timestampCreate_decompose() {
        helpTestRewriteCriteria("timestampCreate(pm3.g1.e2, pm3.g1.e3) = PARSETIMESTAMP('19690920183045', 'yyyyMMddHHmmss')", //$NON-NLS-1$
        "(pm3.g1.e2 = {d'1969-09-20'}) AND (pm3.g1.e3 = {t'18:30:45'})" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseInteger() {
        helpTestRewriteCriteria("parseInteger(pm1.g1.e1, '#,##0') = 1234", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseLong() {
        helpTestRewriteCriteria("parseLong(pm1.g1.e1, '#,##0') = convert(1234, long)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseBigInteger() {
        helpTestRewriteCriteria("parseBigInteger(pm1.g1.e1, '#,##0') = convert(1234, biginteger)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseFloat() {
        helpTestRewriteCriteria("parseFloat(pm1.g1.e1, '#,##0.###') = convert(1234.1234, float)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234.123'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseDouble() {
        helpTestRewriteCriteria("parseDouble(pm1.g1.e1, '$#,##0.00') = convert(1234.5, double)", //$NON-NLS-1$
                                "pm1.g1.e1 = '$1,234.50'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_parseBigDecimal() {
        helpTestRewriteCriteria("parseBigDecimal(pm1.g1.e1, '#,##0.###') = convert(1234.1234, bigdecimal)", //$NON-NLS-1$
                                "pm1.g1.e1 = '1,234.123'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_formatDate() {
        helpTestRewriteCriteria("formatDate(pm3.g1.e2, 'yyyyMMdd') = '20030501'", //$NON-NLS-1$
                                "pm3.g1.e2 = {d'2003-05-01'}" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_formatTime() {
        helpTestRewriteCriteria("formatTime(pm3.g1.e3, 'HH mm ss') = '13 25 04'", //$NON-NLS-1$
                                "pm3.g1.e3 = {t'13:25:04'}" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_formatTimestamp() {
        helpTestRewriteCriteria("formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss') = '05 01, 1974 - 07:00:00'", //$NON-NLS-1$
                                "formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss') = '05 01, 1974 - 07:00:00'" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_formatTimestamp1() {
        helpTestRewriteCriteria("formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss.S') = '05 01, 1974 - 07:00:00.0'", //$NON-NLS-1$
                                "pm3.g1.e4 = {ts'1974-05-01 07:00:00.0'}" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_formatInteger() {
        helpTestRewriteCriteria("formatInteger(pm1.g1.e2, '#,##0') = '1,234'", //$NON-NLS-1$
                                "pm1.g1.e2 = 1234" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_formatInteger1() {
        helpTestRewriteCriteria("formatInteger(pm1.g1.e2, '#5') = '105'", //$NON-NLS-1$
                                "formatInteger(pm1.g1.e2, '#5') = '105'" );         //$NON-NLS-1$
    }

    public void testRewriteCrit_formatLong() {
        helpTestRewriteCriteria("formatLong(convert(pm1.g1.e2, long), '#,##0') = '1,234,567,890,123'", //$NON-NLS-1$
                                "1 = 0" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_formatLong1() {
        helpTestRewriteCriteria("formatLong(convert(pm1.g1.e2, long), '#,##0') = '1,234,567,890'", //$NON-NLS-1$
                                "pm1.g1.e2 = 1234567890" );         //$NON-NLS-1$
    }
    
    public void testRewriteCrit_formatTimestampInvert() { 
        String original = "formatTimestamp(pm3.g1.e4, 'MM dd, yyyy - HH:mm:ss.S') = ?"; //$NON-NLS-1$ 
        String expected = "pm3.g1.e4 = parseTimestamp(?, 'MM dd, yyyy - HH:mm:ss.S')"; //$NON-NLS-1$ 
         
        helpTestRewriteCriteria(original, expected); 
    } 
     
    public void testRewriteCrit_plusInvert() { 
        String original = "pm1.g1.e2 + 1.1 = ?"; //$NON-NLS-1$ 
        String expected = "pm1.g1.e2 = ? - 1.1"; //$NON-NLS-1$ 
         
        helpTestRewriteCriteria(original, expected);
    } 

    public void testRewriteCrit_formatBigInteger() throws Exception {
        String original = "formatBigInteger(convert(pm1.g1.e2, biginteger), '#,##0') = '1,234,567,890'"; //$NON-NLS-1$
        String expected = "pm1.g1.e2 = 1234567890"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        Criteria expectedCrit = parseCriteria(expected, metadata);
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null, null);
        assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
    }

    public void testRewriteCrit_formatFloat() throws Exception {
        String original = "formatFloat(convert(pm1.g1.e4, float), '#,##0.###') = '1,234.123'"; //$NON-NLS-1$
        String expected = "pm1.g1.e4 = 1234.123046875"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null, null);
        assertEquals("Did not rewrite correctly: ", expected, actual.toString()); //$NON-NLS-1$
    }

    public void testRewriteCrit_formatDouble() throws Exception {
        String original = "formatDouble(convert(pm1.g1.e4, double), '$#,##0.00') = '$1,234.50'"; //$NON-NLS-1$
        String expected = "pm1.g1.e4 = '1234.5'"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        Criteria expectedCrit = parseCriteria(expected, metadata);
        ((CompareCriteria)expectedCrit).setRightExpression(new Constant(new Double(1234.5)));
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null, null);
        assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
    }

    public void testRewriteCrit_formatBigDecimal() throws Exception {
        String original = "formatBigDecimal(convert(pm1.g1.e4, bigdecimal), '#,##0.###') = convert(1234.5, bigdecimal)"; //$NON-NLS-1$
        String expected = "pm1.g1.e4 = 1234.5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria(original, metadata);
        Criteria expectedCrit = parseCriteria(expected, metadata);
        
        // rewrite
        Criteria actual = QueryRewriter.rewriteCriteria(origCrit, null, null, null);
        assertEquals("Did not rewrite correctly: ", expectedCrit, actual); //$NON-NLS-1$
    }
    
    public void testRewriteCritTimestampDiffDate1() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_DAY, {d'2003-05-15'}, {d'2003-05-17'} ) = 2", TRUE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteCritTimestampDiffDate2() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_DAY, {d'2003-06-02'}, {d'2003-05-17'} ) = -16", TRUE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteCritTimestampDiffDate3() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_QUARTER, {d'2002-01-25'}, {d'2003-06-01'} ) = 5", TRUE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteCritTimestampDiffTime1() {
        helpTestRewriteCriteria("timestampdiff(SQL_TSI_HOUR, {t'03:04:45'}, {t'05:05:36'} ) = 2", TRUE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteCritTimestampDiffTime1_ignorecase() {
        helpTestRewriteCriteria("timestampdiff(SQL_tsi_HOUR, {t'03:04:45'}, {t'05:05:36'} ) = 2", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteOr1() {
        helpTestRewriteCriteria("(5 = 5) OR (0 = 1)", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteOr2() {
        helpTestRewriteCriteria("(0 = 1) OR (5 = 5)", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteOr3() {
        helpTestRewriteCriteria("(1 = 1) OR (5 = 5)", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteOr4() {
        helpTestRewriteCriteria("(0 = 1) OR (4 = 5)", FALSE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteOr5() {
        helpTestRewriteCriteria("(0 = 1) OR (4 = 5) OR (pm1.g1.e1 = 'x')", "(pm1.g1.e1 = 'x')");         //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteOr6() {
        helpTestRewriteCriteria("(0 = 1) OR (4 = 5) OR (pm1.g1.e1 = 'x') OR (pm1.g1.e1 = 'y')", "(pm1.g1.e1 = 'x') OR (pm1.g1.e1 = 'y')");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteOr7() {
        helpTestRewriteCriteria("(pm1.g1.e1 = 'x') OR (pm1.g1.e1 = 'y')", "(pm1.g1.e1 = 'x') OR (pm1.g1.e1 = 'y')");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteAnd1() {
        helpTestRewriteCriteria("(5 = 5) AND (0 = 1)", FALSE_STR); //$NON-NLS-1$
    }

    public void testRewriteAnd2() {
        helpTestRewriteCriteria("(0 = 1) AND (5 = 5)", FALSE_STR); //$NON-NLS-1$
    }

    public void testRewriteAnd3() {
        helpTestRewriteCriteria("(1 = 1) AND (5 = 5)", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteAnd4() {
        helpTestRewriteCriteria("(0 = 1) AND (4 = 5)", FALSE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteAnd5() { 
        helpTestRewriteCriteria("(1 = 1) AND (5 = 5) AND (pm1.g1.e1 = 'x')", "(pm1.g1.e1 = 'x')");             //$NON-NLS-1$ //$NON-NLS-2$
    }
 
    public void testRewriteAnd6() { 
        helpTestRewriteCriteria("(1 = 1) AND (5 = 5) AND (pm1.g1.e1 = 'x') and (pm1.g1.e1 = 'y')", "(pm1.g1.e1 = 'x') AND (pm1.g1.e1 = 'y')");             //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteAnd7() {
        helpTestRewriteCriteria("(pm1.g1.e1 = 'x') AND (pm1.g1.e1 = 'y')", "(pm1.g1.e1 = 'x') AND (pm1.g1.e1 = 'y')");     //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    public void testRewriteMixed1() {
        helpTestRewriteCriteria("((1=1) AND (1=1)) OR ((1=1) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed2() {
        helpTestRewriteCriteria("((1=2) AND (1=1)) OR ((1=1) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed3() {
        helpTestRewriteCriteria("((1=1) AND (1=2)) OR ((1=1) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed4() {
        helpTestRewriteCriteria("((1=1) AND (1=1)) OR ((1=2) AND (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed5() {
        helpTestRewriteCriteria("((1=1) AND (1=1)) OR ((1=1) AND (1=2))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed6() {
        helpTestRewriteCriteria("((1=2) AND (1=1)) OR ((1=2) AND (1=1))", FALSE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed7() {
        helpTestRewriteCriteria("((1=1) AND (1=2)) OR ((1=1) AND (1=2))", FALSE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed8() {
        helpTestRewriteCriteria("((1=2) AND (1=2)) OR ((1=2) AND (1=2))", FALSE_STR); //$NON-NLS-1$
    }
    
    public void testRewriteMixed9() {
        helpTestRewriteCriteria("((1=1) OR (1=1)) AND ((1=1) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed10() {
        helpTestRewriteCriteria("((1=2) OR (1=1)) AND ((1=1) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed11() {
        helpTestRewriteCriteria("((1=1) OR (1=2)) AND ((1=1) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed12() {
        helpTestRewriteCriteria("((1=1) OR (1=1)) AND ((1=2) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed13() {
        helpTestRewriteCriteria("((1=1) OR (1=1)) AND ((1=1) OR (1=2))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed14() {
        helpTestRewriteCriteria("((1=2) OR (1=1)) AND ((1=2) OR (1=1))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed15() {
        helpTestRewriteCriteria("((1=1) OR (1=2)) AND ((1=1) OR (1=2))", TRUE_STR); //$NON-NLS-1$
    }

    public void testRewriteMixed16() {
        helpTestRewriteCriteria("((1=2) OR (1=2)) AND ((1=2) OR (1=2))", FALSE_STR); //$NON-NLS-1$
    }

    public void testRewriteNot1() {
        helpTestRewriteCriteria("NOT (1=1)", FALSE_STR);     //$NON-NLS-1$
    }   

    public void testRewriteNot2() {
        helpTestRewriteCriteria("NOT (1=2)", TRUE_STR);     //$NON-NLS-1$
    }   
    
    public void testRewriteNot3() {
        helpTestRewriteCriteria("NOT (pm1.g1.e1='x')", "NOT (pm1.g1.e1 = 'x')");     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteDefect1() {
        helpTestRewriteCriteria("(('DE' = 'LN') AND (null > '2002-01-01')) OR (('DE' = 'DE') AND (pm1.g1.e1 > '9000000'))", "(pm1.g1.e1 > '9000000')");         //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteQueryCriteriaAlwaysTrue() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE 0 = 0", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1"); //$NON-NLS-1$
    }
    
    public void testSubquery1() {
        helpTestRewriteCommand("SELECT e1 FROM (SELECT e1 FROM pm1.g1 WHERE (1 - 1) = (0 + 0)) AS x", //$NON-NLS-1$
                                "SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS x"); //$NON-NLS-1$
    }

    public void testExistsSubquery() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE EXISTS (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }

    public void testCompareSubqueryANY() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE '3' = ANY (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE '3' = SOME (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }

    public void testCompareSubquery() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE '3' = SOME (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE '3' = SOME (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }
    
    public void testCompareSubqueryUnknown() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE null = SOME (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE null <> null"); //$NON-NLS-1$
    }

    public void testINClauseSubquery() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 WHERE '3' IN (SELECT e1 FROM pm1.g2)", //$NON-NLS-1$
                                "SELECT e1 FROM pm1.g1 WHERE '3' IN (SELECT e1 FROM pm1.g2)"); //$NON-NLS-1$
    }

    public void testRewriteXMLCriteria1() {
        helpTestRewriteCriteria("context(pm1.g1.e1, pm1.g1.e1) = convert(5, string)", "context(pm1.g1.e1, pm1.g1.e1) = '5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteXMLCriteria2() {
        helpTestRewriteCriteria("context(pm1.g1.e1, convert(5, string)) = 2+3", "context(pm1.g1.e1, '5') = '5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // HAS Criteria
    public void testRewriteProcedure1() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (HAS = CRITERIA ON (vm1.g1.e1))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);
				
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    // HAS Criteria
    public void testRewriteProcedure2() {
    	
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (HAS = CRITERIA ON (vm1.g1.e1))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n";		 //$NON-NLS-1$
		procedure = procedure + "ELSE \n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n";		 //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT vm1.g1.e1 FROM vm1.g1 WHERE "+FALSE_STR+";\n"; //$NON-NLS-1$ //$NON-NLS-2$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);
				
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    // HAS Criteria
    public void testRewriteProcedure3() {
    	
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (HAS = CRITERIA ON (vm1.g1.e1))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n";		 //$NON-NLS-1$
		procedure = procedure + "ELSE \n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n";		 //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT vm1.g1.e1 FROM vm1.g1 WHERE "+FALSE_STR+";\n"; //$NON-NLS-1$ //$NON-NLS-2$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);
				
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    public void testRewriteProcedure4() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (INPUT.e2 = 1)\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT vm1.g1.e1 FROM vm1.g1 WHERE "+FALSE_STR+";\n"; //$NON-NLS-1$ //$NON-NLS-2$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);
				
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }    
    
    // CHANGING
    public void testRewriteProcedure5() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (CHANGING.e1 = \"false\")\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e2);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Update vm1.g1 SET e1 = \"String\", e2 =1 where e2 = 10"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    // CHANGING
    public void testRewriteProcedure6() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (CHANGING.e1 = \"true\")\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Update vm1.g1 SET e1 = \"String\", e2 =1 where e2 = 10"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }     
    
    // TRANSLATE CRITERIA
    public void testRewriteProcedure7() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (CHANGING.e1 = \"true\")\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select e2 from pm1.g1 where TRANSLATE = CRITERIA ON (vm1.g1.e2);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Update vm1.g1 SET e1 = \"String\", e2 =1 where e2 = 10"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT e2 FROM pm1.g1 WHERE pm1.g1.e2 = 10;\n";				 //$NON-NLS-1$
		rewritProc = rewritProc + "END";		 //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    // TRANSLATE CRITERIA
    public void testRewriteProcedure8() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (CHANGING.e1 = \"true\")\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select e2 from pm1.g1 where TRANSLATE = CRITERIA ON (vm1.g1.e2) with (vm1.g1.e2 = convert(sqrt(pm1.g1.e2), integer));\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "Update vm1.g1 SET e1 = \"String\", e2 =1 where e2 = 10"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT e2 FROM pm1.g1 WHERE sqrt(pm1.g1.e2) = 10.0;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    // rewrite input/ changing variables
    public void testRewriteProcedure9() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare String var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 = 'x' or var1 = 'y')\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2, CHANGING.e2, CHANGING.e1 from pm1.g1 order by CHANGING.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (e1) values('x')"; //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE String var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "IF((var1 = 'x') OR (var1 = 'y'))\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2, null AS E2_0, FALSE AS E2_1, TRUE AS E1 FROM pm1.g1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
    public void testRewriteProcedure10() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1, pm1.g2 where TRANSLATE = CRITERIA ON (e2) WITH (e2 = pm1.g1.e2 + 20);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g1 SET e1='x' where e2 = e2 + 50"; //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1, pm1.g2 WHERE (pm1.g1.e2 + 20) = ((pm1.g1.e2 + 20) + 50);\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
	// virtual group elements used in procedure in if statement(HAS CRITERIA)
    public void testRewriteProcedure11() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = INPUT.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g1 SET e1=40";     //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE string var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "var1 = '40';\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
	// with complex query transform
    public void testRewriteProcedure12() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE = CRITERIA WITH (x = CONCAT(e1 , 'z'));\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where x =CONCAT(x , 'y')"; //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1 WHERE CONCAT(e1, 'z') = CONCAT(CONCAT(e1, 'z'), 'y');\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
	// with complex query transform
    public void testRewriteProcedure13() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE = CRITERIA WITH (x = CONCAT(e1 , 'z'), y = convert(CONCAT(e1 , 'k'), integer));\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where x =CONCAT(x , 'y') and y= 1"; //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1 WHERE (CONCAT(e1, 'z') = CONCAT(CONCAT(e1, 'z'), 'y')) AND (CONCAT(e1, 'k') = '1');\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
    public void testRewriteProcedure14() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE = CRITERIA WITH (e4 = sqrt(e4));\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e4= 1"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1 WHERE sqrt(e4) = 1.0;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
    public void testRewriteProcedure15() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE = CRITERIA WITH (e4 = e4/50);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y= 1"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1 WHERE e2 = 0;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
    public void testRewriteProcedure16() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e4= 1"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1 WHERE e4 = 0.02;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
    public void testRewriteProcedure17() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE LIKE CRITERIA WITH (e4 = e4/50);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e4= 1"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1 WHERE "+FALSE_STR+";\n"; //$NON-NLS-1$ //$NON-NLS-2$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// Bug 8212 elements in INPUT and CHANGING special groups are cese sensitive
    public void testRewriteProcedure18() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select Input.E1, Input.e2, CHANGING.e2, CHANGING.E1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (e1, E2) values('x', 1)"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT 'x', 1, TRUE, TRUE FROM pm1.g1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// elements being set in updates are dropped if INPUT var is not available, unless a default is available
    // Note that this test is a little odd in that it is an update inside of an insert
    public void testRewriteProcedure19() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "update pm1.g1 set e1=Input.E1, e2=Input.e2, e3=Input.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (E2) values(1)"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "UPDATE pm1.g1 SET e1 = 'xyz', e2 = 1, e3 = TRUE;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// elements being set in updates are dropped if INPUT var is not available, unless a default is supplied
    
    //this test fails because the default for E1 'xyz' cannot be converted into a integer
    public void testRewriteProcedure21() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "update pm1.g1 set e1=convert(Input.E1, integer)+INPUT.E2, e2=Input.e2, e3=Input.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (E3) values({b'true'})"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "UPDATE pm1.g1 SET e3 = TRUE;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		this.helpFailUpdateProcedure(procedure, userQuery, FakeMetadataObject.Props.INSERT_PROCEDURE);
	}
    
    public void testRewriteProcedure21a() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "update pm1.g1 set e1=convert(Input.E1, integer)+INPUT.E2, e2=Input.e2, e3=Input.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (E1) values(1)"; //$NON-NLS-1$

        String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "UPDATE pm1.g1 SET e1 = null, e2 = null, e3 = TRUE;\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$

        String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
                                                        FakeMetadataObject.Props.INSERT_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }

	
	// none of input variables on update statement changing
    public void testRewriteProcedure22() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "update pm1.g1 set e1=convert(Input.E1, integer)+INPUT.E2, e2=Input.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "update vm1.g1 set E3 = {b'true'}"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// none of input variables on update statement changing
    public void testRewriteProcedure23() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "update pm1.g1 set e2=Input.e2, e3=Input.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "update vm1.g1 set E1 = 'x'"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
    
    //with an insert, defaults are used
    public void testRewriteProcedure23a() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "update pm1.g1 set e2=Input.e2, e3=Input.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (E1) values('x')"; //$NON-NLS-1$

        String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "UPDATE pm1.g1 SET e2 = null, e3 = TRUE;\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$

        String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.INSERT_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
	// elements being set in updates are dropped if INPUT var is not available
    public void testRewriteProcedure24() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET e2=Input.e2 WHERE TRANSLATE LIKE CRITERIA ON (e1) WITH (e1=concat(pm1.g1.e1, \"%\"));\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g1 set E2=1 where e2 = 1 and e1 LIKE 'mnopxyz_'"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "UPDATE pm1.g1 SET e2 = 1 WHERE concat(pm1.g1.e1, '%') LIKE 'mnopxyz_';\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}

	// INPUT vars in insert statements replaced by default variable when user's inser ignores values
    public void testRewriteProcedure25() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "INSERT into pm1.g1 (e1,e2,e3,e4) values (Input.e1, Input.e2, Input.e3, Input.e4);"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "INSERT into vm1.g1 (E2) values (1)"; //$NON-NLS-1$

		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES ('xyz', 1, TRUE, 123.456);\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.INSERT_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
	public void testRewriteProcedure26() {
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e2 from pm1.g1, pm1.g2 where TRANSLATE = CRITERIA ON (e2);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n";         //$NON-NLS-1$

		String userQuery = "UPDATE vm1.g1 SET e1='x' where e2 = e2 + 50"; //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e2 = (pm1.g1.e2 + 50);\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

		assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}
	
	// virtual group elements used in procedure in if statement(TRANSLATE CRITERIA)
	public void testRewriteProcedure27() {
		String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e2 from pm1.g1, pm1.g2 where TRANSLATE = CRITERIA ON (e2);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n";         //$NON-NLS-1$

		String userQuery = "UPDATE vm1.g1 SET e1='x' where e2 LIKE 'xyz'"; //$NON-NLS-1$
        
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT pm1.g1.e2 FROM pm1.g1, pm1.g2 WHERE "+FALSE_STR+";\n"; //$NON-NLS-1$ //$NON-NLS-2$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$

		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.UPDATE_PROCEDURE);

		assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
	}

    /**
     * Per defect 9380 - 
     * A criteria of the form  
     * (? + 1) < (null)  
     * caused a problem in the QueryRewriter.simplifyMathematicalCriteria method.
     * At the beginning of the method, the null constant is rewritten so that it
     * loses it's implicit type conversion to integer, then later on a function
     * descriptor couldn't be found for the "minus" operation for the two types 
     * integer and MetaMatrix's null type.
     */
    public void testRewriteProcedure_9380() {
        
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
        procedure = procedure + "if((var1 + 1) < length(input.e1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "var2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g1 SET e2=30";     //$NON-NLS-1$
        
        String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "DECLARE integer var1;\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "DECLARE integer var2;\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$

        String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE);

        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    //base test.  no change is expected
    public void testRewriteLookupFunction1() {
        String criteria = "lookup('pm1.g1','e1', 'e2', 1) = 'ab'"; //$NON-NLS-1$
        CompareCriteria expected = (CompareCriteria)parseCriteria(criteria, FakeMetadataFactory.example1Cached()); 
        helpTestRewriteCriteria(criteria, expected, FakeMetadataFactory.example1Cached());
    }
    
    public void testRewriteLookupFunction1b() {
        helpTestRewriteCriteria("lookup('pm1.g1','e1', 'e2', pm1.g1.e2) = 'ab'", "lookup('pm1.g1','e1', 'e2', pm1.g1.e2) = 'ab'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** defect 11630 1 should still get rewritten as '1'*/
    public void testRewriteLookupFunctionCompoundCriteria() {
        String criteria = "LOOKUP('pm1.g1','e1', 'e2', 1) IS NULL AND pm1.g1.e1='1'"; //$NON-NLS-1$
        CompoundCriteria expected = (CompoundCriteria)parseCriteria(criteria, FakeMetadataFactory.example1Cached()); 
        helpTestRewriteCriteria("LOOKUP('pm1.g1','e1', 'e2', 1) IS NULL AND pm1.g1.e1=1", expected, FakeMetadataFactory.example1Cached()); //$NON-NLS-1$ 
    }

    public void testSelectWithNoFrom() {
        helpTestRewriteCommand("SELECT 5", "SELECT 5"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    //defect 9822
    public void testStoredProcedure_9822() throws Exception {

        QueryParser parser = new QueryParser();
        Command command = parser.parseCommand("exec pm1.sp4(5)");             //$NON-NLS-1$
        
        // resolve
        QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());
        
        // rewrite
        Command rewriteCommand = QueryRewriter.rewrite(command, null, null, null);
        
        List parameters = ((StoredProcedure)rewriteCommand).getParameters();
        
        Iterator iter = parameters.iterator();
        while(iter.hasNext()){
            SPParameter param = (SPParameter)iter.next();
            if(param.getParameterType() == ParameterInfo.IN || param.getParameterType() == ParameterInfo.INOUT){
                assertTrue(param.getExpression() instanceof Constant);
            }
        }  
    }
    
    public void testRewriteRecursive() {
        Command c = helpTestRewriteCommand("SELECT e2 FROM vm1.g33", "SELECT e2 FROM vm1.g33"); //$NON-NLS-1$ //$NON-NLS-2$
        Command innerCommand = (Command) c.getSubCommands().get(0);
        
        assertEquals("Inner command not rewritten", "SELECT e2 FROM pm1.g1 WHERE e2 = 2", innerCommand.toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
    }
    
    public void testRewriteFunctionThrowsEvaluationError() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria("5 / 0 = 5", metadata); //$NON-NLS-1$
        
        // rewrite
        try { 
            QueryRewriter.rewriteCriteria(origCrit, null, null, null);
            fail("Expected QueryValidatorException due to divide by 0"); //$NON-NLS-1$
        } catch(QueryValidatorException e) {
        	// looks like message is being wrapped with another exception with same message
            assertEquals("Error Code:ERR.015.001.0003 Message:Error Code:ERR.015.001.0003 Message:Unable to evaluate (5 / 0): Error Code:ERR.015.001.0003 Message:Error while evaluating function /", e.getMessage());  //$NON-NLS-1$
        }       
    }
    
    public void testRewriteConvertThrowsEvaluationError() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 
        Criteria origCrit = parseCriteria("convert('x', integer) = 0", metadata); //$NON-NLS-1$
        
        // rewrite
        try { 
            QueryRewriter.rewriteCriteria(origCrit, null, null, null);
            fail("Expected QueryValidatorException due to invalid string"); //$NON-NLS-1$
        } catch(QueryValidatorException e) {
            assertEquals("Error Code:ERR.015.009.0004 Message:Unable to convert 'x' of type [string] to the expected type [integer].", e.getMessage()); //$NON-NLS-1$
        }       
    }
    
    public void testDefect13458() {
    	
		String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";		 //$NON-NLS-1$
		procedure = procedure + "IF (HAS = CRITERIA ON (vm1.g1.e1))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select vm1.g1.e1 from vm1.g1 where HAS = CRITERIA ON (vm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		
		String userQuery = "delete from vm1.g1 where e1='1'"; //$NON-NLS-1$
		
		String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "BEGIN\n";		 //$NON-NLS-1$
		rewritProc = rewritProc + "SELECT vm1.g1.e1 FROM vm1.g1;\n"; //$NON-NLS-1$
		rewritProc = rewritProc + "END"; //$NON-NLS-1$
		
		String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
				FakeMetadataObject.Props.DELETE_PROCEDURE);				
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    public void testRewriteCase1954() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) = '3'", "pm1.g1.e2 = 3"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCase1954a() {
        helpTestRewriteCriteria("cast(pm1.g1.e2 as string) = '3'", "pm1.g1.e2 = 3"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCase1954b() throws Exception{
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached(); 

        // Have to hand-build the criteria, because 3.0 gets parsed as a Float by default
        // pm1.g1.e4 = 3.0
        CompareCriteria expected = new CompareCriteria();
        ElementSymbol leftElement = new ElementSymbol("pm1.g1.e4"); //$NON-NLS-1$
        Constant constant = new Constant(new Double(3.0), DataTypeManager.DefaultDataClasses.DOUBLE);
        expected.setLeftExpression(leftElement);
        expected.setRightExpression(constant);
        // resolve against metadata
        QueryResolver.resolveCriteria(expected, metadata);
        
        helpTestRewriteCriteria("convert(pm1.g1.e4, string) = '3'", expected, metadata); //$NON-NLS-1$ 
    }    

    public void testRewriteCase1954c() {
        helpTestRewriteCriteria("convert(pm1.g1.e1, string) = 'x'", "pm1.g1.e1 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    public void testRewriteCase1954d() {
        helpTestRewriteCriteria("convert(pm1.g1.e1, timestamp) = {ts '2005-01-03 00:00:00.0'}", "pm1.g1.e1 = '2005-01-03 00:00:00.0'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCase1954e() {
        helpTestRewriteCriteria("convert(pm1.g1.e4, integer) = 2", "pm1.g1.e4 = 2.0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** Check that this fails, x is not convertable to an int */
    public void testRewriteCase1954f() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) = 'x'", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /** Check that this returns true, x is not convertable to an int */
    public void testRewriteCase1954f1() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) != 'x'", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCase1954Set() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) in ('2', '3')", "pm1.g1.e2 IN (2,3)"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    public void testRewriteCase1954SetA() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) in ('2', 'x')", "pm1.g1.e2 = 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    public void testRewriteCase1954SetB() {
        helpTestRewriteCriteria("cast(pm1.g1.e2 as string) in ('2', '3')", "pm1.g1.e2 IN (2,3)"); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    public void testRewriteCase1954SetC() {
        helpTestRewriteCriteria("concat(pm1.g1.e2, 'string') in ('2', '3')", "concat(pm1.g1.e2, 'string') in ('2', '3')"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    public void testRewriteCase1954SetD() {
        helpTestRewriteCriteria("convert(pm1.g1.e2, string) in ('2', pm1.g1.e1)", "convert(pm1.g1.e2, string) in ('2', pm1.g1.e1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }      
    
    // First WHEN always true, so rewrite as THEN expression
    public void testRewriteCaseExpr1() {
        helpTestRewriteCriteria("case when 0=0 then 1 else 2 end = 1", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN always false, so rewrite as ELSE expression
    public void testRewriteCaseExpr2() {
        helpTestRewriteCriteria("case when 0=1 then 1 else 2 end = 1", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN can't be rewritten, so no changes
    public void testRewriteCaseExpr3() {
        helpTestRewriteCriteria("case when 0 = pm1.g1.e2 then 1 else 2 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 1 ELSE 2 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCaseExpr4() {
        helpTestRewriteCriteria("lookup('pm1.g1', 'e2', 'e1', case when 1=1 then pm1.g1.e1 end) = 0", "lookup('pm1.g1', 'e2', 'e1', pm1.g1.e1) = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN always false, so remove it
    public void testRewriteCaseExpr5() {
        helpTestRewriteCriteria("case when 0=1 then 1 when 0 = pm1.g1.e2 then 2 else 3 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 2 ELSE 3 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCaseExprForCase5413aFrom502() {
        helpTestRewriteCriteria("pm1.g2.e1 = case when 0 = pm1.g1.e2 then 2 else 2 end", "pm1.g2.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCaseExprForCase5413bFrom502() {
        helpTestRewriteCriteria("case when 0 = pm1.g1.e2 then null else null end IS NULL", TRUE_STR); //$NON-NLS-1$ 
    }
    
    
    public void testRewriteCaseExprForCase5413a() {
        helpTestRewriteCriteria("pm1.g2.e1 = case when 0 = pm1.g1.e2 then 2 else 2 end", "pm1.g2.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteCaseExprForCase5413b() {
        helpTestRewriteCriteria("case when 0 = pm1.g1.e2 then null else null end IS NULL", TRUE_STR); //$NON-NLS-1$ 
    }

    // First WHEN always true, so rewrite as THEN expression
    public void testRewriteSearchedCaseExpr1() {
        helpTestRewriteCriteria("case 0 when 0 then 1 else 2 end = 1", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // First WHEN always false, so rewrite as ELSE expression
    public void testRewriteSearchedCaseExpr2() {
        helpTestRewriteCriteria("case 0 when 1 then 1 else 2 end = 1", "1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteSearchedCaseExpr3() {
        helpTestRewriteCriteria("case 0 when pm1.g1.e2 then 1 else 2 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 1 ELSE 2 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteSearchedCaseExpr4() {
        String criteria = "lookup('pm1.g1', 'e2', 'e1', '2') = 0"; //$NON-NLS-1$
        CompareCriteria expected = (CompareCriteria)parseCriteria(criteria, FakeMetadataFactory.example1Cached()); 
        helpTestRewriteCriteria("lookup('pm1.g1', 'e2', 'e1', case 0 when 1 then pm1.g1.e1 else 2 end) = 0", expected, FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    // First WHEN always false, so remove it
    public void testRewriteSearchedCaseExpr5() {
        helpTestRewriteCriteria("case 0 when 1 then 1 when pm1.g1.e2 then 2 else 3 end = 1", "CASE WHEN pm1.g1.e2 = 0 THEN 2 ELSE 3 END = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDefect16879_1(){
    	helpTestRewriteCommand("SELECT decodestring(e1, 'a, b') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDefect16879_2(){
    	helpTestRewriteCommand("SELECT decodestring(e1, 'a, b, c, d') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' WHEN e1 = 'c' THEN 'd' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDefect16879_3(){
    	helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDefect16879_4(){
    	helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b, c, d') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' WHEN e1 = 'c' THEN 'd' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDefect16879_5(){
        helpTestRewriteCommand("SELECT decodeinteger(e1, 'null, b, c, d') FROM pm1.g1", "SELECT CASE WHEN e1 IS NULL THEN 'b' WHEN e1 = 'c' THEN 'd' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDefect16879_6(){
        helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b, null, d') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' WHEN e1 IS NULL THEN 'd' ELSE e1 END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testDefect16879_7(){
        helpTestRewriteCommand("SELECT decodeinteger(e1, 'a, b, null, d, e') FROM pm1.g1", "SELECT CASE WHEN e1 = 'a' THEN 'b' WHEN e1 IS NULL THEN 'd' ELSE 'e' END FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testCaseExpressionThatResolvesToNull() {
        String sqlBefore = "SELECT CASE 'x' WHEN 'Old Inventory System' THEN NULL WHEN 'New Inventory System' THEN NULL END"; //$NON-NLS-1$
        String sqlAfter = "SELECT null"; //$NON-NLS-1$

        Command cmd = helpTestRewriteCommand( sqlBefore, sqlAfter );
        
        ExpressionSymbol es = (ExpressionSymbol)cmd.getProjectedSymbols().get(0);
        assertEquals( DataTypeManager.DefaultDataClasses.STRING, es.getType() );
    }


    //note that the env is now treated as deterministic, however it is really only deterministic within a session
    public void testRewriteExecEnv() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("exec pm1.sq2(env('sessionid'))");             //$NON-NLS-1$
        
        QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());
        
        CommandContext context = new CommandContext();
        Properties props = new Properties();
        props.setProperty(ContextProperties.SESSION_ID, "1"); //$NON-NLS-1$
        context.setEnvironmentProperties(props);
        Command rewriteCommand = QueryRewriter.rewrite(command, null, null, context);
        
        assertEquals("SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'", rewriteCommand.toString()); //$NON-NLS-1$
    }

    public void testRewriteExecCase6455() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("exec pm1.sq2(env('sessionid')) OPTION PLANONLY DEBUG");             //$NON-NLS-1$
        
        QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());
        
        CommandContext context = new CommandContext();
        Properties props = new Properties();
        props.setProperty(ContextProperties.SESSION_ID, "1"); //$NON-NLS-1$
        context.setEnvironmentProperties(props);
        Command rewriteCommand = QueryRewriter.rewrite(command, null, null, context);
        
        assertEquals("SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1' OPTION PLANONLY DEBUG", rewriteCommand.toString()); //$NON-NLS-1$
    }


    public void testRewriteNestedFunctions() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 where convert(parsedate(e1, 'yyyy-MM-dd'), string) = '2006-07-01'", "SELECT e1 FROM pm1.g1 WHERE e1 = '2006-07-01'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteWithReference() {
        helpTestRewriteCommand("SELECT e1 FROM pm1.g1 where parsetimestamp(e1, 'yyyy-MM-dd') != ?", "SELECT e1 FROM pm1.g1 WHERE e1 <> formattimestamp(?, 'yyyy-MM-dd')"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewiteJoinCriteria() {
        helpTestRewriteCommand("SELECT pm1.g1.e1 FROM pm1.g1 inner join pm1.g2 on (pm1.g1.e1 = null)", "SELECT pm1.g1.e1 FROM pm1.g1 INNER JOIN pm1.g2 ON 1 = 0"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewiteCompoundCriteria() {
        helpTestRewriteCriteria("(pm1.g1.e1 = 1 and pm1.g1.e2 = 2) and (pm1.g1.e3 = 1 and pm1.g1.e4 = 2)", "(pm1.g1.e1 = '1') AND (pm1.g1.e2 = 2) AND (pm1.g1.e3 = TRUE) AND (pm1.g1.e4 = 2.0)"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteWhile() throws Exception {
        
        String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "while (1 = 1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select vm1.g1.e1 from vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.INSERT_PROCEDURE, procedure);
        
        QueryParser parser = new QueryParser();
        Command userCommand = parser.parseCommand(userQuery);
        QueryResolver.resolveCommand(userCommand, metadata);
        
        try {       
            QueryRewriter.rewrite(userCommand, null, metadata, null);
            fail("exception expected"); //$NON-NLS-1$
        } catch (QueryValidatorException e) {
            assertEquals("Infinite loop detected, procedure will not be executed.", e.getMessage()); //$NON-NLS-1$
        }
                
    }
    
    public void testRewriteWhile1() {
        
        String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";       //$NON-NLS-1$
        procedure = procedure + "while (1 = 0)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select vm1.g1.e1 from vm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$
        
        String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$
        
        String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.INSERT_PROCEDURE);
                
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    /**
     * Tests that VariableSubstitutionVisitor does not cause an NPE on count(*)
     */
    public void testRewriteProcedureWithCount() {
        
        String procedure = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select count(*) from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userQuery = "Insert into vm1.g1 (e1, e2) values (\"String\", 1)"; //$NON-NLS-1$
        
        String rewritProc = "CREATE PROCEDURE\n"; //$NON-NLS-1$
        rewritProc = rewritProc + "BEGIN\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "SELECT COUNT(*) FROM pm1.g1;\n";         //$NON-NLS-1$
        rewritProc = rewritProc + "END"; //$NON-NLS-1$
        
        String procReturned = this.getReWrittenProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.INSERT_PROCEDURE);
                
        assertEquals("Rewritten command was not expected", rewritProc, procReturned); //$NON-NLS-1$
    }
    
    /**
     * Test to ensure the update changing list retains e1 = ?
     */
    public void testVariableSubstitutionVisitor() throws Exception {
        String procedure1 = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure1 += "BEGIN\n"; //$NON-NLS-1$
        procedure1 += "DECLARE string var1 = INPUT.e1;\n"; //$NON-NLS-1$
        procedure1 += "ROWS_UPDATED = UPDATE vm1.g2 SET e1=var1;\n"; //$NON-NLS-1$
        procedure1 += "END"; //$NON-NLS-1$
        
        String procedure2 = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure2 += "BEGIN\n"; //$NON-NLS-1$
        procedure2 += "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure2 += "IF (INPUT.e1 = 1)\n"; //$NON-NLS-1$
        procedure2 += "ROWS_UPDATED = 5;\n"; //$NON-NLS-1$
        procedure2 += "ELSE\n"; //$NON-NLS-1$
        procedure2 += "ROWS_UPDATED = 5;\n"; //$NON-NLS-1$
        procedure2 += "END"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1 = 'x' WHERE e2 = 5"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure1, procedure2);
        
        Update command = (Update)helpTestRewriteCommand(userUpdateStr, userUpdateStr, metadata);
                     
        String expected = "CREATE PROCEDURE\nBEGIN\nDECLARE string var1 = 'x';\nROWS_UPDATED = UPDATE vm1.g2 SET e1 = var1;\nEND"; //$NON-NLS-1$
        
        assertEquals(expected, command.getSubCommand().toString());
    }
    
    public void testRemoveEmptyLoop() {
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
    
    public void testRewriteDeclare() {
        String procedure1 = "CREATE virtual PROCEDURE  "; //$NON-NLS-1$
        procedure1 += "BEGIN\n"; //$NON-NLS-1$
        procedure1 += "declare integer x = 1 + 1;\n"; //$NON-NLS-1$
        procedure1 += "END"; //$NON-NLS-1$
        
        String expected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nDECLARE integer x = 2;\nEND"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure1, expected);
    }
      
    public void testRewriteUnionJoin() {
        String sql = "select pm1.g1.e1 from pm1.g1 union join pm1.g2 where g1.e1 = 1"; //$NON-NLS-1$
        String expected = "SELECT pm1.g1.e1 FROM pm1.g1 FULL OUTER JOIN pm1.g2 ON 1 = 0 WHERE g1.e1 = '1'"; //$NON-NLS-1$
                
        helpTestRewriteCommand(sql, expected);        
    }

    public void testRewriteNonNullDependentFunction() {
        helpTestRewriteCriteria("pm1.g1.e1 = concat(null, pm1.g1.e2)", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteInWithNull() {
        helpTestRewriteCriteria("convert(null, string) in (pm1.g1.e1, pm1.g1.e2)", "null <> null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteXMLCriteriaCases5630And5640() {
        helpTestRewriteCommand("select * from xmltest.doc1 where node1 = null", "SELECT * FROM xmltest.doc1 WHERE node1 = null"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCorrelatedSubqueryInHaving() {
        String sql = "select pm1.g1.e1 from pm1.g1 group by pm1.g1.e1 having pm1.g1.e1 in (select pm1.g1.e1 from pm1.g2)"; //$NON-NLS-1$
        String expected = "SELECT pm1.g1.e1 FROM pm1.g1 GROUP BY pm1.g1.e1 HAVING pm1.g1.e1 IN (SELECT pm1.g1.e1 FROM pm1.g2)"; //$NON-NLS-1$
                
        Query query = (Query)helpTestRewriteCommand(sql, expected);
        
        List<Reference> refs = new LinkedList<Reference>();
        
        CorrelatedReferenceCollectorVisitor.collectReferences(query, Arrays.asList(new GroupSymbol("pm1.g1")), refs);//$NON-NLS-1$
        
        assertEquals(1, refs.size());
    }
    
    public void testRewriteSelectInto() {
        String sql = "select distinct pm1.g1.e1 into #temp from pm1.g1"; //$NON-NLS-1$
        String expected = "SELECT DISTINCT pm1.g1.e1 INTO #temp FROM pm1.g1"; //$NON-NLS-1$
                
        helpTestRewriteCommand(sql, expected);        
    }
    
    /**
     * Accounts for type change with duplicate names
     */
    public void testRewriteSelectInto1() {
        String sql = "select distinct e2, e2, e3, e4 into pm1.g1 from pm1.g2"; //$NON-NLS-1$
        String expected = "SELECT PM1_G1_1.E2 AS E2, PM1_G1_1.E2_0, PM1_G1_1.E3, PM1_G1_1.E4 INTO pm1.g1 FROM (SELECT DISTINCT e2, e2 AS E2_0, e3, e4 FROM pm1.g2) AS pm1_g1_1"; //$NON-NLS-1$
                
        helpTestRewriteCommand(sql, expected);        
    }
    
    public void testUnionQueryNullInOneBranch() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT e1, null FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER });
    }
    
    public void testUnionQueryNullInOneBranch2() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT e1, e2 FROM pm1.g2 UNION ALL SELECT e1, null FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER });
    }

    public void testUnionQueryNullInOneBranch3() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, null FROM pm1.g1 UNION ALL SELECT e1, null FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER });
    }

    public void testUnionQueryNullInAllBranches() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1, null FROM pm1.g1 UNION ALL SELECT e1, null FROM pm1.g2 UNION ALL SELECT e1, null FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
    }
    
    public void testUnionQueryWithTypeConversion() throws Exception {
        verifyProjectedTypesOnUnionBranches("SELECT e1 FROM pm1.g1 UNION ALL SELECT e2 FROM pm1.g2", //$NON-NLS-1$
                                            new Class[] { DataTypeManager.DefaultDataClasses.STRING});
    }

    private void verifyProjectedTypesOnUnionBranches(String unionQuery, Class[] types) throws QueryValidatorException, QueryParserException, QueryResolverException, MetaMatrixComponentException {
        SetQuery union = (SetQuery)QueryParser.getQueryParser().parseCommand(unionQuery);
        QueryResolver.resolveCommand(union, FakeMetadataFactory.example1Cached());
        
        union = (SetQuery)QueryRewriter.rewrite(union, null, FakeMetadataFactory.example1Cached(), null);
        
        for (QueryCommand query : union.getQueryCommands()) {
            List projSymbols = query.getProjectedSymbols();
            for(int i=0; i<projSymbols.size(); i++) {
                assertEquals("Found type mismatch at column " + i, types[i], ((SingleElementSymbol) projSymbols.get(i)).getType()); //$NON-NLS-1$
            }                
        }
    }
    
    public void testRewiteOrderBy() {
        helpTestRewriteCommand("SELECT 1+1 as a FROM pm1.g1 order by a", "SELECT 2 AS a FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewiteOrderBy1() {
        helpTestRewriteCommand("SELECT 1+1 as a FROM pm1.g1 union select pm1.g2.e1 from pm1.g2 order by a", "SELECT '2' AS a FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY a"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * The rewrite creates inline view to do the type conversion.
     * 
     * It also ensures that all project symbols are uniquely named in the inline view
     */
    public void testSelectIntoWithOrderByAndTypeConversion() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE\n"; //$NON-NLS-1$
        procedure += "BEGIN\n";       //$NON-NLS-1$
        procedure += "CREATE local temporary table temp (x string, y integer, z integer);\n";       //$NON-NLS-1$
        procedure += "Select pm1.g1.e2, 1 as x, 2 as x into temp from pm1.g1 order by pm1.g1.e2 limit 1;\n"; //$NON-NLS-1$
        procedure += "Select x from temp;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$
        
        helpTestRewriteCommand(procedure, "CREATE VIRTUAL PROCEDURE\nBEGIN\nCREATE LOCAL TEMPORARY TABLE temp (x string, y integer, z integer);\nSELECT TEMP_1.E2 AS E2, TEMP_1.X, TEMP_1.X_0 INTO temp FROM (SELECT pm1.g1.e2, 1 AS x, 2 AS X_0 FROM pm1.g1 ORDER BY pm1.g1.e2 LIMIT 1) AS temp_1;\nSELECT x FROM temp;\nEND"); //$NON-NLS-1$
    }
    
    
    public void testInsertWithQuery() throws Exception {
        String sql = "insert into pm1.g1 select e1, e2, e3, e4 from pm1.g2 union select e1, e2, e3, e4 from pm1.g2"; //$NON-NLS-1$
        
        helpTestRewriteCommand(sql, "SELECT PM1_G1_1.E1, PM1_G1_1.E2, PM1_G1_1.E3, PM1_G1_1.E4 INTO pm1.g1 FROM (SELECT e1, e2, e3, e4 FROM pm1.g2 UNION SELECT e1, e2, e3, e4 FROM pm1.g2) AS pm1_g1_1"); //$NON-NLS-1$
    }
    
    public void testRewriteNot() {
        helpTestRewriteCriteria("not(not(pm1.g1.e1 = 1 + 1))", "pm1.g1.e1 = '2'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteQueryWithNoFrom() {
        String sql = "select 1 as a order by a"; //$NON-NLS-1$
        
        helpTestRewriteCommand(sql, "SELECT 1 AS a"); //$NON-NLS-1$
    }
    
    public void testOrderByDuplicateRemoval() {
        String sql = "SELECT pm1.g1.e1, pm1.g1.e1 as c1234567890123456789012345678901234567890 FROM pm1.g1 ORDER BY c1234567890123456789012345678901234567890, e1 "; //$NON-NLS-1$
        helpTestRewriteCommand(sql, "SELECT pm1.g1.e1, pm1.g1.e1 AS c1234567890123456789012345678901234567890 FROM pm1.g1 ORDER BY c1234567890123456789012345678901234567890"); //$NON-NLS-1$
    }
    
    /**
     * Case 4814
     */
    public void testVirtualRightOuterJoinSwap() throws Exception {
        String sql = "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM (select * from BQT1.smalla) sa RIGHT OUTER JOIN (select BQT1.mediumb.intkey from BQT1.mediumb) mb ON sa.IntKey = mb.IntKey"; //$NON-NLS-1$
        helpTestRewriteCommand(sql, "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM (SELECT BQT1.mediumb.intkey FROM BQT1.mediumb) AS mb LEFT OUTER JOIN (SELECT * FROM BQT1.smalla) AS sa ON sa.IntKey = mb.IntKey", FakeMetadataFactory.exampleBQTCached()); //$NON-NLS-1$
    }
    
    /**
     * Case 4814
     */
    public void testVirtualRightOuterJoinSwap1() throws Exception {
        String sql = "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM ((select * from BQT1.smalla) sa inner join BQT1.smallb on sa.intkey = smallb.intkey) RIGHT OUTER JOIN (select BQT1.mediumb.intkey from BQT1.mediumb) mb ON sa.IntKey = mb.IntKey"; //$NON-NLS-1$
        helpTestRewriteCommand(sql, "SELECT sa.IntKey AS sa_IntKey, mb.IntKey AS mb_IntKey FROM (SELECT BQT1.mediumb.intkey FROM BQT1.mediumb) AS mb LEFT OUTER JOIN ((SELECT * FROM BQT1.smalla) AS sa INNER JOIN BQT1.smallb ON sa.intkey = smallb.intkey) ON sa.IntKey = mb.IntKey", FakeMetadataFactory.exampleBQTCached()); //$NON-NLS-1$
    }
    
    public void testRewriteConcat2() {
        helpTestRewriteCriteria("concat2('a','b') = 'ab'", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteConcat2_1() {
        helpTestRewriteCriteria("concat2(null, null) is null", "1 = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteConcat2_2() throws Exception {
        helpTestRewriteCriteria("concat2(pm1.g1.e1, null) = 'xyz'", "CASE WHEN pm1.g1.e1 IS NULL THEN null ELSE concat(ifnull(pm1.g1.e1, ''), '') END = 'xyz'", true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteConcat2_3() throws Exception {
        helpTestRewriteCriteria("concat2(pm1.g1.e1, convert(pm1.g1.e2, string)) = 'xyz'", "CASE WHEN (pm1.g1.e1 IS NULL) AND (convert(pm1.g1.e2, string) IS NULL) THEN null ELSE concat(ifnull(pm1.g1.e1, ''), ifnull(convert(pm1.g1.e2, string), '')) END = 'xyz'", true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRewriteConcat2_4() throws Exception {
        helpTestRewriteCriteria("concat2('a', pm1.g1.e1) = 'xyz'", "concat('a', ifnull(pm1.g1.e1, '')) = 'xyz'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewiteEvaluatableAggregate() {
    	helpTestRewriteCommand("select pm1.g1.e1, max(1) from pm1.g1", "SELECT pm1.g1.e1, 1 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteFromUnixTime() throws Exception {
    	TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
    	try {
    		helpTestRewriteCriteria("from_unixtime(pm1.g1.e2) = '1992-12-01 07:00:00'", "timestampadd(SQL_TSI_SECOND, pm1.g1.e2, {ts'1969-12-31 18:00:00.0'}) = {ts'1992-12-01 07:00:00.0'}"); //$NON-NLS-1$ //$NON-NLS-2$
    	} finally {
    		TimestampWithTimezone.resetCalendar(null);
    	}
    }
    
    public void testRewriteNullIf() throws Exception {
    	helpTestRewriteCriteria("nullif(pm1.g1.e2, pm1.g1.e4) = 1", "CASE WHEN pm1.g1.e2 = pm1.g1.e4 THEN convert(null, double) ELSE pm1.g1.e2 END = 1.0", true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testRewriteCoalesce() throws Exception {
    	helpTestRewriteCriteria("coalesce(convert(pm1.g1.e2, double), pm1.g1.e4) = 1", "ifnull(convert(pm1.g1.e2, double), pm1.g1.e4) = 1", true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testProcWithNull() throws Exception {
        String sql = "exec pm1.vsp26(1, null)"; //$NON-NLS-1$
        
        try {
        	helpTestRewriteCommand(sql, "", FakeMetadataFactory.example1Cached());
        	fail("expected exception");
        } catch (QueryValidatorException e) {
        	
        }
        
    }

    /**
     * Test <code>QueryRewriter</code>'s ability to rewrite a query that 
     * contains an aggregate function which uses a <code>CASE</code> 
     * expression which contains <code>BETWEEN</code> criteria as its value.
     * <p>
     * An aggregate function list is defined and queries are created that 
     * use each function from the list.  The list includes:
     * <p>
     * "SUM", "MAX", "MIN", "AVG", "COUNT"   
     * <p>
     * It is expected that the BETWEEN expression will be rewritten as 
     * <code>CompoundCriteria</code>.
     * <p>
     * <table>
     * <tr><th align="left" colspan=2>For example:
     * <tr><td width="10*"><td>SELECT SUM(CASE WHEN e2 BETWEEN 3 AND 5 
     * THEN e2 ELSE -1 END) FROM pm1.g1
     * <tr><th align="left" colspan=2>Is rewritten as:
     * <tr><td width="10*"><td>SELECT SUM(CASE WHEN (e2 >= 3) AND (e2 <= 5) 
     * THEN e2 ELSE -1 END) FROM pm1.g1
     * </table>
     * 
     * @see com.metamatrix.query.rewriter.QueryRewriter
     * @see com.metamatrix.query.sql.lang.BetweenCriteria
     * @see com.metamatrix.query.sql.lang.CompoundCriteria
     * @see com.metamatrix.query.sql.symbol.AggregateSymbol
     * @see com.metamatrix.query.sql.symbol.SearchedCaseExpression
     */
    public void testAggregateWithBetweenInCaseInSelect() {
    	// Define a list of aggregates to test against
    	List<String> aggregateCommands = Arrays.asList( new String[] { "SUM", "MAX", "MIN", "AVG", "COUNT" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    	
    	// Define a query and the expected rewritten query
    	// ?AGGREGATE? represents the string substitution for an aggregate from aggregateCommands
    	String sqlBefore = "SELECT ?AGGREGATE?(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1"; //$NON-NLS-1$
    	String sqlAfter  = "SELECT ?AGGREGATE?(CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END) FROM pm1.g1"; //$NON-NLS-1$

    	// Iterate through the aggregateCommands
    	for ( String aCmd : aggregateCommands ) {
    		// Replace ?AGGREGATE? with the command from aggregateCommands
    		String sql = sqlBefore.replace("?AGGREGATE?", aCmd); //$NON-NLS-1$
    		String exSql = sqlAfter.replace("?AGGREGATE?", aCmd); //$NON-NLS-1$
    		// Test QueryRewriter
        	Command cmd = helpTestRewriteCommand( sql, exSql );
        	// Check the rewritten command to verify that CompundCriteria replaced BetweenCriteria
        	CompoundCriteria ccrit = (CompoundCriteria) ((SearchedCaseExpression) ((ExpressionSymbol) cmd.getProjectedSymbols().get(0)).getExpression()).getWhen().get(0);
        	assertEquals( "e2 >= 3", ccrit.getCriteria(0).toString() ); //$NON-NLS-1$
        	assertEquals( "e2 <= 5", ccrit.getCriteria(1).toString() ); //$NON-NLS-1$
    	}
    }
    
    /**
     * Test <code>QueryRewriter</code>'s ability to rewrite a query that 
     * contains a <code>CASE</code> expression which contains 
     * <code>BETWEEN</code> criteria in the queries <code>SELECT</code> clause.
     * <p>
     * It is expected that the BETWEEN expression will be rewritten as 
     * <code>CompoundCriteria</code>.
     * <p>
     * <table>
     * <tr><th align="left" colspan=2>For example:
     * <tr><td width="10*"><td>SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 
     * ELSE -1 END FROM pm1.g1 
     * <tr><th align="left" colspan=2>Is rewritten as:
     * <tr><td width="10*"><td>SELECT CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 
     * ELSE -1 END FROM pm1.g1
     * </table>
     * 
     * @see com.metamatrix.query.rewriter.QueryRewriter
     * @see com.metamatrix.query.sql.lang.BetweenCriteria
     * @see com.metamatrix.query.sql.lang.CompoundCriteria
     * @see com.metamatrix.query.sql.symbol.SearchedCaseExpression
     */
    public void testBetweenInCaseInSelect() {
    	String sqlBefore = "SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1"; //$NON-NLS-1$
    	String sqlAfter = "SELECT CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END FROM pm1.g1"; //$NON-NLS-1$
    	
    	Command cmd = helpTestRewriteCommand( sqlBefore, sqlAfter );
    	CompoundCriteria ccrit = (CompoundCriteria) ((SearchedCaseExpression) ((ExpressionSymbol) cmd.getProjectedSymbols().get(0)).getExpression()).getWhen().get(0);
    	assertEquals( "e2 >= 3", ccrit.getCriteria(0).toString() ); //$NON-NLS-1$
    	assertEquals( "e2 <= 5", ccrit.getCriteria(1).toString() ); //$NON-NLS-1$
    }
    
    /**
     * Test <code>QueryRewriter</code>'s ability to rewrite a query that 
     * contains a <code>CASE</code> expression which contains 
     * <code>BETWEEN</code> criteria in the queries <code>WHERE</code> clause.
     * <p>
     * It is expected that the BETWEEN expression will be rewritten as 
     * <code>CompoundCriteria</code>.
     * <p>
     * <table>
     * <tr><th align="left" colspan=2>For example:
     * <tr><td width="10*"><td>SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN e2 
     * BETWEEN 3 AND 5 THEN e2 ELSE -1 END 
     * <tr><th align="left" colspan=2>Is rewritten as:
     * <tr><td width="10*"><td>SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN  
     * (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END
     * </table>
     * 
     * @see com.metamatrix.query.rewriter.QueryRewriter
     * @see com.metamatrix.query.sql.lang.BetweenCriteria
     * @see com.metamatrix.query.sql.lang.CompoundCriteria
     * @see com.metamatrix.query.sql.symbol.SearchedCaseExpression
     */
    public void testBetweenInCase() {
    	String sqlBefore = "SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END"; //$NON-NLS-1$
    	String sqlAfter = "SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END"; //$NON-NLS-1$
    	
    	Command cmd = helpTestRewriteCommand( sqlBefore, sqlAfter );
    	CompoundCriteria ccrit = (CompoundCriteria) ((SearchedCaseExpression) ((CompareCriteria) ((Query) cmd).getCriteria()).getRightExpression()).getWhen().get(0);
    	assertEquals( "e2 >= 3", ccrit.getCriteria(0).toString() ); //$NON-NLS-1$
    	assertEquals( "e2 <= 5", ccrit.getCriteria(1).toString() ); //$NON-NLS-1$
    }

}
