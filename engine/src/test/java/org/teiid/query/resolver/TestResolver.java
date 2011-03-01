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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.SelectSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;
import org.teiid.query.unittest.FakeMetadataStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;

@SuppressWarnings("nls")
public class TestResolver {

	private FakeMetadataFacade metadata;

	@Before public void setUp() {
		metadata = FakeMetadataFactory.example1Cached();
	}

	// ################################## TEST HELPERS ################################

	static Command helpParse(String sql) { 
        try { 
            return QueryParser.getQueryParser().parseCommand(sql);
        } catch(TeiidException e) { 
            throw new RuntimeException(e);
        }
	}
    	
    /**
     * Helps resolve command, then check that the actual resolved Elements variables are the same as
     * the expected variable names.  The variableNames param will be empty unless the subquery
     * is a correlated subquery.
     * @param sql Command to parse and resolve
     * @param variableNames expected element symbol variable names, in order
     * @return parsed and resolved Query
     */
    private Command helpResolveSubquery(String sql, String[] variableNames){
        Query query = (Query)helpResolve(sql);
        Collection<ElementSymbol> variables = getVariables(query);

        assertTrue("Expected variables size " + variableNames.length + " but was " + variables.size(),  //$NON-NLS-1$ //$NON-NLS-2$
                   variables.size() == variableNames.length);
        Iterator variablesIter = variables.iterator();
        for (int i=0; variablesIter.hasNext(); i++) {
            ElementSymbol variable = (ElementSymbol)variablesIter.next();
            assertTrue("Expected variable name " + variableNames[i] + " but was " + variable.getName(),  //$NON-NLS-1$ //$NON-NLS-2$
                       variable.getName().equalsIgnoreCase(variableNames[i]));
        }

        if (variableNames.length == 0){
            //There should be no TempMetadataIDs
            Collection symbols = CheckNoTempMetadataIDsVisitor.checkSymbols(query);
            assertTrue("Expected no symbols with temp metadataIDs, but got " + symbols, symbols.isEmpty()); //$NON-NLS-1$
        }
        
        return query;         
    }

	public static Collection<ElementSymbol> getVariables(LanguageObject languageObject) {
		Collection<ElementSymbol> variables = ElementCollectorVisitor.getElements(languageObject, false, true);
    	for (Iterator<ElementSymbol> iterator = variables.iterator(); iterator.hasNext();) {
			ElementSymbol elementSymbol = iterator.next();
			if (!elementSymbol.isExternalReference()) {
				iterator.remove();
			}
		}
		return variables;
	}
    
    public static Command helpResolve(String sql, QueryMetadataInterface queryMetadata, AnalysisRecord analysis){
        return helpResolve(helpParse(sql), queryMetadata);
    }
    
	private Command helpResolve(String sql) { 
		return helpResolve(helpParse(sql));
	}
	
    private Command helpResolve(Command command) {    
        return helpResolve(command, this.metadata);  
    }	

	static Command helpResolve(Command command, QueryMetadataInterface queryMetadataInterface) {		
        // resolve
        try { 
            QueryResolver.resolveCommand(command, queryMetadataInterface);
        } catch(TeiidException e) {
            throw new RuntimeException(e);
        } 

        CheckSymbolsAreResolvedVisitor vis = new CheckSymbolsAreResolvedVisitor();
        DeepPreOrderNavigator.doVisit(command, vis);
        Collection unresolvedSymbols = vis.getUnresolvedSymbols();
        assertTrue("Found unresolved symbols: " + unresolvedSymbols, unresolvedSymbols.isEmpty()); //$NON-NLS-1$
        return command; 
	}		

	/**
	 * Expect a QueryResolverException (not any other kind of Throwable)
	 */
	private void helpResolveFails(Command command) {
		// resolve
		QueryResolverException exception = null;
		try {
			QueryResolver.resolveCommand(command, metadata);
		} catch(QueryResolverException e) {
			exception = e;
		} catch(TeiidException e) {
			fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		assertNotNull("Expected a QueryResolverException but got none.", exception); //$NON-NLS-1$
	}

    private Criteria helpResolveCriteria(String sql) { 
        Criteria criteria = null;
        
        // parse
        try { 
            criteria = QueryParser.getQueryParser().parseCriteria(sql);
           
        } catch(TeiidException e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
        }   
   
   		// resolve
        try { 
            QueryResolver.resolveCriteria(criteria, metadata);
        } catch(TeiidException e) {
            e.printStackTrace();
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 

        CheckSymbolsAreResolvedVisitor vis = new CheckSymbolsAreResolvedVisitor();
        DeepPreOrderNavigator.doVisit(criteria, vis);
        Collection unresolvedSymbols = vis.getUnresolvedSymbols();
        assertTrue("Found unresolved symbols: " + unresolvedSymbols, unresolvedSymbols.isEmpty()); //$NON-NLS-1$
        return criteria;
    }
    
    public static Command helpResolveWithBindings(String sql, QueryMetadataInterface metadata, List bindings) throws QueryResolverException, TeiidComponentException { 
       
        // parse
        Command command = helpParse(sql);
        
        QueryNode qn = new QueryNode("x", sql);
        qn.setBindings(bindings);
        // resolve
    	QueryResolver.resolveWithBindingMetadata(command, metadata, qn, true);

        CheckSymbolsAreResolvedVisitor vis = new CheckSymbolsAreResolvedVisitor();
        DeepPreOrderNavigator.doVisit(command, vis);

        Collection unresolvedSymbols = vis.getUnresolvedSymbols();
        assertTrue("Found unresolved symbols: " + unresolvedSymbols, unresolvedSymbols.isEmpty()); //$NON-NLS-1$
        return command;
    }

    static void helpResolveException(String sql, QueryMetadataInterface queryMetadata){
    	helpResolveException(sql, queryMetadata, null);	
    }
    
    static void helpResolveException(String sql, QueryMetadataInterface queryMetadata, String expectedExceptionMessage){

    	// parse
        Command command = helpParse(sql);
        
        // resolve
        try { 
            QueryResolver.resolveCommand(command, queryMetadata);
            fail("Expected exception for resolving " + sql);         //$NON-NLS-1$
        } catch(QueryResolverException e) {
        	if(expectedExceptionMessage != null){
            	assertEquals(expectedExceptionMessage, e.getMessage());
            }
        } catch(TeiidComponentException e) {
            throw new RuntimeException(e);
        }       
    }
    
	private void helpResolveException(String sql, String expectedExceptionMessage) {
		TestResolver.helpResolveException(sql, this.metadata, expectedExceptionMessage);
	}
	
	private void helpResolveException(String sql) {
        TestResolver.helpResolveException(sql, this.metadata);
	}
	
	private void helpCheckFrom(Query query, String[] groupIDs) { 
		From from = query.getFrom();
		List groups = from.getGroups();			
		assertEquals("Wrong number of group IDs: ", groupIDs.length, groups.size()); //$NON-NLS-1$
		
		for(int i=0; i<groups.size(); i++) { 
			GroupSymbol group = (GroupSymbol) groups.get(i);
            String matchString = null;
            if(group.getMetadataID() instanceof FakeMetadataObject) {
                matchString = ((FakeMetadataObject)group.getMetadataID()).getName();
            } else if(group.getMetadataID() instanceof TempMetadataID) {
                matchString = ((TempMetadataID)group.getMetadataID()).getID();
            }
			assertEquals("Group ID does not match: ", groupIDs[i].toUpperCase(), matchString.toUpperCase()); //$NON-NLS-1$
		}
	}
	
	private void helpCheckSelect(Query query, String[] elementNames) {
		Select select = query.getSelect();
		List elements = select.getSymbols();
		assertEquals("Wrong number of select symbols: ", elementNames.length, elements.size()); //$NON-NLS-1$

		for(int i=0; i<elements.size(); i++) {
			SelectSymbol symbol = (SelectSymbol) elements.get(i);
			assertEquals("Element name does not match: ", elementNames[i].toUpperCase(), symbol.getName().toUpperCase()); //$NON-NLS-1$
		}
	}

	private void helpCheckElements(LanguageObject langObj, String[] elementNames, String[] elementIDs) {
		List elements = new ArrayList();
		ElementCollectorVisitor.getElements(langObj, elements);
		assertEquals("Wrong number of elements: ", elementNames.length, elements.size()); //$NON-NLS-1$

		for(int i=0; i<elements.size(); i++) { 
			ElementSymbol symbol = (ElementSymbol) elements.get(i);
			assertEquals("Element name does not match: ", elementNames[i].toUpperCase(), symbol.getName().toUpperCase()); //$NON-NLS-1$
			
			FakeMetadataObject elementID = (FakeMetadataObject) symbol.getMetadataID();
			assertNotNull("ElementSymbol " + symbol + " was not resolved and has no metadataID", elementID); //$NON-NLS-1$ //$NON-NLS-2$
			assertEquals("ElementID name does not match: ", elementIDs[i].toUpperCase(), elementID.getName().toUpperCase()); //$NON-NLS-1$
		}
	}
    
    private void helpTestIsXMLQuery(String sql, boolean isXML) throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        // parse
        Query query = (Query) helpParse(sql);

        // check whether it's xml
        boolean actual = QueryResolver.isXMLQuery(query, metadata);
        assertEquals("Wrong answer for isXMLQuery", isXML, actual); //$NON-NLS-1$
    }
	
    /**
     * Helper method to resolve an exec aka stored procedure, then check that the
     * expected parameter expressions are the same as actual parameter expressions. 
     * @param sql
     * @param expectedParameterExpressions
     * @since 4.3
     */
    private StoredProcedure helpResolveExec(String sql, Object[] expectedParameterExpressions) {

        StoredProcedure proc = (StoredProcedure)helpResolve(sql);
        
        List params = proc.getParameters();

        // Remove all but IN and IN/OUT params
        Iterator paramIter = params.iterator();
        while (paramIter.hasNext()) {
            final SPParameter param = (SPParameter)paramIter.next();
            if (param.getParameterType() != ParameterInfo.IN && param.getParameterType() != ParameterInfo.INOUT) {
                paramIter.remove();
            }
        }

        // Check remaining params against expected expressions
        assertEquals(expectedParameterExpressions.length, params.size());
        for (int i=0; i<expectedParameterExpressions.length; i++) {
            SPParameter param = (SPParameter)params.get(i);
            if (expectedParameterExpressions[i] == null) {
                assertNull(param.getExpression());
            } else {
                assertEquals(expectedParameterExpressions[i], param.getExpression());
            }
        }
        
        return proc;
    }
        
    
	// ################################## ACTUAL TESTS ################################
	
	
	@Test public void testElementSymbolForms() {
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 AS a, e4 AS b FROM pm1.g1"; //$NON-NLS-1$
		Query resolvedQuery = (Query) helpResolve(sql);
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "pm1.g1.e2", "a", "b" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
	}

	@Test public void testElementSymbolFormsWithAliasedGroup() {
        String sql = "SELECT x.e1, e2, x.e3 AS a, e4 AS b FROM pm1.g1 AS x"; //$NON-NLS-1$
		Query resolvedQuery = (Query) helpResolve(sql);
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x.e1", "x.e2", "a", "b" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
	}

    @Test public void testGroupWithVDB() {
        String sql = "SELECT e1 FROM myvdb.pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
    }

    @Test public void testAliasedGroupWithVDB() {
        String sql = "SELECT e1 FROM myvdb.pm1.g1 AS x"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString());         //$NON-NLS-1$
    }
    
    @Test public void testPartiallyQualifiedGroup1() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }    
    
    @Test public void testPartiallyQualifiedGroup2() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.g2" }); //$NON-NLS-1$
    }
    
    @Test public void testPartiallyQualifiedGroup3() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }
    
    @Test public void testPartiallyQualifiedGroup4() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.cat2.g2" }); //$NON-NLS-1$
    }
    
    @Test public void testPartiallyQualifiedGroup5() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.g3"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat2.g3" }); //$NON-NLS-1$
    }    
    
    @Test public void testPartiallyQualifiedGroup6() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.cat1.g1" }); //$NON-NLS-1$
    }    
    
    @Test public void testPartiallyQualifiedGroup7() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM g4"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.g4" }); //$NON-NLS-1$
    }    
    
    @Test public void testPartiallyQualifiedGroup8() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM pm2.g3"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.g3" }); //$NON-NLS-1$
    }
    
    @Test public void testPartiallyQualifiedGroupWithAlias() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT X.e1 FROM cat2.cat3.g1 as X"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    } 
    
    @Test public void testPartiallyQualifiedElement1() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat2.cat3.g1.e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }

    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement2() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }
    
    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement3() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1 FROM cat2.cat3.g1, cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }
    
    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement4() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat1.g2.e1 FROM cat2.cat3.g1, cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.g2.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    @Test public void testPartiallyQualifiedElement5() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat1.g2.e1 FROM myvdb.pm1.cat1.cat2.cat3.g1, pm1.cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.g2.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement6() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, e2 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
	    helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.cat2.cat3.g1.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    @Test public void testPartiallyQualifiedElement7() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat2.cat3.g1.e2, g1.e3 FROM pm1.cat1.cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.cat2.cat3.g1.e2", "pm1.cat1.cat2.cat3.g1.e3" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    } 
    
    @Test public void testFailPartiallyQualifiedGroup1() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM cat3.g1"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedGroup2() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g1"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedGroup3() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g2"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedGroup4() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g3"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedGroup5() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g5");		 //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedElement1() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM pm1.cat1.cat2.cat3.g1, pm2.cat3.g1"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedElement2() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT g1.e1 FROM pm1.cat1.cat2.cat3.g1, pm2.cat3.g1"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedElement3() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM pm2.cat2.g2, pm1.cat2.g3"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedElement4() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM pm2.cat2.g2"); //$NON-NLS-1$
    }
    
    @Test public void testFailPartiallyQualifiedElement5() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM g1"); //$NON-NLS-1$
    }    

    @Test public void testElementWithVDB() {
        String sql = "SELECT myvdb.pm1.g1.e1 FROM pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
    }

    @Test public void testAliasedElementWithVDB() {
        Query resolvedQuery = (Query) helpResolve("SELECT myvdb.pm1.g1.e1 AS x FROM pm1.g1"); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
    }

	@Test public void testSelectStar() {
		Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	@Test public void testSelectStarFromAliasedGroup() {
		Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1 as x"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	@Test public void testSelectStarFromMultipleAliasedGroups() {
		Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1 as x, pm1.g1 as y"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1", "pm1.g1" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4", "y.e1", "y.e2", "y.e3", "y.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4", "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
	}

    @Test public void testSelectStarWhereSomeElementsAreNotSelectable() {
        Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g4"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g4" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g4.e1", "pm1.g4.e3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "pm1.g4.e1", "pm1.g4.e3" } ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectGroupStarWhereSomeElementsAreNotSelectable() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g4.* FROM pm1.g4"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g4" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g4.*" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g4.e1", "pm1.g4.e3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "pm1.g4.e1", "pm1.g4.e3" } ); //$NON-NLS-1$ //$NON-NLS-2$
    }

	@Test public void testFullyQualifiedSelectStar() {
		Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.* FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	@Test public void testSelectAllInAliasedGroup() {
		Query resolvedQuery = (Query) helpResolve("SELECT x.* FROM pm1.g1 as x"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x.*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	@Test public void testSelectExpressions() {
		Query resolvedQuery = (Query) helpResolve("SELECT e1, concat(e1, 's'), concat(e1, 's') as c FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "expr", "c" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "pm1.g1.e1", "pm1.g1.e1", "pm1.g1.e1" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new String[] { "pm1.g1.e1", "pm1.g1.e1", "pm1.g1.e1" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	@Test public void testSelectCountStar() {
		Query resolvedQuery = (Query) helpResolve("SELECT count(*) FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "count" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(), new String[] { }, new String[] { } );
	}
	
	@Test public void testMultipleIdenticalElements() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1, e1 FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e1" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testMultipleIdenticalElements2() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1, pm1.g1.e1 FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e1" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testMultipleIdenticalElements3() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1, e1 as x FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "x" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e1" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testDifferentElementsSameName() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1 as x, e2 as x FROM pm1.g2"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g2" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x", "x" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g2.e1", "pm1.g2.e2" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g2.e1", "pm1.g2.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testDifferentConstantsSameName() { 
		Query resolvedQuery = (Query) helpResolve("SELECT 1 as x, 2 as x FROM pm1.g2"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g2" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x", "x" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { }, 
			new String[] { });
	}
	
	@Test public void testFailSameGroupsWithSameNames() { 
		helpResolveException("SELECT * FROM pm1.g1 as x, pm1.g1 as x"); //$NON-NLS-1$
	}

	@Test public void testFailDifferentGroupsWithSameNames() { 
		helpResolveException("SELECT * FROM pm1.g1 as x, pm1.g2 as x"); //$NON-NLS-1$
	}

	@Test public void testFailAmbiguousElement() { 
		helpResolveException("SELECT e1 FROM pm1.g1, pm1.g2"); //$NON-NLS-1$
	}

	@Test public void testFailAmbiguousElementAliasedGroup() { 
		helpResolveException("SELECT e1 FROM pm1.g1 as x, pm1.g1"); //$NON-NLS-1$
	}

	@Test public void testFailFullyQualifiedElementUnknownGroup() { 
		helpResolveException("SELECT pm1.g1.e1 FROM pm1.g2"); //$NON-NLS-1$
	}

	@Test public void testFailUnknownGroup() { 
		helpResolveException("SELECT x.e1 FROM x"); //$NON-NLS-1$
	}

	@Test public void testFailUnknownElement() { 
		helpResolveException("SELECT x FROM pm1.g1"); //$NON-NLS-1$
	}

    @Test public void testFailFunctionOfAggregatesInSelect() {        
        helpResolveException("SELECT (SUM(e0) * COUNT(e0)) FROM test.group GROUP BY e0"); //$NON-NLS-1$
    }
	
	/*
	 * per defect 4404 
	 */
	@Test public void testFailGroupNotReferencedByAlias() { 
		helpResolveException("SELECT pm1.g1.x FROM pm1.g1 as H"); //$NON-NLS-1$
	}

	/*
	 * per defect 4404 - this one reproduced the defect,
	 * then succeeded after the fix
	 */
	@Test public void testFailGroupNotReferencedByAliasSelectAll() { 
		helpResolveException("SELECT pm1.g1.* FROM pm1.g1 as H"); //$NON-NLS-1$
	}

	@Test public void testComplicatedQuery() {
		Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e2 as y, pm1.g1.E3 as z, CONVERT(pm1.g1.e1, integer) * 1000 as w  FROM pm1.g1 WHERE e1 <> 'x'"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "y", "z", "w" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		helpCheckElements(resolvedQuery, 
			new String[] { "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e1", "pm1.g1.e1" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e1", "pm1.g1.e1" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	@Test public void testJoinQuery() {
		Query resolvedQuery = (Query) helpResolve("SELECT pm3.g1.e2, pm3.g2.e2 FROM pm3.g1, pm3.g2 WHERE pm3.g1.e2=pm3.g2.e2"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm3.g1", "pm3.g2" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckSelect(resolvedQuery, new String[] { "pm3.g1.e2", "pm3.g2.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery, 
			new String[] { "pm3.g1.e2", "pm3.g2.e2", "pm3.g1.e2", "pm3.g2.e2" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm3.g1.e2", "pm3.g2.e2", "pm3.g1.e2", "pm3.g2.e2" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
    @Test public void testHavingRequiringConvertOnAggregate1() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING MAX(e2) > 1.2"); //$NON-NLS-1$
    }

    @Test public void testHavingRequiringConvertOnAggregate2() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING MIN(e2) > 1.2"); //$NON-NLS-1$
    }

    @Test public void testHavingRequiringConvertOnAggregate3() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING 1.2 > MAX(e2)"); //$NON-NLS-1$
    }

    @Test public void testHavingRequiringConvertOnAggregate4() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING 1.2 > MIN(e2)"); //$NON-NLS-1$
    }

    @Test public void testHavingWithAggsOfDifferentTypes() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING MIN(e1) = MIN(e2)"); //$NON-NLS-1$
    }
    
    @Test public void testCaseInGroupBy() {
        String sql = "SELECT SUM(e2) FROM pm1.g1 GROUP BY CASE WHEN e2 = 0 THEN 1 ELSE 2 END"; //$NON-NLS-1$
        Command command = helpResolve(sql);
        assertEquals(sql, command.toString());
        
        helpCheckElements(command, new String[] {"pm1.g1.e2", "pm1.g1.e2"}, new String[] {"pm1.g1.e2", "pm1.g1.e2"});  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
    }

    @Test public void testFunctionInGroupBy() {
        String sql = "SELECT SUM(e2) FROM pm1.g1 GROUP BY (e2 + 1)"; //$NON-NLS-1$
        Command command = helpResolve(sql);
        assertEquals(sql, command.toString());
        
        helpCheckElements(command, new String[] {"pm1.g1.e2", "pm1.g1.e2"}, new String[] {"pm1.g1.e2", "pm1.g1.e2"});  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
    }

	@Test public void testUnknownFunction() {	    
		helpResolveException("SELECT abc(e1) FROM pm1.g1", "Error Code:ERR.015.008.0039 Message:The function 'abc(e1)' is an unknown form.  Check that the function name and number of arguments is correct."); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testConversionNotPossible() {	    
		helpResolveException("SELECT dayofmonth('2002-01-01') FROM pm1.g1", "Error Code:ERR.015.008.0040 Message:The function 'dayofmonth('2002-01-01')' is a valid function form, but the arguments do not match a known type signature and cannot be converted using implicit type conversions."); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
    @Test public void testResolveParameters() throws Exception {
        List bindings = new ArrayList();
        bindings.add("pm1.g2.e1"); //$NON-NLS-1$
        bindings.add("pm1.g2.e2"); //$NON-NLS-1$
        
        Query resolvedQuery = (Query) helpResolveWithBindings("SELECT pm1.g1.e1, ? FROM pm1.g1 WHERE pm1.g1.e1 = ?", metadata, bindings); //$NON-NLS-1$

        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "expr" }); //$NON-NLS-1$ //$NON-NLS-2$
        helpCheckElements(resolvedQuery.getCriteria(), 
            new String[] { "pm1.g1.e1", "pm1.g2.e2" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1", "pm1.g2.e2" } ); //$NON-NLS-1$
            
    }

    @Test public void testResolveParametersInsert() throws Exception {
    	List<String> bindings = Arrays.asList("pm1.g2.e1"); //$NON-NLS-1$
        
        helpResolveWithBindings("INSERT INTO pm1.g1 (e1) VALUES (?)", metadata, bindings); //$NON-NLS-1$
    }
    
    @Test public void testResolveParametersExec() throws Exception {
        List<String> bindings = Arrays.asList("pm1.g2.e1"); //$NON-NLS-1$
        
        Query resolvedQuery = (Query)helpResolveWithBindings("SELECT * FROM (exec pm1.sq2(?)) as a", metadata, bindings); //$NON-NLS-1$
        StoredProcedure sp = (StoredProcedure)((SubqueryFromClause)resolvedQuery.getFrom().getClauses().get(0)).getCommand();
        assertEquals(String.class, sp.getInputParameters().get(0).getExpression().getType());
    }
    
    @Test public void testResolveParametersExecNamed() throws Exception {
        List<String> bindings = Arrays.asList("pm1.g2.e1 as x"); //$NON-NLS-1$
        
        helpResolveWithBindings("SELECT * FROM (exec pm1.sq2(input.x)) as a", metadata, bindings); //$NON-NLS-1$
    }

    @Test public void testUseNonExistentAlias() {
        helpResolveException("SELECT portfoliob.e1 FROM ((pm1.g1 AS portfoliob JOIN pm1.g2 AS portidentb ON portfoliob.e1 = portidentb.e1) RIGHT OUTER JOIN pm1.g3 AS identifiersb ON portidentb.e1 = 'ISIN' and portidentb.e2 = identifiersb.e2) RIGHT OUTER JOIN pm1.g1 AS issuesb ON a.identifiersb.e1 = issuesb.e1"); //$NON-NLS-1$
    }       

    @Test public void testCriteria1() {                  
        CompareCriteria expected = new CompareCriteria();
        ElementSymbol es = new ElementSymbol("pm1.g1.e1"); //$NON-NLS-1$
        GroupSymbol gs = new GroupSymbol("pm1.g1"); //$NON-NLS-1$
        es.setGroupSymbol(gs);
        expected.setLeftExpression(es);
        expected.setOperator(CompareCriteria.EQ);
        expected.setRightExpression(new Constant("abc")); //$NON-NLS-1$

        Criteria actual = helpResolveCriteria("pm1.g1.e1 = 'abc'"); //$NON-NLS-1$

        assertEquals("Did not match expected criteria", expected, actual);         //$NON-NLS-1$
    }
        
    @Test public void testSubquery1() {
        Query resolvedQuery = (Query) helpResolve("SELECT e1 FROM pm1.g1, (SELECT pm1.g2.e1 AS x FROM pm1.g2) AS y WHERE e1 = x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1", "y" }); //$NON-NLS-1$ //$NON-NLS-2$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
        
    }
    
    @Test public void testStoredQuery1() {                
        StoredProcedure proc = (StoredProcedure) helpResolve("EXEC pm1.sq2('abc')"); //$NON-NLS-1$
        
        // Check number of resolved parameters
        List params = proc.getParameters();
        assertEquals("Did not get expected parameter count", 2, params.size()); //$NON-NLS-1$
        
        // Check resolved parameters
        SPParameter param1 = (SPParameter) params.get(0);
        helpCheckParameter(param1, ParameterInfo.RESULT_SET, 1, "pm1.sq2.ret", java.sql.ResultSet.class, null); //$NON-NLS-1$

        SPParameter param2 = (SPParameter) params.get(1);
        helpCheckParameter(param2, ParameterInfo.IN, 2, "pm1.sq2.in", DataTypeManager.DefaultDataClasses.STRING, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
	/**
	 * per defect 8211 - Input params do not have to be numbered sequentially in metadata.  For example,
	 * the first input param can be #1 and the second input param can be #3.  (This occurs in 
	 * QueryBuilder's metadata where the return param happens to be created in between the two
	 * input params and is numbered #2, but is not loaded into QueryBuilder's runtime env).  
	 * When the user's query is parsed and resolved, the placeholder
	 * input params are numbered #1 and #2.  This test tests that this disparity in ordering should not
	 * be a problem as long as RELATIVE ordering is in synch.
	 */
	@Test public void testStoredQueryParamOrdering_8211() {                
		StoredProcedure proc = (StoredProcedure) helpResolve("EXEC pm1.sq3a('abc', 123)"); //$NON-NLS-1$
		
		// Check number of resolved parameters
		List params = proc.getParameters();
		assertEquals("Did not get expected parameter count", 2, params.size()); //$NON-NLS-1$
        
		// Check resolved parameters
		SPParameter param1 = (SPParameter) params.get(0);
		helpCheckParameter(param1, ParameterInfo.IN, 1, "pm1.sq3a.in", DataTypeManager.DefaultDataClasses.STRING, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$

		SPParameter param2 = (SPParameter) params.get(1);
		helpCheckParameter(param2, ParameterInfo.IN, 3, "pm1.sq3a.in2", DataTypeManager.DefaultDataClasses.INTEGER, new Constant(new Integer(123))); //$NON-NLS-1$
	}    
    
    private void helpCheckParameter(SPParameter param, int paramType, int index, String name, Class type, Expression expr) {
        assertEquals("Did not get expected parameter type", paramType, param.getParameterType()); //$NON-NLS-1$
        assertEquals("Did not get expected index for param", index, param.getIndex()); //$NON-NLS-1$
        assertEquals("Did not get expected name for param", name, param.getName()); //$NON-NLS-1$
        assertEquals("Did not get expected type for param", type, param.getClassType()); //$NON-NLS-1$
        assertEquals("Did not get expected type for param", expr, param.getExpression());                 //$NON-NLS-1$
    }
    
    @Test public void testStoredSubQuery1() {
        Query resolvedQuery = (Query) helpResolve("select x.e1 from (EXEC pm1.sq1()) as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x.e1" });         //$NON-NLS-1$
    }
    
    @Test public void testStoredSubQuery2() {
        Query resolvedQuery = (Query) helpResolve("select x.e1 from (EXEC pm1.sq3('abc', 5)) as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x.e1" });         //$NON-NLS-1$
    }

    @Test public void testStoredSubQuery3() {
        Query resolvedQuery = (Query) helpResolve("select * from (EXEC pm1.sq2('abc')) as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        
        List elements = (List) ElementCollectorVisitor.getElements(resolvedQuery.getSelect(), false);
        
        ElementSymbol elem1 = (ElementSymbol)elements.get(0);
        assertEquals("Did not get expected element", "X.e1", elem1.getName() ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Did not get expected type", DataTypeManager.DefaultDataClasses.STRING, elem1.getType()); //$NON-NLS-1$

        ElementSymbol elem2 = (ElementSymbol)elements.get(1);
        assertEquals("Did not get expected element", "X.e2", elem2.getName() ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Did not get expected type", DataTypeManager.DefaultDataClasses.INTEGER, elem2.getType()); //$NON-NLS-1$
    }

    @Test public void testStoredQueryTransformationWithVariable4() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("EXEC pm1.sq2(pm1.sq2.in)"); //$NON-NLS-1$

        // resolve
        try {
            // Construct command metadata
            GroupSymbol sqGroup = new GroupSymbol("pm1.sq5"); //$NON-NLS-1$
            ArrayList sqParams = new ArrayList();
            ElementSymbol in = new ElementSymbol("pm1.sq5.in1"); //$NON-NLS-1$
            in.setType(DataTypeManager.DefaultDataClasses.STRING);
            sqParams.add(in);
            Map externalMetadata = new HashMap();
            externalMetadata.put(sqGroup, sqParams);

            QueryResolver.resolveCommand(command, metadata);
            
            fail("Expected exception on invalid variable pm1.sq2.in"); //$NON-NLS-1$
        } catch(QueryResolverException e) {
        	assertEquals("Symbol pm1.sq2.\"in\" is specified with an unknown group context", e.getMessage()); //$NON-NLS-1$
        } 
    }

    @Test public void testExec1() {
        helpResolve("EXEC pm1.sq2('xyz')"); //$NON-NLS-1$
    }

    @Test public void testExec2() {
        // implicity convert 5 to proper type
        helpResolve("EXEC pm1.sq2(5)"); //$NON-NLS-1$
    }
    
    @Test public void testExecNamedParam() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz")};//$NON-NLS-1$
        helpResolveExec("EXEC pm1.sq2(\"in\" = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }
    
    @Test public void testExecNamedParamDup() {
        helpResolveException("EXEC pm1.sq2(\"in\" = 'xyz', \"in\" = 'xyz1')");//$NON-NLS-1$
    }

    /** Should get exception because param name is wrong. */
    @Test public void testExecWrongParamName() {
        helpResolveException("EXEC pm1.sq2(in1 = 'xyz')");//$NON-NLS-1$
    }

    @Test public void testExecNamedParams() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(new Integer(5))};//$NON-NLS-1$
        helpResolveExec("EXEC pm1.sq3(\"in\" = 'xyz', in2 = 5)", expectedParameterExpressions);//$NON-NLS-1$
    }   
    
    /** try entering params out of order */
    @Test public void testExecNamedParamsReversed() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(new Integer(5))};//$NON-NLS-1$
        helpResolveExec("EXEC pm1.sq3(in2 = 5, \"in\" = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }    
    
    /** test omitting an optional parameter */
    @Test public void testExecNamedParamsOptionalParam() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(null), new Constant("something")};//$NON-NLS-1$ //$NON-NLS-2$
        helpResolveExec("EXEC pm1.sq3b(\"in\" = 'xyz', in3 = 'something')", expectedParameterExpressions);//$NON-NLS-1$
    }    

    /** test omitting a required parameter that has a default value */
    @Test public void testExecNamedParamsOmitRequiredParamWithDefaultValue() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(new Integer(666)), new Constant("YYZ")};//$NON-NLS-1$ //$NON-NLS-2$
        StoredProcedure sp = helpResolveExec("EXEC pm1.sq3b(\"in\" = 'xyz', in2 = 666)", expectedParameterExpressions);//$NON-NLS-1$
        assertEquals("EXEC pm1.sq3b(\"in\" => 'xyz', in2 => 666)", sp.toString());
    }    
    
    @Test public void testExecNamedParamsOptionalParamWithDefaults() {
        Object[] expectedParameterExpressions = helpGetStoredProcDefaultValues();
        //override the default value for the first parameter
        expectedParameterExpressions[0] = new Constant("xyz"); //$NON-NLS-1$
        helpResolveExec("EXEC pm1.sqDefaults(inString = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }    

    @Test public void testExecNamedParamsOptionalParamWithDefaultsCaseInsensitive() {
        Object[] expectedParameterExpressions = helpGetStoredProcDefaultValues();
        //override the default value for the first parameter
        expectedParameterExpressions[0] = new Constant("xyz"); //$NON-NLS-1$
        helpResolveExec("EXEC pm1.sqDefaults(iNsTrInG = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }    

    /** try just a few named parameters, in no particular order */
    @Test public void testExecNamedParamsOptionalParamWithDefaults2() {
        Object[] expectedParameterExpressions = helpGetStoredProcDefaultValues();
        //override the proper default values in expected results
        expectedParameterExpressions[3] = new Constant(Boolean.FALSE); 
        expectedParameterExpressions[9] = new Constant(new Integer(666));
        helpResolveExec("EXEC pm1.sqDefaults(ininteger = 666, inboolean={b'false'})", expectedParameterExpressions);//$NON-NLS-1$
    }    

    /** 
     * Try entering in no actual parameters, rely entirely on defaults.  
     * This also tests the default value transformation code in ExecResolver. 
     */
    @Test public void testExecNamedParamsOptionalParamWithAllDefaults() {
        Object[] expectedParameterExpressions = helpGetStoredProcDefaultValues();
        helpResolveExec("EXEC pm1.sqDefaults()", expectedParameterExpressions);//$NON-NLS-1$
    }     

    /**
     * Retrieve the Object array of expected default values for the stored procedure
     * "pm1.sqDefaults" in FakeMetadataFactory.example1().
     * @return
     * @since 4.3
     */
    private Object[] helpGetStoredProcDefaultValues() {
        
        // This needs to match what's in FakeMetadataFactory.example1 for this stored proc
        return new Object[]  {
            new Constant("x"), //$NON-NLS-1$
            new Constant(new BigDecimal("13.0")),//$NON-NLS-1$
            new Constant(new BigInteger("13")),//$NON-NLS-1$
            new Constant(Boolean.TRUE),
            new Constant(new Byte("1")),//$NON-NLS-1$
            new Constant(new Character('q')),
            new Constant(Date.valueOf("2003-03-20")),//$NON-NLS-1$
            new Constant(new Double(13.0)),
            new Constant(new Float(13.0)),
            new Constant(new Integer(13)),
            new Constant(new Long(13)),
            new Constant(new Short((short)13)),
            new Constant(Timestamp.valueOf("2003-03-20 21:26:00.000000")),//$NON-NLS-1$
            new Constant(Time.valueOf("21:26:00")),//$NON-NLS-1$
        };
    }
    
    /** Should get exception because there are two required params */
    @Test public void testExceptionNotSupplyingRequiredParam() {
        helpResolveException("EXEC pm1.sq3(in2 = 5)");//$NON-NLS-1$
    }
    
    /** Should get exception because the default value in metadata is bad for input param */
    @Test public void testExceptionBadDefaultValue() {
        helpResolveException("EXEC pm1.sqBadDefault()");//$NON-NLS-1$
    }
    
    @Test public void testExecWithForcedConvertOfStringToCorrectType() {
        // force conversion of '5' to proper type (integer)
        helpResolve("EXEC pm1.sq3('x', '5')"); //$NON-NLS-1$
    }

    /**
     * True/false are consistently representable by integers
     */
    @Test public void testExecBadType() {
        helpResolve("EXEC pm1.sq3('xyz', {b'true'})"); //$NON-NLS-1$
    }
    
    @Test public void testSubqueryInUnion() {
        String sql = "SELECT IntKey, FloatNum FROM BQT1.MediumA WHERE (IntKey >= 0) AND (IntKey < 15) " + //$NON-NLS-1$
            "UNION ALL " + //$NON-NLS-1$
            "SELECT BQT2.SmallB.IntKey, y.FloatNum " +  //$NON-NLS-1$
            "FROM BQT2.SmallB INNER JOIN " + //$NON-NLS-1$
            "(SELECT IntKey, FloatNum FROM BQT1.MediumA ) AS y ON BQT2.SmallB.IntKey = y.IntKey " + //$NON-NLS-1$
            "WHERE (y.IntKey >= 10) AND (y.IntKey < 30) " + //$NON-NLS-1$
            "ORDER BY IntKey, FloatNum";  //$NON-NLS-1$

        helpResolve(sql, FakeMetadataFactory.exampleBQTCached(), null);
    }

    @Test public void testSubQueryINClause1(){
		//select e1 from pm1.g1 where e2 in (select e2 from pm4.g1)

		//sub command
		Select innerSelect = new Select();
		ElementSymbol e2inner = new ElementSymbol("e2"); //$NON-NLS-1$
		innerSelect.addSymbol(e2inner);
		From innerFrom = new From();
		GroupSymbol pm4g1 = new GroupSymbol("pm4.g1"); //$NON-NLS-1$
		innerFrom.addGroup(pm4g1);
		Query innerQuery = new Query();
		innerQuery.setSelect(innerSelect);
		innerQuery.setFrom(innerFrom);
		
		//outer command
		Select outerSelect = new Select();
		ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
    	outerSelect.addSymbol(e1);
		From outerFrom = new From();
    	GroupSymbol pm1g1 = new GroupSymbol("pm1.g1"); //$NON-NLS-1$
		outerFrom.addGroup(pm1g1);
    	ElementSymbol e2outer = new ElementSymbol("e2"); //$NON-NLS-1$
		SubquerySetCriteria crit = new SubquerySetCriteria(e2outer, innerQuery);
    	Query outerQuery = new Query();
    	outerQuery.setSelect(outerSelect);
    	outerQuery.setFrom(outerFrom);
    	outerQuery.setCriteria(crit);
    	
    	//test
    	helpResolve(outerQuery);

    	helpCheckFrom(outerQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
    	helpCheckFrom(innerQuery, new String[] { "pm4.g1" }); //$NON-NLS-1$
    	helpCheckSelect(outerQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
    	helpCheckSelect(innerQuery, new String[] { "pm4.g1.e2" }); //$NON-NLS-1$
    	helpCheckElements(outerQuery.getSelect(),
    		new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
    		new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
    	helpCheckElements(innerQuery.getSelect(),
    		new String[] { "pm4.g1.e2" }, //$NON-NLS-1$
    		new String[] { "pm4.g1.e2" } ); //$NON-NLS-1$

    	String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm4.g1)"; //$NON-NLS-1$
    	assertEquals("Resolved string form was incorrect ", sql, outerQuery.toString()); //$NON-NLS-1$
    }    

	/**
	 * An implicit type conversion needs to be inserted because the
	 * project symbol of the subquery is not the same type as the expression in
	 * the SubquerySetCriteria object
	 */
	@Test public void testSubQueryINClauseImplicitConversion(){
		//select e1 from pm1.g1 where e2 in (select e1 from pm4.g1)
	
		//sub command
		Select innerSelect = new Select();
		ElementSymbol e1inner = new ElementSymbol("e1"); //$NON-NLS-1$
		innerSelect.addSymbol(e1inner);
		From innerFrom = new From();
		GroupSymbol pm4g1 = new GroupSymbol("pm4.g1"); //$NON-NLS-1$
		innerFrom.addGroup(pm4g1);
		Query innerQuery = new Query();
		innerQuery.setSelect(innerSelect);
		innerQuery.setFrom(innerFrom);
		
		//outer command
		Select outerSelect = new Select();
		ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
		outerSelect.addSymbol(e1);
		From outerFrom = new From();
		GroupSymbol pm1g1 = new GroupSymbol("pm1.g1"); //$NON-NLS-1$
		outerFrom.addGroup(pm1g1);
		ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
		SubquerySetCriteria crit = new SubquerySetCriteria(e2, innerQuery);
		Query outerQuery = new Query();
		outerQuery.setSelect(outerSelect);
		outerQuery.setFrom(outerFrom);
		outerQuery.setCriteria(crit);
		
		//test
		helpResolve(outerQuery);
		
		helpCheckFrom(outerQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckFrom(innerQuery, new String[] { "pm4.g1" }); //$NON-NLS-1$
		helpCheckSelect(outerQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
		helpCheckSelect(innerQuery, new String[] { "pm4.g1.e1" }); //$NON-NLS-1$
		helpCheckElements(outerQuery.getSelect(),
			new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
			new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
		helpCheckElements(innerQuery.getSelect(),
			new String[] { "pm4.g1.e1" }, //$NON-NLS-1$
			new String[] { "pm4.g1.e1" } ); //$NON-NLS-1$
		
		String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e1 FROM pm4.g1)"; //$NON-NLS-1$
		assertEquals("Resolved string form was incorrect ", sql, outerQuery.toString()); //$NON-NLS-1$
		
		//make sure there is a convert function wrapping the criteria left expression
		Collection functions = FunctionCollectorVisitor.getFunctions(outerQuery, true);
		assertTrue(functions.size() == 1);
		Function function = (Function)functions.iterator().next();
		assertTrue(function.getName().equals(FunctionLibrary.CONVERT));
		Expression[] args = function.getArgs();
		assertSame(e2, args[0]);
		assertTrue(args[1] instanceof Constant);		
	}
    
	/**
	 * Tests that resolving fails if there is no implicit conversion between the
	 * type of the expression of the SubquerySetCriteria and the type of the
	 * projected symbol of the subquery.
	 */
	@Test public void testSubQueryINClauseNoConversionFails(){
		//select e1 from pm1.g1 where e1 in (select e2 from pm4.g1)

		//sub command
		Select innerSelect = new Select();
		ElementSymbol e2inner = new ElementSymbol("e2"); //$NON-NLS-1$
		innerSelect.addSymbol(e2inner);
		From innerFrom = new From();
		GroupSymbol pm4g1 = new GroupSymbol("pm4.g1"); //$NON-NLS-1$
		innerFrom.addGroup(pm4g1);
		Query innerQuery = new Query();
		innerQuery.setSelect(innerSelect);
		innerQuery.setFrom(innerFrom);

		//outer command
		Select outerSelect = new Select();
		ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
		outerSelect.addSymbol(e1);
		From outerFrom = new From();
		GroupSymbol pm1g1 = new GroupSymbol("pm1.g1"); //$NON-NLS-1$
		outerFrom.addGroup(pm1g1);
		SubquerySetCriteria crit = new SubquerySetCriteria(e1, innerQuery);
		Query outerQuery = new Query();
		outerQuery.setSelect(outerSelect);
		outerQuery.setFrom(outerFrom);
		outerQuery.setCriteria(crit);

		//test
		this.helpResolveFails(outerQuery);
	}

    @Test public void testSubQueryINClauseTooManyColumns(){
        String sql = "select e1 from pm1.g1 where e1 in (select e1, e2 from pm4.g1)"; //$NON-NLS-1$

        //test
        this.helpResolveException(sql);
    }

	@Test public void testStoredQueryInFROMSubquery() {
		String sql = "select X.e1 from (EXEC pm1.sq3('abc', 123)) as X"; //$NON-NLS-1$

        helpResolve(sql);
	}
	
	@Test public void testStoredQueryInINSubquery() throws Exception {
		String sql = "select * from pm1.g1 where e1 in (EXEC pm1.sqsp1())"; //$NON-NLS-1$

        helpResolve(sql);
	}	
    
    @Test public void testIsXMLQuery1() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM pm1.g1", false);     //$NON-NLS-1$
    }

    @Test public void testIsXMLQuery2() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM xmltest.doc1", true); //$NON-NLS-1$
    }

    /**
     * Must be able to resolve XML query if short doc name
     * is used (assuming short doc name isn't ambiguous in a
     * VDB).  Defect 11479.
     */
    @Test public void testIsXMLQuery3() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM doc1", true); //$NON-NLS-1$
    }

    @Test public void testIsXMLQueryFail1() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM xmltest.doc1, xmltest.doc2", false); //$NON-NLS-1$
    }

    @Test public void testIsXMLQueryFail2() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM xmltest.doc1, pm1.g1", false); //$NON-NLS-1$
    }

    @Test public void testIsXMLQueryFail3() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM pm1.g1, xmltest.doc1", false); //$NON-NLS-1$
    }

    /**
     * "docA" is ambiguous as there exist two documents called
     * xmlTest2.docA and xmlTest3.docA.  Defect 11479.
     */
    @Test public void testIsXMLQueryFail4() throws Exception {
        Query query = (Query) helpParse("SELECT * FROM docA"); //$NON-NLS-1$

        try {
            QueryResolver.isXMLQuery(query, metadata);
            fail("expected exception"); //$NON-NLS-1$
        } catch (QueryResolverException e) {
            assertEquals("Group specified is ambiguous, resubmit the query by fully qualifying the group name: docA", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    @Test public void testStringConversion1() {
		// Expected left expression
        ElementSymbol e1 = new ElementSymbol("pm3.g1.e2"); //$NON-NLS-1$
        e1.setType(DataTypeManager.DefaultDataClasses.DATE);
      
        // Expected right expression
        Class srcType = DataTypeManager.DefaultDataClasses.STRING;
        String tgtTypeName = DataTypeManager.DefaultDataTypes.DATE;
        Expression expression = new Constant("2003-02-27"); //$NON-NLS-1$
        
		FunctionLibrary library = FakeMetadataFactory.SFM.getSystemFunctionLibrary();                         
		FunctionDescriptor fd = library.findFunction(FunctionLibrary.CONVERT, new Class[] { srcType, DataTypeManager.DefaultDataClasses.STRING });

		Function conversion = new Function(fd.getName(), new Expression[] { expression, new Constant(tgtTypeName) });
		conversion.setType(DataTypeManager.getDataTypeClass(tgtTypeName));
		conversion.setFunctionDescriptor(fd);
		conversion.makeImplicit();
		
		// Expected criteria
		CompareCriteria expected = new CompareCriteria();
		expected.setLeftExpression(e1);
		expected.setOperator(CompareCriteria.EQ);
		expected.setRightExpression(conversion);
         
		// Resolve the query and check against expected objects
		CompareCriteria actual = (CompareCriteria) helpResolveCriteria("pm3.g1.e2='2003-02-27'");	 //$NON-NLS-1$
	
		//if (! actual.getLeftExpression().equals(expected.getLeftExpression())) {
		//	fail("left exprs not equal");
		//} else if (!actual.getRightExpression().equals(expected.getRightExpression())) {
		//	fail("right not equal");
		//}
		
		assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
    }
		
	@Test public void testStringConversion2() {
		// Expected left expression
		ElementSymbol e1 = new ElementSymbol("pm3.g1.e2"); //$NON-NLS-1$
		e1.setType(DataTypeManager.DefaultDataClasses.DATE);
      
		// Expected right expression
		Class srcType = DataTypeManager.DefaultDataClasses.STRING;
		String tgtTypeName = DataTypeManager.DefaultDataTypes.DATE;
		Expression expression = new Constant("2003-02-27"); //$NON-NLS-1$
        
		FunctionLibrary library = FakeMetadataFactory.SFM.getSystemFunctionLibrary();                        
		FunctionDescriptor fd = library.findFunction(FunctionLibrary.CONVERT, new Class[] { srcType, DataTypeManager.DefaultDataClasses.STRING });

		Function conversion = new Function(fd.getName(), new Expression[] { expression, new Constant(tgtTypeName) });
		conversion.setType(DataTypeManager.getDataTypeClass(tgtTypeName));
		conversion.setFunctionDescriptor(fd);
		conversion.makeImplicit();
		
		// Expected criteria
		CompareCriteria expected = new CompareCriteria();
		expected.setLeftExpression(conversion);
		expected.setOperator(CompareCriteria.EQ);
		expected.setRightExpression(e1);
         
		// Resolve the query and check against expected objects
		CompareCriteria actual = (CompareCriteria) helpResolveCriteria("'2003-02-27'=pm3.g1.e2");	 //$NON-NLS-1$
	
		//if (! actual.getLeftExpression().equals(expected.getLeftExpression())) {
		//	fail("Left expressions not equal");
		//} else if (!actual.getRightExpression().equals(expected.getRightExpression())) {
		//	fail("Right expressions not equal");
		//}
		
		assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
	}		
    
    // special test for both sides are String
	@Test public void testStringConversion3() {
		// Expected left expression
		ElementSymbol e1 = new ElementSymbol("pm3.g1.e1"); //$NON-NLS-1$
		e1.setType(DataTypeManager.DefaultDataClasses.STRING);
			   
		// Expected right expression
		Constant e2 = new Constant("2003-02-27"); //$NON-NLS-1$
			   
		// Expected criteria
		CompareCriteria expected = new CompareCriteria();
		expected.setLeftExpression(e1);
		expected.setOperator(CompareCriteria.EQ);
		expected.setRightExpression(e2);
         
		// Resolve the query and check against expected objects
		CompareCriteria actual = (CompareCriteria) helpResolveCriteria("pm3.g1.e1='2003-02-27'");	 //$NON-NLS-1$
		
		//if (! actual.getLeftExpression().equals(expected.getLeftExpression())) {
		//	System.out.println("left exprs not equal");
		//} else if (!actual.getRightExpression().equals(expected.getRightExpression())) {
		//	System.out.println("right exprs not equal");
		//}
		
		assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
	}	

    @Test public void testDateToTimestampConversion_defect9747() {
        // Expected left expression
        ElementSymbol e1 = new ElementSymbol("pm3.g1.e4"); //$NON-NLS-1$
        e1.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
               
        // Expected right expression
        Constant e2 = new Constant(TimestampUtil.createDate(96, 0, 31), DataTypeManager.DefaultDataClasses.DATE);
        Function f1 = new Function("convert", new Expression[] { e2, new Constant(DataTypeManager.DefaultDataTypes.TIMESTAMP)}); //$NON-NLS-1$
        f1.makeImplicit();
               
        // Expected criteria
        CompareCriteria expected = new CompareCriteria();
        expected.setLeftExpression(e1);
        expected.setOperator(CompareCriteria.GT);
        expected.setRightExpression(f1);
         
        // Resolve the query and check against expected objects
        CompareCriteria actual = (CompareCriteria) helpResolveCriteria("pm3.g1.e4 > {d '1996-01-31'}");    //$NON-NLS-1$
        
        assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
    }   
        
    @Test public void testFailedConversion_defect9725() throws Exception{
    	helpResolveException("select * from pm3.g1 where pm3.g1.e4 > {b 'true'}", "Error Code:ERR.015.008.0027 Message:The expressions in this criteria are being compared but are of differing types (timestamp and boolean) and no implicit conversion is available:  pm3.g1.e4 > TRUE"); //$NON-NLS-1$ //$NON-NLS-2$
    } 
            
    @Test public void testLookupFunction() {     
        String sql = "SELECT lookup('pm1.g1', 'e1', 'e2', e2) AS x, lookup('pm1.g1', 'e4', 'e3', e3) AS y FROM pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x", "y" }); //$NON-NLS-1$ //$NON-NLS-2$
        helpCheckElements(resolvedQuery.getSelect(), 
            new String[] { "PM1.G1.E2", "PM1.G1.E3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "PM1.G1.E2", "PM1.G1.E3" } ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
        
        List projSymbols = resolvedQuery.getSelect().getProjectedSymbols();
        assertEquals("Wrong number of projected symbols", 2, projSymbols.size()); //$NON-NLS-1$
        assertEquals("Wrong type for first symbol", String.class, ((SingleElementSymbol)projSymbols.get(0)).getType()); //$NON-NLS-1$
        assertEquals("Wrong type for second symbol", Double.class, ((SingleElementSymbol)projSymbols.get(1)).getType()); //$NON-NLS-1$
    }

    @Test public void testLookupFunctionFailBadElement() {     
        String sql = "SELECT lookup('nosuch', 'elementhere', 'e2', e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }

    @Test public void testLookupFunctionFailNotConstantArg1() {     
        String sql = "SELECT lookup(e1, 'e1', 'e2', e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }

    @Test public void testLookupFunctionFailNotConstantArg2() {     
        String sql = "SELECT lookup('pm1.g1', e1, 'e2', e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }
   		
    @Test public void testLookupFunctionFailNotConstantArg3() {     
        String sql = "SELECT lookup('pm1.g1', 'e1', e1, e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }
 
	@Test public void testLookupFunctionVirtualGroup() throws Exception {     
		String sql = "SELECT lookup('vm1.g1', 'e1', 'e2', e2)  FROM vm1.g1 "; //$NON-NLS-1$
		Query command = (Query) helpParse(sql);
		QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());  		
	}
	
	@Test public void testLookupFunctionPhysicalGroup() throws Exception {     
		String sql = "SELECT lookup('pm1.g1', 'e1', 'e2', e2)  FROM pm1.g1 "; //$NON-NLS-1$
		Query command = (Query) helpParse(sql);
		QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());
	}
	
    @Test public void testLookupFunctionFailBadKeyElement() throws Exception {
    	String sql = "SELECT lookup('pm1.g1', 'e1', 'x', e2) AS x, lookup('pm1.g1', 'e4', 'e3', e3) AS y FROM pm1.g1"; //$NON-NLS-1$
    	Command command = QueryParser.getQueryParser().parseCommand(sql);
    	try {
    		QueryResolver.resolveCommand(command, metadata);
    		fail("exception expected"); //$NON-NLS-1$
    	} catch (QueryResolverException e) {
    		
    	}
    }
    
	@Test public void testNamespacedFunction() throws Exception {     
		String sql = "SELECT namespace.func('e1')  FROM vm1.g1 "; //$NON-NLS-1$

        FunctionLibrary funcLibrary = new FunctionLibrary(FakeMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new FakeFunctionMetadataSource()));
        FakeMetadataFacade metadata = new FakeMetadataFacade(FakeMetadataFactory.example1Cached().getStore(), funcLibrary);
		
		Query command = (Query) helpParse(sql);
		QueryResolver.resolveCommand(command, metadata);
		
		command = (Query) helpParse("SELECT func('e1')  FROM vm1.g1 ");
		QueryResolver.resolveCommand(command, metadata);  		
		
	}    
    
    // special test for both sides are String
    @Test public void testSetCriteriaCastFromExpression_9657() {
        // parse
        Criteria expected = null;
        Criteria actual = null;
        try { 
            actual = QueryParser.getQueryParser().parseCriteria("bqt1.smalla.shortvalue IN (1, 2)"); //$NON-NLS-1$
            expected = QueryParser.getQueryParser().parseCriteria("convert(bqt1.smalla.shortvalue, integer) IN (1, 2)"); //$NON-NLS-1$
           
        } catch(TeiidException e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
        }   
   
        // resolve
        try { 
            QueryResolver.resolveCriteria(expected, FakeMetadataFactory.exampleBQTCached());
            QueryResolver.resolveCriteria(actual, FakeMetadataFactory.exampleBQTCached());
        } catch(TeiidException e) { 
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 
        
        // Tweak expected to hide convert function - this is expected
        ((Function) ((SetCriteria)expected).getExpression()).makeImplicit();
        
        assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
    }    
    
    /** select e1 from pm1.g1 where e2 BETWEEN 1000 AND 2000 */
    @Test public void testBetween1(){
        String sql = "select e1 from pm1.g1 where e2 BETWEEN 1000 AND 2000"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where e2 NOT BETWEEN 1000 AND 2000 */
    @Test public void testBetween2(){
        String sql = "select e1 from pm1.g1 where e2 NOT BETWEEN 1000 AND 2000"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e2 from pm1.g1 where e4 BETWEEN 1000 AND e2 */
    @Test public void testBetween3(){
        String sql = "select e2 from pm1.g1 where e4 BETWEEN 1000 AND e2"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e2 from pm1.g1 where e2 BETWEEN 1000 AND e4 */
    @Test public void testBetween4(){
        String sql = "select e2 from pm1.g1 where e2 BETWEEN 1000 AND e4"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where 1000 BETWEEN e1 AND e2 */
    @Test public void testBetween5(){
        String sql = "select e1 from pm1.g1 where 1000 BETWEEN e1 AND e2"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where 1000 BETWEEN e2 AND e1 */
    @Test public void testBetween6(){
        String sql = "select e1 from pm1.g1 where 1000 BETWEEN e2 AND e1"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm3.g1 where e2 BETWEEN e3 AND e4 */
    @Test public void testBetween7(){
        String sql = "select e1 from pm3.g1 where e2 BETWEEN e3 AND e4"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select pm3.g1.e1 from pm3.g1, pm3.g2 where pm3.g1.e4 BETWEEN pm3.g1.e2 AND pm3.g2.e2 */
    @Test public void testBetween8(){
        String sql = "select pm3.g1.e1 from pm3.g1, pm3.g2 where pm3.g1.e4 BETWEEN pm3.g1.e2 AND pm3.g2.e2"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where e2 = any (select e2 from pm4.g1) */
    @Test public void testCompareSubQuery1(){

        String sql = "select e1 from pm1.g1 where e2 = any (select e2 from pm4.g1)"; //$NON-NLS-1$
        Query outerQuery = (Query) this.helpResolveSubquery(sql, new String[0]);

        helpCheckFrom(outerQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(outerQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
        helpCheckElements(outerQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
//        helpCheckFrom(innerQuery, new String[] { "pm4.g1" });
//        helpCheckSelect(innerQuery, new String[] { "pm4.g1.e2" });
//        helpCheckElements(innerQuery.getSelect(),
//            new String[] { "pm4.g1.e2" },
//            new String[] { "pm4.g1.e2" } );

        String sqlActual = "SELECT e1 FROM pm1.g1 WHERE e2 = ANY (SELECT e2 FROM pm4.g1)"; //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sqlActual, outerQuery.toString()); //$NON-NLS-1$
    }    

    /** select e1 from pm1.g1 where e2 = all (select e2 from pm4.g1) */
    @Test public void testCompareSubQuery2(){
        String sql = "select e1 from pm1.g1 where e2 = all (select e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    /** select e1 from pm1.g1 where e2 < (select e2 from pm4.g1 where e1 = '3') */
    @Test public void testCompareSubQuery3(){
        String sql = "select e1 from pm1.g1 where e2 < (select e2 from pm4.g1 where e1 = '3')"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    /** select e1 from pm1.g1 where e2 < (select e2 from pm4.g1 where e1 = '3') */
    @Test public void testCompareSubQueryImplicitConversion(){
        String sql = "select e1 from pm1.g1 where e1 < (select e2 from pm4.g1 where e1 = '3')"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    @Test public void testExistsSubQuery(){
        String sql = "select e1 from pm1.g1 where exists (select e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    @Test public void testExistsSubQuery2(){
        String sql = "select e1 from pm1.g1 where exists (select e1, e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    @Test public void testScalarSubQueryInSelect(){
        String sql = "select e1, (select e2 from pm4.g1 where e1 = '3') from pm1.g1"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    @Test public void testScalarSubQueryInSelect2(){
        String sql = "select (select e2 from pm4.g1 where e1 = '3'), e1 from pm1.g1"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    @Test public void testScalarSubQueryInSelectWithAlias(){
        String sql = "select e1, (select e2 from pm4.g1 where e1 = '3') as X from pm1.g1"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    @Test public void testSelectWithNoFrom() {
        String sql = "SELECT 5"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testSelectWithNoFrom_Alias() {
        String sql = "SELECT 5 AS INTKEY"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testSelectWithNoFrom_Alias_OrderBy() {
        String sql = "SELECT 5 AS INTKEY ORDER BY INTKEY"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testSubqueryCorrelatedInCriteria(){
        String sql = "select e2 from pm1.g1 where e2 = all (select e2 from pm4.g1 where pm1.g1.e1 = pm4.g1.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm1.g1.e1"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInCriteria2(){
        String sql = "select e1 from pm1.g1 where e2 = all (select e2 from pm4.g1 where pm1.g1.e1 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm1.g1.e1"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInCriteria3(){
        String sql = "select e1 from pm1.g1 X where e2 = all (select e2 from pm4.g1 where X.e1 = pm4.g1.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }
    
    @Test public void testSubqueryCorrelatedInCriteria4(){
        String sql = "select e2 from pm1.g1 X where e2 in (select e2 from pm1.g1 Y where X.e1 = Y.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }    

    @Test public void testSubqueryCorrelatedInCriteria5(){
        String sql = "select e1 from pm1.g1 X where e2 = all (select e2 from pm1.g1 Y where X.e1 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }    

    /* 'e5' is only in pm4.g2 */
    @Test public void testSubqueryCorrelatedInCriteria6(){
        String sql = "select e1 from pm4.g2 where e2 = some (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    /* 'e5' is only in pm4.g2 */
    @Test public void testSubqueryCorrelatedInCriteria7(){
        String sql = "select e1 from pm4.g2 where exists (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInHaving(){
        String sql = "select e1, e2 from pm4.g2 group by e2 having e2 in (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInHaving2(){
        String sql = "select e1, e2 from pm4.g2 group by e2 having e2 <= all (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    /* 'e5' is only in pm4.g2 */
    @Test public void testSubqueryCorrelatedInSelect(){
        String sql = "select e1, (select e2 from pm4.g1 where e5 = e1) from pm4.g2"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInSelect2(){
        String sql = "select e1, (select e2 from pm4.g1 where pm4.g2.e5 = e1) from pm4.g2"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInSelect3(){
        String sql = "select e1, (select e2 from pm4.g1 Y where X.e5 = Y.e1) from pm4.g2 X"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e5"}); //$NON-NLS-1$
    }

    /* 'e5' is only in pm4.g2 */
    @Test public void testNestedCorrelatedSubqueries(){
        String sql = "select e1, (select e2 from pm1.g1 where e2 = all (select e2 from pm4.g1 where e5 = e1)) from pm4.g2"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    /**
     * 'e5' is in pm4.g2, so it will be resolved to the group aliased as 'Y'
     */
    @Test public void testNestedCorrelatedSubqueries2(){
        String sql = "select e1, (select e2 from pm4.g2 Y where e2 = all (select e2 from pm4.g1 where e5 = e1)) from pm4.g2 X"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"Y.e5"}); //$NON-NLS-1$
    }

    /**
     *  'e5' is in pm4.g2; it will be resolved to the group aliased as 'X' 
     */
    @Test public void testNestedCorrelatedSubqueries3(){
        String sql = "select e1, (select e2 from pm4.g2 Y where e2 = all (select e2 from pm4.g1 where X.e5 = e1)) from pm4.g2 X"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e5"}); //$NON-NLS-1$
    }

    /**
     *  'e5' is in X and Y 
     */
    @Test public void testNestedCorrelatedSubqueries4(){
        String sql = "select X.e2 from pm4.g2 Y, pm4.g2 X where X.e2 = all (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        helpResolveException(sql, metadata, "Element \"e5\" is ambiguous, it exists in two or more groups."); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInCriteriaVirtualLayer(){
        String sql = "select e2 from vm1.g1 where e2 = all (select e2 from vm1.g2 where vm1.g1.e1 = vm1.g2.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"vm1.g1.e1"}); //$NON-NLS-1$
    }

    @Test public void testSubqueryCorrelatedInCriteriaVirtualLayer2(){
        String sql = "select e2 from vm1.g1 X where e2 = all (select e2 from vm1.g2 where X.e1 = vm1.g2.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }

    /** 
     * Although this query makes no sense, the "e1" in the nested criteria is
     * NOT a correlated reference 
     */
    @Test public void testSubqueryNonCorrelatedInCriteria(){
        String sql = "select e2 from pm1.g1 where e2 = all (select e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /** 
     * Although this query makes no sense, the "e1" in the nested criteria is
     * NOT a correlated reference 
     */
    @Test public void testSubqueryNonCorrelatedInCriteria2(){
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1))"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /** 
     * Although this query makes no sense, the "e1" in the nested criteria is
     * NOT a correlated reference 
     */
    @Test public void testSubqueryNonCorrelatedInCriteria3(){
        String sql = "SELECT e2 FROM pm2.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /** 
     * The group pm1.g1 in the FROM clause of the subquery should resolve to the 
     * group in metadata, not the temporary child metadata group defined by the
     * outer query.
     */
    @Test public void testSubquery_defect10090(){
        String sql = "select pm1.g1.e1 from pm1.g1 where pm1.g1.e2 in (select pm1.g1.e2 from pm1.g1 where pm1.g1.e4 = 2.0)";  //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /**
     * Workaround is to alias group in FROM of outer query (aliasing subquery group doesn't work)
     */
    @Test public void testSubquery_defect10090Workaround(){
        String sql = "select X.e1 from pm1.g1 X where X.e2 in (select pm1.g1.e2 from pm1.g1 where pm1.g1.e4 = 2.0)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    @Test public void testSubquery2_defect10090(){
        String sql = "select pm1.g1.e1 from pm1.g1 where pm1.g1.e2 in (select X.e2 from pm1.g1 X where X.e4 = 2.0)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }
    
    /** test jdbc USER method */
    @Test public void testUser() {
        //String sql = "select intkey from SmallA where user() = 'bqt2'";

        // Expected left expression
        FunctionLibrary library = FakeMetadataFactory.SFM.getSystemFunctionLibrary();                          
        FunctionDescriptor fd = library.findFunction(FunctionLibrary.USER, new Class[] { });
        Function user = new Function(fd.getName(), new Expression[] {});
        user.setFunctionDescriptor(fd);

        // Expected criteria
        CompareCriteria expected = new CompareCriteria();
        // Expected right expression
        Expression e1 = new Constant("bqt2", String.class); //$NON-NLS-1$
        // Expected left expression
        expected.setLeftExpression(user);
        expected.setOperator(CompareCriteria.EQ);
        expected.setRightExpression(e1);
         
        // Resolve the query and check against expected objects
        CompareCriteria actual = (CompareCriteria) helpResolveCriteria("user()='bqt2'");    //$NON-NLS-1$
        assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
    }
    
    @Test public void testCaseExpression1() {
        String sql = "SELECT e1, CASE e2 WHEN 0 THEN 20 WHEN 1 THEN 21 WHEN 2 THEN 500 END AS testElement FROM pm1.g1" //$NON-NLS-1$
                    +" WHERE e1 = CASE WHEN e2 = 0 THEN 'a' WHEN e2 = 1 THEN 'b' ELSE 'c' END"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    
    @Test public void testCaseExpression2() {
        // nested case expressions
        String sql = "SELECT CASE e2" + //$NON-NLS-1$
                                " WHEN 0 THEN CASE e1 " + //$NON-NLS-1$
                                                " WHEN 'a' THEN 100" + //$NON-NLS-1$
                                                " WHEN 'b' THEN 200 " + //$NON-NLS-1$
                                                " ELSE 1000 " + //$NON-NLS-1$
                                            " END" + //$NON-NLS-1$
                                " WHEN 1 THEN 21" + //$NON-NLS-1$
                                " WHEN (CASE WHEN e1 = 'z' THEN 2 WHEN e1 = 'y' THEN 100 ELSE 3 END) THEN 500" + //$NON-NLS-1$
                           " END AS testElement FROM pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testCaseExpressionWithNestedFunction() {
        String sql = "SELECT CASE WHEN e2 < 0 THEN abs(CASE WHEN e2 < 0 THEN -1 ELSE e2 END)" + //$NON-NLS-1$
                           " ELSE e2 END FROM pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testFunctionWithNestedCaseExpression() {
        String sql = "SELECT abs(CASE e1 WHEN 'testString1' THEN -13" + //$NON-NLS-1$
                                       " WHEN 'testString2' THEN -5" + //$NON-NLS-1$
                                       " ELSE abs(e2)" + //$NON-NLS-1$
                               " END) AS absVal FROM pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }
 
    @Test public void testDefect10809(){
        String sql = "select * from LOB_TESTING_ONE where CLOB_COLUMN LIKE '%fff%'"; //$NON-NLS-1$
        helpResolve(helpParse(sql), FakeMetadataFactory.exampleBQTCached());
    }
    
    @Test public void testNonAutoConversionOfLiteralIntegerToShort() throws Exception {       
        // parse
        Query command = (Query) QueryParser.getQueryParser().parseCommand("SELECT intkey FROM bqt1.smalla WHERE shortvalue = 5"); //$NON-NLS-1$
        
        // resolve
        QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQTCached());
        
        // Check whether an implicit conversion was added on the correct side
        CompareCriteria crit = (CompareCriteria) command.getCriteria();
         
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, crit.getRightExpression().getType());
        assertEquals("Sql is incorrect after resolving", "SELECT intkey FROM bqt1.smalla WHERE shortvalue = 5", command.toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNonAutoConversionOfLiteralIntegerToShort2() throws Exception {       
        // parse
        Query command = (Query) QueryParser.getQueryParser().parseCommand("SELECT intkey FROM bqt1.smalla WHERE 5 = shortvalue"); //$NON-NLS-1$
        
        // resolve
        QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQTCached());
        
        // Check whether an implicit conversion was added on the correct side
        CompareCriteria crit = (CompareCriteria) command.getCriteria();
         
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, crit.getLeftExpression().getType());
        assertEquals("Sql is incorrect after resolving", "SELECT intkey FROM bqt1.smalla WHERE 5 = shortvalue", command.toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }               

    @Test public void testAliasedOrderBy() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1 as y FROM pm1.g1 ORDER BY y"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "y" }); //$NON-NLS-1$
    }        
    
    @Test public void testUnaliasedOrderBySucceeds() {
        helpResolve("SELECT pm1.g1.e1 a, pm1.g1.e1 b FROM pm1.g1 ORDER BY pm1.g1.e1"); //$NON-NLS-1$
    }
    
    @Test public void testUnaliasedOrderBySucceeds1() {
        helpResolve("SELECT pm1.g1.e1 a FROM pm1.g1 group by pm1.g1.e1 ORDER BY pm1.g1.e1"); //$NON-NLS-1$
    }
    
    @Test public void testUnaliasedOrderByFails() {
        helpResolveException("SELECT pm1.g1.e1 e2 FROM pm1.g1 group by pm1.g1.e1 ORDER BY pm1.g1.e2"); //$NON-NLS-1$
    }
    
    @Test public void testUnaliasedOrderByFails1() {
        helpResolveException("SELECT pm1.g1.e1 e2 FROM pm1.g1 group by pm1.g1.e1 ORDER BY pm1.g1.e2 + 1"); //$NON-NLS-1$
    }

    /** 
     * the group g1 is not known to the order by clause of a union
     */
    @Test public void testUnionOrderByFail() {
        helpResolveException("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY g1.e1", "ORDER BY expression 'g1.e1' cannot be used with a set query."); //$NON-NLS-1$ //$NON-NLS-2$
    }      
    
    @Test public void testUnionOrderByFail1() {
        helpResolveException("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY pm1.g1.e1", "ORDER BY expression 'pm1.g1.e1' cannot be used with a set query."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testOrderByPartiallyQualified() {
        helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY g1.e1"); //$NON-NLS-1$
    }
    
    /** 
     * the group g1 is not known to the order by clause of a union
     */
    @Test public void testUnionOrderBy() {
        helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY e1"); //$NON-NLS-1$
    } 
    
    /** 
     * Test for defect 12087 - Insert with implicit conversion from integer to short
     */
    @Test public void testImplConversionBetweenIntAndShort() throws Exception {       
    	Insert command = (Insert)QueryParser.getQueryParser().parseCommand("Insert into pm5.g3(e2) Values(100)"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);
        assertTrue(((Expression)command.getValues().get(0)).getType() == DataTypeManager.DefaultDataClasses.SHORT);
    }
    
    public static FakeMetadataFacade example_12968() { 
        // Create models
        FakeMetadataObject pm1 = FakeMetadataFactory.createPhysicalModel("myModel"); //$NON-NLS-1$
        FakeMetadataObject pm2 = FakeMetadataFactory.createPhysicalModel("myModel2"); //$NON-NLS-1$
        
        FakeMetadataObject pm1g1 = FakeMetadataFactory.createPhysicalGroup("myModel.myTable", pm1); //$NON-NLS-1$
        FakeMetadataObject pm2g1 = FakeMetadataFactory.createPhysicalGroup("myModel2.mySchema.myTable2", pm2); //$NON-NLS-1$
        
        List pm1g1e = FakeMetadataFactory.createElements(pm1g1, 
            new String[] { "myColumn", "myColumn2" }, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List pm2g1e = FakeMetadataFactory.createElements(pm2g1, 
            new String[] { "myColumn", "myColumn2" }, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        
        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(pm1);
        store.addObject(pm1g1);     
        store.addObjects(pm1g1e);
        store.addObject(pm2g1);     
        store.addObjects(pm2g1e);
        
        return new FakeMetadataFacade(store);
    }
        
    @Test public void testDefect12968_union() {
        helpResolve(
            helpParse("SELECT myModel.myTable.myColumn AS myColumn from myModel.myTable UNION " + //$NON-NLS-1$
                "SELECT convert(null, string) AS myColumn From myModel2.mySchema.myTable2"),  //$NON-NLS-1$
            example_12968());
    }


    @Test public void testUnionQueryWithNull() throws Exception{
    	helpResolve("SELECT NULL, e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT NULL, e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL FROM pm1.g2 UNION ALL SELECT e1, NULL FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL as e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL as e2 FROM pm1.g1 UNION ALL SELECT e1, e3 FROM pm1.g2"); //$NON-NLS-1$
    }
    
    @Test public void testUnionQueryWithDiffTypes() throws Exception{
        helpResolve("SELECT e1, e3 FROM pm1.g1 UNION ALL SELECT e2, e3 FROM pm1.g2"); //$NON-NLS-1$
        helpResolve("SELECT e1, e3 FROM pm1.g1 UNION ALL SELECT e2, e3 FROM pm1.g2 UNION ALL SELECT NULL, e3 FROM pm1.g2");      //$NON-NLS-1$
        helpResolve("SELECT e1, e3 FROM pm1.g1 UNION ALL SELECT e3, e3 FROM pm1.g2 UNION ALL SELECT NULL, e3 FROM pm1.g2");      //$NON-NLS-1$
        helpResolve("SELECT e1, e2 FROM pm1.g3 UNION ALL SELECT MAX(e4), e2 FROM pm1.g1 UNION ALL SELECT e3, e2 FROM pm1.g2"); //$NON-NLS-1$
        helpResolve("SELECT e1, e4 FROM pm1.g1 UNION ALL SELECT e2, e3 FROM pm1.g2"); //$NON-NLS-1$
        helpResolve("SELECT e4, e2 FROM pm1.g1 UNION ALL SELECT e3, e2 FROM pm1.g2"); //$NON-NLS-1$
        helpResolve("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT e3, e4 FROM pm1.g2");   //$NON-NLS-1$
        helpResolve("SELECT e4, e2 FROM pm1.g1 UNION ALL SELECT e3, e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g2");  //$NON-NLS-1$
        helpResolve("SELECT e4, e2 FROM pm1.g1 UNION ALL SELECT e1, e2 FROM pm1.g2"); //$NON-NLS-1$
        helpResolve("SELECT MAX(e4), e2 FROM pm1.g1 UNION ALL SELECT e3, e2 FROM pm1.g2"); //$NON-NLS-1$
        //chooses a common type
        helpResolve("select e2 from pm3.g1 union select e3 from pm3.g1 union select e4 from pm3.g1"); //$NON-NLS-1$
    } 
    
    @Test public void testUnionQueryWithDiffTypesFails() throws Exception{
        helpResolveException("SELECT e1 FROM pm1.g1 UNION (SELECT e2 FROM pm1.g2 UNION SELECT e2 from pm1.g1 order by e2)", "The Expression e2 used in a nested UNION ORDER BY clause cannot be implicitly converted from type integer to type string."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testNestedUnionQueryWithNull() throws Exception{
        SetQuery command = (SetQuery)helpResolve("SELECT e2, e3 FROM pm1.g1 UNION (SELECT null, e3 FROM pm1.g2 UNION SELECT null, e3 from pm1.g1)"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((SingleElementSymbol)command.getProjectedSymbols().get(0)).getType());
    }
    
    @Test public void testUnionQueryClone() throws Exception{
        SetQuery command = (SetQuery)helpResolve("SELECT e2, e3 FROM pm1.g1 UNION SELECT e3, e2 from pm1.g1"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((SingleElementSymbol)command.getProjectedSymbols().get(1)).getType());
        
        command = (SetQuery)command.clone();
        
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((SingleElementSymbol)command.getProjectedSymbols().get(1)).getType());
    }
    
    @Test public void testSelectIntoNoFrom() {
        helpResolve("SELECT 'a', 19, {b'true'}, 13.999 INTO pm1.g1"); //$NON-NLS-1$
    }
    
    @Test public void testSelectInto() {
        helpResolve("SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2"); //$NON-NLS-1$
    }
    
    @Test public void testSelectIntoTempGroup() {
        helpResolve("SELECT 'a', 19, {b'true'}, 13.999 INTO #myTempTable"); //$NON-NLS-1$
        helpResolve("SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g1"); //$NON-NLS-1$
    }
                
    //procedural relational mapping
    @Test public void testProcInVirtualGroup1(){
        String sql = "select e1 from pm1.vsp26 where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testProcInVirtualGroup2(){
        String sql = "select * from pm1.vsp26 as p where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testProcInVirtualGroup3(){
        String sql = "SELECT P.e1 as ve3 FROM pm1.vsp26 as P, pm1.g2 where P.e1=g2.e1 and param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testProcInVirtualGroup4(){
        String sql = "SELECT P.e1 as ve3 FROM pm1.vsp26 as P, vm1.g1 where P.e1=g1.e1 and param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testProcInVirtualGroup5(){
        String sql = "SELECT * FROM (SELECT p.* FROM pm1.vsp26 as P, vm1.g1 where P.e1=g1.e1) x where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testProcInVirtualGroup6(){
        String sql = "SELECT P.e1 as ve3, P.e2 as ve4 FROM pm1.vsp26 as P where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }

    @Test public void testProcInVirtualGroup7(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }

    @Test public void testProcInVirtualGroup7a(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param1=1"; //$NON-NLS-1$
        helpResolve(sql);
    }
        
    @Test public void testProcParamComparison_defect13653() {
        String userSql = "SELECT * FROM (EXEC mmspTest1.MMSP5('a')) AS a, (EXEC mmsptest1.mmsp6('b')) AS b"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();
        AnalysisRecord analysis = AnalysisRecord.createNonRecordingRecord();
        
        Query query = (Query) helpResolve(userSql, metadata, analysis);
        From from = query.getFrom();
        Collection fromClauses = from.getClauses();
        SPParameter params[] = new SPParameter[2];
        Iterator iter = fromClauses.iterator();
        while(iter.hasNext()) {
            SubqueryFromClause clause = (SubqueryFromClause) iter.next();
            StoredProcedure proc = (StoredProcedure) clause.getCommand();
            List procParams = proc.getParameters();
            for(int i=0; i<procParams.size(); i++) {
                SPParameter param = (SPParameter) procParams.get(i);
                if(param.getParameterType() == ParameterInfo.IN) {
                    if(params[0] == null) {
                        params[0] = param;
                    } else {
                        params[1] = param;
                    }
                }
            }
        }
        
        assertTrue("Params should be not equal", ! params[0].equals(params[1])); //$NON-NLS-1$
    }
    
    @Test public void testNullConstantInSelect() throws Exception {
        String userSql = "SELECT null as x"; //$NON-NLS-1$
        Query query = (Query)helpParse(userSql);
        
        QueryResolver.resolveCommand(query, FakeMetadataFactory.exampleBQTCached());
        
        // Check type of resolved null constant
        SingleElementSymbol symbol = (SingleElementSymbol) query.getSelect().getSymbols().get(0);
        assertNotNull(symbol.getType());
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, symbol.getType());
    }

    @Test public void test11716() throws Exception {
    	String sql = "SELECT e1 FROM pm1.g1 where e1='1'"; //$NON-NLS-1$
    	Map externalMetadata = new HashMap();
    	GroupSymbol inputSet = new GroupSymbol("INPUT"); //$NON-NLS-1$
    	List inputSetElements = new ArrayList();
    	ElementSymbol inputSetElement = new ElementSymbol("INPUT.e1"); //$NON-NLS-1$
    	inputSetElements.add(inputSetElement);
    	externalMetadata.put(inputSet, inputSetElements);
        Query command = (Query)helpParse(sql);
        QueryResolver.resolveCommand(command, metadata);
        Collection groups = GroupCollectorVisitor.getGroups(command, false);
        assertFalse(groups.contains(inputSet));
    }
    
    @Test public void testInputToInputsConversion() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = (Select pm1.g1.e2 from pm1.g1 where e2=INPUTS.e2);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e2=40"; //$NON-NLS-1$
        
        Command command = helpResolveUpdateProcedure(procedure, userUpdateStr);
        assertEquals("CREATE PROCEDURE\nBEGIN\nDECLARE integer var1;\nROWS_UPDATED = (SELECT pm1.g1.e2 FROM pm1.g1 WHERE e2 = INPUTS.e2);\nEND", command.toString());
    }
    
    @Test public void testDefect16894_resolverException_1() {
        helpResolve("SELECT * FROM (SELECT * FROM Pm1.g1 AS Y) AS X"); //$NON-NLS-1$
    }

    @Test public void testDefect16894_resolverException_2() {
        helpResolve("SELECT * FROM (SELECT * FROM Pm1.g1) AS X"); //$NON-NLS-1$
    }

    @Test public void testDefect17385() throws Exception{  
		String sql = "select e1 as x ORDER BY x"; //$NON-NLS-1$      
		helpResolveException(sql);
	}
    
    @Test public void testValidFullElementNotInQueryGroups() {
        helpResolveException("select pm1.g1.e1 FROM pm1.g1 g"); //$NON-NLS-1$
    }
    
    @Test public void testUnionInSubquery() throws Exception {
        String sql = "SELECT StringKey FROM (SELECT BQT2.SmallB.StringKey FROM BQT2.SmallB union SELECT convert(BQT2.SmallB.FloatNum, string) FROM BQT2.SmallB) x";  //$NON-NLS-1$
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQTCached());
    }

    @Test public void testParameterError() throws Exception {
        helpResolveException("EXEC pm1.sp2(1, 2)", metadata, "Error Code:ERR.015.008.0007 Message:Incorrect number of parameters specified on the stored procedure pm1.sp2 - expected 1 but got 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testUnionOfAliasedLiteralsGetsModified() {
        String sql = "SELECT 5 AS x UNION ALL SELECT 10 AS x"; //$NON-NLS-1$
        Command c = helpResolve(sql); 
        assertEquals(sql, c.toString());
    }
    
    @Test public void testXMLWithProcSubquery() {
        String sql = "SELECT * FROM xmltest.doc4 WHERE node2 IN (SELECT e1 FROM (EXEC pm1.vsp1()) AS x)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
    }
    
    @Test public void testDefect18832() {
        String sql = "SELECT * from (SELECT null as a, e1 FROM pm1.g1) b"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        List projectedSymbols = c.getProjectedSymbols();
        for(int i=0; i< projectedSymbols.size(); i++) {
            ElementSymbol symbol = (ElementSymbol)projectedSymbols.get(i);
            assertTrue(!symbol.getType().equals(DataTypeManager.DefaultDataClasses.NULL));           
        }
    }
    
    @Test public void testDefect18832_2() {
        String sql = "SELECT a.*, b.* from (SELECT null as a, e1 FROM pm1.g1) a, (SELECT e1 FROM pm1.g1) b"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        List projectedSymbols = c.getProjectedSymbols();
        for(int i=0; i< projectedSymbols.size(); i++) {
            ElementSymbol symbol = (ElementSymbol)projectedSymbols.get(i);
            assertTrue(!symbol.getType().equals(DataTypeManager.DefaultDataClasses.NULL));           
        }
    }
    
    @Test public void testDefect20113() {
        String sql = "SELECT g1.* from pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }

    @Test public void testDefect20113_2() {
        String sql = "SELECT g7.* from g7"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    private void verifyProjectedTypes(Command c, Class[] types) {
        List projSymbols = c.getProjectedSymbols();
        for(int i=0; i<projSymbols.size(); i++) {
            assertEquals("Found type mismatch at column " + i, types[i], ((SingleElementSymbol) projSymbols.get(i)).getType()); //$NON-NLS-1$
        }                
    }
    
    @Test public void testNestedInlineViews() throws Exception {
        String sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM pm1.g1) AS Y) AS X"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
        
        verifyProjectedTypes(c, new Class[] { String.class, Integer.class, Boolean.class, Double.class });
    }

    @Test public void testNestedInlineViewsNoStar() throws Exception {
        String sql = "SELECT e1 FROM (SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS Y) AS X"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());      
        
        verifyProjectedTypes(c, new Class[] { String.class });
    }

    @Test public void testNestedInlineViewsCount() throws Exception {
        String sql = "SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM pm1.g1) AS Y) AS X"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());        
        verifyProjectedTypes(c, new Class[] { Integer.class });
    }
    
    @Test public void testAggOverInlineView() throws Exception {
        String sql = "SELECT SUM(x) FROM (SELECT (e2 + 1) AS x FROM pm1.g1) AS g"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());        
        verifyProjectedTypes(c, new Class[] { Long.class });
        
    }

    @Test public void testCaseOverInlineView() throws Exception {
        String sql = "SELECT CASE WHEN x > 0 THEN 1.0 ELSE 2.0 END FROM (SELECT e2 AS x FROM pm1.g1) AS g"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());        
        verifyProjectedTypes(c, new Class[] { Double.class });
        
    }
    
    //procedure - select * from temp table 
    @Test public void testDefect20083_1 (){
        helpResolve("EXEC pm1.vsp56()");   //$NON-NLS-1$
    }
    
    //procedure - select * from temp table order by
    @Test public void testDefect20083_2 (){
        helpResolve("EXEC pm1.vsp57()");   //$NON-NLS-1$
    }
    
    @Test public void testTypeConversionOverUnion() throws Exception { 
        String sql = "SELECT * FROM (SELECT e2, e1 FROM pm1.g1 UNION SELECT convert(e2, string), e1 FROM pm1.g1) FOO where e2/2 = 1"; //$NON-NLS-1$ 
        helpResolveException(sql); 
    }
    
    @Test public void testVariableDeclarationAfterStatement() throws Exception{
        String procedure = "CREATE VIRTUAL PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "select * from pm1.g1 where pm1.g1.e1 = VARIABLES.X;\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.X = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        helpResolveException(procedure, "Element \"VARIABLES.X\" is not defined by any relevant group."); //$NON-NLS-1$
    }
    
    /**
     * same as above, but with an xml query 
     * @throws Exception
     */
    @Test public void testVariableDeclarationAfterStatement1() throws Exception{
        String procedure = "CREATE VIRTUAL PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "select * from xmltest.doc1 where node1 = VARIABLES.X;\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.X = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        helpResolveException(procedure, "Error Code:ERR.015.008.0019 Message:Unable to resolve element: VARIABLES.X"); //$NON-NLS-1$
    }
    
    @Test public void testCreate() {
        String sql = "CREATE LOCAL TEMPORARY TABLE temp_table (column1 string)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());  
    }
    
    @Test public void testCreateQualifiedName() {
        String sql = "CREATE LOCAL TEMPORARY TABLE pm1.g1 (column1 string)"; //$NON-NLS-1$
        helpResolveException(sql, "Cannot create temporary table \"pm1.g1\". Local temporary tables must be created with unqualified names."); //$NON-NLS-1$
    }

    @Test public void testCreatePk() {
        String sql = "CREATE LOCAL TEMPORARY TABLE foo (column1 string, column2 integer, primary key (column1, column2))"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    @Test public void testCreateUnknownPk() {
        String sql = "CREATE LOCAL TEMPORARY TABLE foo (column1 string, primary key (column2))"; //$NON-NLS-1$
        helpResolveException(sql, "Element \"column2\" is not defined by any relevant group."); //$NON-NLS-1$
    }

    @Test public void testCreateAlreadyExists() {
        String sql = "CREATE LOCAL TEMPORARY TABLE g1 (column1 string)"; //$NON-NLS-1$
        helpResolveException(sql, "Cannot create temporary table \"g1\". A table with the same name already exists."); //$NON-NLS-1$
    }

    @Test public void testCreateImplicitName() {
        String sql = "CREATE LOCAL TEMPORARY TABLE #g1 (column1 string)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
    }
    
    @Test public void testCreateInProc() throws Exception{
        helpResolveException("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table g1(c1 string); end", "Cannot create temporary table \"g1\". A table with the same name already exists.");//$NON-NLS-1$ //$NON-NLS-2$
    }
    
    //this was the old virt.agg procedure.  It was defined in such a way that relied on the scope leak of #temp
    //the exception here is a little weak since there are multiple uses of #temp in the block
    @Test public void testTempTableScope() {
        String proc =  "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
            + "BEGIN " //$NON-NLS-1$
            + "        DECLARE integer VARIABLES.BITS;" //$NON-NLS-1$
            + "        LOOP ON (SELECT DISTINCT phys.t.ID, phys.t.Name FROM phys.t) AS idCursor" //$NON-NLS-1$
            + "        BEGIN" //$NON-NLS-1$
            + "                VARIABLES.BITS = 0;" //$NON-NLS-1$
            + "                LOOP ON (SELECT phys.t.source_bits FROM phys.t WHERE phys.t.ID = idCursor.id) AS bitsCursor" //$NON-NLS-1$
            + "                BEGIN" //$NON-NLS-1$
            + "                        VARIABLES.BITS = bitor(VARIABLES.BITS, bitsCursor.source_bits);" //$NON-NLS-1$
            + "                END" //$NON-NLS-1$
            + "                SELECT idCursor.id, idCursor.name, VARIABLES.BITS INTO #temp;" //$NON-NLS-1$
            + "        END" //$NON-NLS-1$
            + "        SELECT ID, Name, #temp.BITS AS source_bits FROM #temp;" //$NON-NLS-1$                                          
            + "END"; //$NON-NLS-1$ 
        
        helpResolveException(proc, FakeMetadataFactory.exampleBitwise(), "Group does not exist: #temp"); //$NON-NLS-1$
    }
    
    @Test public void testDrop() {
        String sql = "DROP TABLE temp_table"; //$NON-NLS-1$
        helpResolveException(sql, "Group does not exist: temp_table"); //$NON-NLS-1$ 
    }
    
    @Test public void testResolveUnqualifiedCriteria() throws Exception{
        Criteria criteria = QueryParser.getQueryParser().parseCriteria("e1 = 1"); //$NON-NLS-1$
           
        // resolve
        try { 
            QueryResolver.resolveCriteria(criteria, metadata);
            fail("Exception expected"); //$NON-NLS-1$
        } catch(QueryResolverException e) {
            assertEquals("Symbol e1 is specified with an unknown group context", e.getMessage()); //$NON-NLS-1$
        } 
    }
    
    @Test public void testSameNameRoot() {
        String sql = "select p.e1 from pm1.g1 as pp, pm1.g1 as p"; //$NON-NLS-1$
        
        helpResolve(sql);
    }
        
    @Test public void testBatchedUpdateResolver() throws Exception {
        String update1 = "update pm1.g1 set e1 =1"; //$NON-NLS-1$
        String update2 = "update pm2.g1 set e1 =1"; //$NON-NLS-1$
        
        List commands = new ArrayList();
        commands.add(QueryParser.getQueryParser().parseCommand(update1));
        commands.add(QueryParser.getQueryParser().parseCommand(update2));
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        
        helpResolve(command);
    }
    
    @Test public void testAmbiguousAllInGroup() {
        String sql = "SELECT g1.* from pm1.g1, pm2.g1"; //$NON-NLS-1$
        helpResolveException(sql, metadata, "The symbol g1.* refers to more than one group defined in the FROM clause."); //$NON-NLS-1$
    }
    
    @Test public void testRowsUpdatedInProcedure(){
        String sql = "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
            + "BEGIN " //$NON-NLS-1$
            +"SELECT ROWS_UPDATED; " //$NON-NLS-1$
            +"end "; //$NON-NLS-1$
        
        helpResolveException(sql, metadata, "Element \"ROWS_UPDATED\" is not defined by any relevant group."); //$NON-NLS-1$
    }
    
    @Test public void testXMLQueryWithVariable() {
        String sql = "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
            + "BEGIN " //$NON-NLS-1$
            + "declare string x = '1'; " //$NON-NLS-1$
            +"select * from xmltest.doc1 where node1 = x; " //$NON-NLS-1$
            +"end "; //$NON-NLS-1$

        CreateUpdateProcedureCommand command = (CreateUpdateProcedureCommand) helpResolve(sql); 
        
        CommandStatement cmdStmt = (CommandStatement)command.getBlock().getStatements().get(1);
        
        CompareCriteria criteria = (CompareCriteria)((Query)cmdStmt.getCommand()).getCriteria();
        
        assertEquals(ProcedureReservedWords.VARIABLES, ((ElementSymbol)criteria.getRightExpression()).getGroupSymbol().getCanonicalName());
    }
    
    /**
     *  We could check to see if the expressions are evaluatable to a constant, but that seems unnecessary
     */
    @Test public void testLookupWithoutConstant() throws Exception{
        String sql = "SELECT lookup('pm1.g1', convert('e3', float), 'e2', e2) FROM pm1.g1"; //$NON-NLS-1$
        
        helpResolveException(sql, metadata, "Error Code:ERR.015.008.0063 Message:The first three arguments for the LOOKUP function must be specified as constants."); //$NON-NLS-1$
    }
    
    /**
     * We cannot implicitly convert the argument to double due to lack of precision
     */
    @Test public void testPowerWithBigInteger_Fails() throws Exception {
        String sql = "SELECT power(10, 999999999999999999999999999999999999999999999)"; //$NON-NLS-1$
        
        helpResolveException(sql);
    }
    
    @Test public void testPowerWithLong_Fails() throws Exception {
        String sql = "SELECT power(10, 999999999999)"; //$NON-NLS-1$
        
        helpResolveException(sql);
    }
    
    @Test public void testUpdateError() {
        String userUpdateStr = "UPDATE vm1.g2 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveException(userUpdateStr, metadata, "Error Code:ERR.015.008.0009 Message:Update is not allowed on the view vm1.g2: a procedure must be defined to handle the Update."); //$NON-NLS-1$
    }
    
    @Test public void testInsertError() {
        String userUpdateStr = "INSERT into vm1.g2 (e1) values ('x')"; //$NON-NLS-1$
        
        helpResolveException(userUpdateStr, metadata, "Error Code:ERR.015.008.0009 Message:Insert is not allowed on the view vm1.g2: a procedure must be defined to handle the Insert."); //$NON-NLS-1$
    }
    
    @Test public void testDeleteError() {
        String userUpdateStr = "DELETE from vm1.g2 where e1='x'"; //$NON-NLS-1$
        
        helpResolveException(userUpdateStr, metadata, "Error Code:ERR.015.008.0009 Message:Delete is not allowed on the view vm1.g2: a procedure must be defined to handle the Delete."); //$NON-NLS-1$
    }
                
    @Test public void testResolveXMLSelect() {
        String procedure = "CREATE VIRTUAL PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.X = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "select VARIABLES.X from xmltest.doc1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        helpResolveException(procedure, "Error Code:ERR.015.008.0019 Message:Unable to resolve element: VARIABLES.X"); //$NON-NLS-1$
    }
    
    @Test public void testXMLJoinFail() {
        String query = "select * from xmltest.doc1, xmltest.doc2"; //$NON-NLS-1$
         
        helpResolveException(query, "Error Code:ERR.015.008.0003 Message:Only one XML document may be specified in the FROM clause of a query."); //$NON-NLS-1$
    }
    
    @Test public void testExecProjectedSymbols() {
        String query = "exec pm1.sq1()"; //$NON-NLS-1$
         
        StoredProcedure proc = (StoredProcedure)helpResolve(query); 
        
        List projected = proc.getProjectedSymbols();
        
        assertEquals(2, projected.size());
        
        for (Iterator i = projected.iterator(); i.hasNext();) {
            ElementSymbol symbol = (ElementSymbol)i.next();
            assertNotNull(symbol.getGroupSymbol());
        }
    }
    
    @Test public void testExecWithDuplicateNames() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataStore store = metadata.getStore();
        
        FakeMetadataObject pm1 = store.findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs2", pm1, new String[] { "in", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq2 = FakeMetadataFactory.createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        store.addObject(rs2);
        store.addObject(sq2);
        
        helpResolveException("select * from pm1.sq2", metadata, "Cannot access procedure pm1.sq2 using table semantics since the parameter and result set column names are not all unique."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testInlineViewNullLiteralInUnion() {
        String sql = "select e2 from pm1.g1 union all (select x from (select null as x) y)"; //$NON-NLS-1$
        
        helpResolve(sql);
    }
    
    @Test public void testSelectIntoWithDuplicateNames() {
        String sql = "select 1 as x, 2 as x into #temp"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot create group '#temp' with multiple columns named 'x'"); //$NON-NLS-1$
    }
    
    @Test public void testCreateWithDuplicateNames() {
        String sql = "CREATE LOCAL TEMPORARY TABLE temp_table (column1 string, column1 string)"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot create group \'temp_table\' with multiple columns named \'column1\'"); //$NON-NLS-1$
    }
    
    @Test public void testXMLQuery4() {
        helpResolveException("SELECT * FROM xmltest.doc1 group by a2", "Queries against XML documents can not have a GROUP By clause"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testXMLQuery5() {
        helpResolveException("SELECT * FROM xmltest.doc1 having a2='x'", "Queries against XML documents can not have a HAVING clause"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testSelectIntoWithOrderBy() {
        String sql = "select e1, e2 into #temp from pm1.g1 order by e1 limit 10"; //$NON-NLS-1$
        
        helpResolve(sql);
    }
    
    @Test public void testUnionBranchesWithDifferentElementCounts() {
        helpResolveException("SELECT e2, e3 FROM pm1.g1 UNION SELECT e2 FROM pm1.g2","Queries combined with the set operator UNION must have the same number of output elements."); //$NON-NLS-1$ //$NON-NLS-2$
        helpResolveException("SELECT e2 FROM pm1.g1 UNION SELECT e2, e3 FROM pm1.g2","Queries combined with the set operator UNION must have the same number of output elements."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testSelectIntoWithNullLiteral() {
        String sql = "select null as x into #temp from pm1.g1"; //$NON-NLS-1$
        
        Query query = (Query)helpResolve(sql);
        
        TempMetadataStore store = new TempMetadataStore(query.getTemporaryMetadata());
        
        TempMetadataID id = store.getTempElementID("#temp.x"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, id.getType());
    }
    
    @Test public void testInsertWithNullLiteral() {
        String sql = "insert into #temp (x) values (null)"; //$NON-NLS-1$
        
        Insert insert = (Insert)helpResolve(sql);
        
        TempMetadataStore store = new TempMetadataStore(insert.getTemporaryMetadata());
        
        TempMetadataID id = store.getTempElementID("#temp.x"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, id.getType());
    }
    
    @Test public void testInsertWithoutColumnsFails() {
        String sql = "Insert into pm1.g1 values (1, 2)"; //$NON-NLS-1$
        
        helpResolveException(sql, "Error Code:ERR.015.008.0010 Message:INSERT statement must have the same number of elements and values specified.  This statement has 4 elements and 2 values."); //$NON-NLS-1$
    }
    
    @Test public void testInsertWithoutColumnsFails1() {
        String sql = "Insert into pm1.g1 values (1, 2, 3, 4)"; //$NON-NLS-1$
        
        helpResolveException(sql, "Error Code:ERR.015.008.0041 Message:Expected value of type 'boolean' but '3' is of type 'integer' and no implicit conversion is available."); //$NON-NLS-1$
    }
    
    @Test public void testInsertWithQueryFails() {
        String sql = "Insert into pm1.g1 select 1, 2, 3, 4"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot convert insert query expression projected symbol '3' of type java.lang.Integer to insert column 'pm1.g1.e3' of type java.lang.Boolean"); //$NON-NLS-1$
    }
    
    @Test public void testInsertWithQueryImplicitWithoutColumns() {
        String sql = "Insert into #X select 1 as x, 2 as y, 3 as z"; //$NON-NLS-1$
        helpResolve(sql); //$NON-NLS-1$
    }
    
    @Test public void testInsertWithQueryImplicitWithoutColumns1() {
        String sql = "Insert into #X select 1 as x, 2 as y, 3 as y"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot create group '#X' with multiple columns named 'y'"); //$NON-NLS-1$
    }

    @Test public void testInsertWithoutColumnsPasses() {
        String sql = "Insert into pm1.g1 values (1, 2, true, 4)"; //$NON-NLS-1$
        
        helpResolve(sql);
        Insert command = (Insert)helpResolve(sql);
        assertEquals(4, command.getVariables().size());
    }

    @Test public void testInsertWithoutColumnsUndefinedTemp() {
        String sql = "Insert into #temp values (1, 2)"; //$NON-NLS-1$

        Insert command = (Insert)helpResolve(sql);
        assertEquals(2, command.getVariables().size());
    }
    
    @Test public void testImplicitTempInsertWithNoColumns() {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table #matt (x integer);") //$NON-NLS-1$
        .append("\n  insert into #matt values (1);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        Command cmd = helpResolve(proc.toString()); 

        String sExpected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nCREATE LOCAL TEMPORARY TABLE #matt (x integer);\nINSERT INTO #matt (#MATT.x) VALUES (1);\nEND\n\tCREATE LOCAL TEMPORARY TABLE #matt (x integer)\n\tINSERT INTO #matt (#MATT.x) VALUES (1)\n";   //$NON-NLS-1$
        String sActual = cmd.printCommandTree(); 
        assertEquals( sExpected, sActual );
    }

    @Test public void testCase6319() throws QueryResolverException, TeiidComponentException {
        String sql = "select floatnum from bqt1.smalla group by floatnum having sum(floatnum) between 51.0 and 100.0 "; //$NON-NLS-1$
        Query query = (Query)helpParse(sql);
        QueryResolver.resolveCommand(query, FakeMetadataFactory.exampleBQTCached());
    }

    @Test public void testUniqeNamesWithInlineView() {
        helpResolveException("select * from (select count(intNum) a, count(stringKey) b, bqt1.smalla.intkey as b from bqt1.smalla group by bqt1.smalla.intkey) q1 order by q1.a", FakeMetadataFactory.exampleBQTCached(), "Cannot create group 'q1' with multiple columns named 'b'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
            
    @Test public void testResolveOldProcRelational() {
        helpResolveException("SELECT * FROM pm1.g1, (exec pm1.sq2(pm1.g1.e1)) as a", "Symbol pm1.g1.e1 is specified with an unknown group context"); //$NON-NLS-1$  //$NON-NLS-2$
    }
    
    @Test public void testResolverOrderOfPrecedence() {
        helpResolveException("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 CROSS JOIN (pm1.g2 LEFT OUTER JOIN pm2.g1 on pm1.g1.e1 = pm2.g1.e1)", "Symbol pm1.g1.e1 is specified with an unknown group context"); //$NON-NLS-1$  //$NON-NLS-2$
    }
    
    /**
     * The cross join should parse/resolve with higher precedence
     */
    @Test public void testResolverOrderOfPrecedence_1() {
        helpResolve("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 CROSS JOIN pm1.g2 LEFT OUTER JOIN pm2.g1 on pm1.g1.e1 = pm2.g1.e1"); //$NON-NLS-1$ 
    }
    
    @Test public void testInvalidColumnReferenceWithNestedJoin() {
    	helpResolveException("SELECT a.* FROM (pm1.g2 a left outer join pm1.g2 b on a.e1= b.e1) LEFT OUTER JOIN (select a.e1) c on (a.e1 = c.e1)"); //$NON-NLS-1$ 
    }
    
    /**
     * should be the same as exec with too many params
     */
	@Test public void testCallableStatementTooManyParameters() throws Exception {
		String sql = "{call pm4.spTest9(?, ?)}"; //$NON-NLS-1$
		
		TestResolver.helpResolveException(sql, FakeMetadataFactory.exampleBQTCached(), "Error Code:ERR.015.008.0007 Message:Incorrect number of parameters specified on the stored procedure pm4.spTest9 - expected 1 but got 2"); //$NON-NLS-1$
	}	
	    
    @Test public void testUpdateSetClauseReferenceType() {
    	String sql = "UPDATE pm1.g1 SET pm1.g1.e1 = 1, pm1.g1.e2 = ?;"; //$NON-NLS-1$
    	
    	Update update = (Update)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	
    	Expression ref = update.getChangeList().getClauses().get(1).getValue();
    	assertTrue(ref instanceof Reference);
    	assertNotNull(ref.getType());
    }
    
    @Test public void testNoTypeCriteria() {
    	String sql = "select * from pm1.g1 where ? = ?"; //$NON-NLS-1$
    	
    	helpResolveException(sql, FakeMetadataFactory.example1Cached(), "Error Code:ERR.015.008.0026 Message:Expression '? = ?' has a parameter with non-determinable type information.  The use of an explicit convert may be necessary."); //$NON-NLS-1$
    }
    
    @Test public void testReferenceInSelect() {
    	String sql = "select ?, e1 from pm1.g1"; //$NON-NLS-1$
    	Query command = (Query)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	assertEquals(DataTypeManager.DefaultDataClasses.STRING, ((SingleElementSymbol)command.getProjectedSymbols().get(0)).getType());
    }
    
    @Test public void testReferenceInSelect1() {
    	String sql = "select convert(?, integer), e1 from pm1.g1"; //$NON-NLS-1$
    	
    	Query command = (Query)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((SingleElementSymbol)command.getProjectedSymbols().get(0)).getType());
    }
    
    @Test public void testUnionWithObjectTypeConversion() {
    	String sql = "select convert(null, xml) from pm1.g1 union all select 1"; //$NON-NLS-1$
    	
    	SetQuery query = (SetQuery)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	assertEquals(DataTypeManager.DefaultDataClasses.OBJECT, ((SingleElementSymbol)query.getProjectedSymbols().get(0)).getType());
    }
    
    @Test public void testUnionWithSubQuery() {
    	String sql = "select 1 from pm1.g1 where exists (select 1) union select 2"; //$NON-NLS-1$

        SetQuery command = (SetQuery)helpResolve(sql);
        
        assertEquals(1, command.getSubCommands().size());
    }
    @Test public void testOrderBy_J658a() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2, e3 as x, (5+2) as y FROM pm1.g1 ORDER BY e3"); //$NON-NLS-1$
        OrderBy orderBy = resolvedQuery.getOrderBy();
        int[] expectedPositions = new int[] {2};
        helpTestOrderBy(orderBy, expectedPositions);
    }

	private void helpTestOrderBy(OrderBy orderBy, int[] expectedPositions) {
		assertEquals(expectedPositions.length, orderBy.getVariableCount());
        for (int i = 0; i < expectedPositions.length; i++) {
        	assertEquals(expectedPositions[i], orderBy.getExpressionPosition(i));
        }
	}
    @Test public void testOrderBy_J658b() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2, e3 as x, (5+2) as y FROM pm1.g1 ORDER BY e2, e3 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});
    }
    @Test public void testOrderBy_J658c() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as y FROM pm1.g1 ORDER BY x, e3 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});    
    }
    
    // ambiguous, should fail
    @Test public void testOrderBy_J658d() {
        helpResolveException("SELECT pm1.g1.e1, e2 as x, e3 as x FROM pm1.g1 ORDER BY x, e1 ", "Error Code:ERR.015.008.0042 Message:Element 'x' in ORDER BY is ambiguous and may refer to more than one element of SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    @Test public void testOrderBy_J658e() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as e2 FROM pm1.g1 ORDER BY x, e2 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});
    }
    
    @Test public void testSPOutParamWithExec() {
    	StoredProcedure proc = (StoredProcedure)helpResolve("exec pm2.spTest8(1)", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(2, proc.getProjectedSymbols().size());
    }

    /**
     * Note that the call syntax is not quite correct, the output parameter is not in the arg list.
     * That hack is handled by the PreparedStatementRequest
     */
    @Test public void testSPOutParamWithCallableStatement() {
    	StoredProcedure proc = (StoredProcedure)helpResolve("{call pm2.spTest8(1)}", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(3, proc.getProjectedSymbols().size());
    }
    
    @Test public void testOutWithWrongType() {
    	helpResolveException("exec pm2.spTest8(inkey=>1, outkey=>{t '12:00:00'})", FakeMetadataFactory.exampleBQTCached());
    }
    
    @Test public void testProcRelationalWithOutParam() {
    	Query proc = (Query)helpResolve("select * from pm2.spTest8 where inkey = 1", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(3, proc.getProjectedSymbols().size());
    }
    
    @Test public void testSPReturnParamWithNoResultSet() {
    	StoredProcedure proc = (StoredProcedure)helpResolve("exec pm4.spTest9(1)", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(1, proc.getProjectedSymbols().size());
    }
    
    @Test public void testSecondPassFunctionResolving() {
    	helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 where lower(?) = e1 "); //$NON-NLS-1$
    }
    
    @Test public void testSecondPassFunctionResolving1() {
    	try {
    		helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 where 1/(e1 - 2) <> 4 "); //$NON-NLS-1$
    		fail("expected exception");
    	} catch (RuntimeException e) {
    		QueryResolverException qre = (QueryResolverException)e.getCause();
    		assertEquals("ERR.015.008.0040", qre.getCode());
    	}
    }
    
    @Ignore("currently not supported - we get type hints from the criteria not from the possible signatures")
    @Test public void testSecondPassFunctionResolving2() {
    	helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 where (lower(?) || 1) = e1 "); //$NON-NLS-1$
    }

    /**
     * Test <code>QueryResolver</code>'s ability to resolve a query that 
     * contains an aggregate <code>SUM</code> which uses a <code>CASE</code> 
     * expression which contains <code>BETWEEN</code> criteria as its value.
     * <p>
     * For example:
     * <p>
     * SELECT SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1
     */
    @Test public void testAggregateWithBetweenInCaseInSelect() {
    	String sql = "SELECT SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1"; //$NON-NLS-1$
    	helpResolve(sql);
    }
    
    /**
     * Test <code>QueryResolver</code>'s ability to resolve a query that 
     * contains a <code>CASE</code> expression which contains 
     * <code>BETWEEN</code> criteria in the queries <code>SELECT</code> clause.
     * <p>
     * For example:
     * <p>
     * SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1
     */
    @Test public void testBetweenInCaseInSelect() {
    	String sql = "SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1"; //$NON-NLS-1$
    	helpResolve(sql);
    }
    
    /**
     * Test <code>QueryResolver</code>'s ability to resolve a query that 
     * contains a <code>CASE</code> expression which contains 
     * <code>BETWEEN</code> criteria in the queries <code>WHERE</code> clause.
     * <p>
     * For example:
     * <p>
     * SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END
     */
    @Test public void testBetweenInCase() {
    	String sql = "SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END"; //$NON-NLS-1$
    	helpResolve(sql);
    }
    
    @Test public void testOrderByUnrelated() {
        helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as y FROM pm1.g1 ORDER BY e4"); //$NON-NLS-1$
    }

    @Test public void testOrderByUnrelated1() {
        helpResolveException("SELECT distinct pm1.g1.e1, e2 as x, e3 as y FROM pm1.g1 ORDER BY e4"); //$NON-NLS-1$
    }

    @Test public void testOrderByUnrelated2() {
        helpResolveException("SELECT max(e2) FROM pm1.g1 group by e1 ORDER BY e4"); //$NON-NLS-1$
    }
    
    @Test public void testOrderByExpression() {
    	Query query = (Query)helpResolve("select pm1.g1.e1 from pm1.g1 order by e2 || e3 "); //$NON-NLS-1$
    	assertEquals(-1, query.getOrderBy().getExpressionPosition(0));
    }
    
    @Test public void testOrderByExpression1() {
    	Query query = (Query)helpResolve("select pm1.g1.e1 || e2 from pm1.g1 order by pm1.g1.e1 || e2 "); //$NON-NLS-1$
    	assertEquals(0, query.getOrderBy().getExpressionPosition(0));
    }
    
    @Test public void testOrderByExpression2() {
    	helpResolveException("select pm1.g1.e1 from pm1.g1 union select pm1.g2.e1 from pm1.g2 order by pm1.g1.e1 || 2", "ORDER BY expression '(pm1.g1.e1 || 2)' cannot be used with a set query."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOrderByConstantFails() {
    	helpResolveException("select pm1.g1.e1 from pm1.g1 order by 2"); //$NON-NLS-1$
    }
    
    @Test public void testCorrelatedNestedTableReference() {
    	helpResolve("select pm1.g1.e1 from pm1.g1, table (exec pm1.sq2(pm1.g1.e2)) x"); //$NON-NLS-1$
    	helpResolveException("select pm1.g1.e1 from pm1.g1, (exec pm1.sq2(pm1.g1.e2)) x"); //$NON-NLS-1$
    }
    
    @Test public void testCorrelatedTextTable() {
    	Command command = helpResolve("select x.* from pm1.g1, texttable(e1 COLUMNS x string) x"); //$NON-NLS-1$
    	assertEquals(1, command.getProjectedSymbols().size());
    }
    
    @Test public void testQueryString() throws Exception {
    	helpResolveException("select querystring(xmlparse(document '<a/>'))");
    }
    
	// validating AssignmentStatement, ROWS_UPDATED element assigned
    @Test(expected=QueryResolverException.class) public void testCreateUpdateProcedure9() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(procedure, userUpdateStr);
    }

	CreateUpdateProcedureCommand helpResolveUpdateProcedure(String procedure,
			String userUpdateStr) throws QueryParserException,
			QueryResolverException, TeiidComponentException,
			QueryMetadataException {
		FakeMetadataFacade metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);

        ProcedureContainer userCommand = (ProcedureContainer)QueryParser.getQueryParser().parseCommand(userUpdateStr);
        QueryResolver.resolveCommand(userCommand, metadata);
        
        return (CreateUpdateProcedureCommand)QueryResolver.expandCommand(userCommand, metadata, AnalysisRecord.createNonRecordingRecord());
	}
    
	// validating AssignmentStatement, variable type and assigned type 
	// do not match
    @Test(expected=QueryResolverException.class) public void testCreateUpdateProcedure10() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr);
    }
    
    //return should be first, then out
    @Test public void testParamOrder() {
        Query resolvedQuery = (Query)helpResolve("SELECT * FROM (exec pm4.spRetOut()) as a", RealMetadataFactory.exampleBQTCached(), null); //$NON-NLS-1$
        
        assertEquals("A.ret", resolvedQuery.getProjectedSymbols().get(0).getName());
    }
    
    @Test public void testOrderByAggregatesError() throws Exception {
    	helpResolveException("select count(*) from pm1.g1 order by e1");
    }
    
    @Test public void testWithDuplidateName() {
    	helpResolveException("with x as (TABLE pm1.g1), x as (TABLE pm1.g2) SELECT * from x");
    }
    
    @Test public void testWithColumns() {
    	helpResolveException("with x (a, b) as (TABLE pm1.g1) SELECT * from x");
    }
    
    @Test public void testWithNameMatchesFrom() {
    	helpResolve("with x as (TABLE pm1.g1) SELECT * from (TABLE x) x");
    }
    
	// variables cannot be used among insert elements
    @Test(expected=QueryResolverException.class) public void testCreateUpdateProcedure23() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Update pm1.g1 SET pm1.g1.e2 =1 , var1 = 2;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userQuery);
	}
}