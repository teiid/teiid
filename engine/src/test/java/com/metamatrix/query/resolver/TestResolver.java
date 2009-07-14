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

package com.metamatrix.query.resolver;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.analysis.QueryAnnotation;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.util.BindVariableVisitor;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SelectSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.FunctionCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.sql.visitor.VariableCollectorVisitor;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;
import com.metamatrix.query.unittest.TimestampUtil;

public class TestResolver extends TestCase {

	private FakeMetadataFacade metadata;

	// ################################## FRAMEWORK ################################
	
	public TestResolver(String name) { 
		super(name);
	}	

	public void setUp() {
		metadata = FakeMetadataFactory.example1Cached();
	}

	// ################################## TEST HELPERS ################################

	static Command helpParse(String sql) { 
        try { 
            return QueryParser.getQueryParser().parseCommand(sql);
        } catch(MetaMatrixException e) { 
            throw new RuntimeException(e);
        }
	}
    
    public static Map getProcedureExternalMetadata(GroupSymbol virtualGroup, QueryMetadataInterface metadata)
    throws QueryMetadataException, MetaMatrixComponentException {
        Map externalMetadata = new HashMap();

        // Look up elements for the virtual group
        List elements = ResolverUtil.resolveElementsInGroup(virtualGroup, metadata);
        // virtual group metadata info
        externalMetadata.put(virtualGroup, elements);

        // INPUT group metadata info
        GroupSymbol inputGroup = new GroupSymbol(ProcedureReservedWords.INPUT);
        List inputElments = new ArrayList(elements.size());
        List elementIds = new ArrayList();
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol virtualElmnt = (ElementSymbol)elements.get(i);
            ElementSymbol inputElement = (ElementSymbol)virtualElmnt.clone();
            inputElments.add(inputElement);
            elementIds.add(new TempMetadataID(ProcedureReservedWords.INPUT + ElementSymbol.SEPARATOR + virtualElmnt.getShortName(), virtualElmnt.getType()));
        }
        inputGroup.setMetadataID(new TempMetadataID(ProcedureReservedWords.INPUT, elementIds));
        externalMetadata.put(inputGroup, inputElments);

        // CHANGING group metadata info
        // Switch type to be boolean for all CHANGING variables
        GroupSymbol changeGroup = new GroupSymbol(ProcedureReservedWords.CHANGING);
        List changingElments = new ArrayList(elements.size());
        elementIds = new ArrayList();
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol changeElement = (ElementSymbol)((ElementSymbol)elements.get(i)).clone();
            changeElement.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            changingElments.add(changeElement);
            elementIds.add(new TempMetadataID(ProcedureReservedWords.INPUT + ElementSymbol.SEPARATOR + changeElement.getShortName(), changeElement.getType()));
        }
        changeGroup.setMetadataID(new TempMetadataID(ProcedureReservedWords.CHANGING, elementIds));
        externalMetadata.put(changeGroup, changingElments);

        return externalMetadata;
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
        Collection variables = new HashSet();
        VariableCollectorVisitor visitor = new VariableCollectorVisitor(variables);
        DeepPreOrderNavigator.doVisit(query, visitor);

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
    
    public static Command helpResolve(String sql, QueryMetadataInterface queryMetadata, AnalysisRecord analysis){
        return helpResolve(helpParse(sql), queryMetadata, analysis);
    }
    
	private Command helpResolve(String sql) { 
		return helpResolve(helpParse(sql));
	}
	
	private Command helpResolveUpdateProcedure(String procedure, String userUpdateStr, String procedureType) {
        metadata = FakeMetadataFactory.exampleUpdateProc(procedureType, procedure);

        return helpResolve(userUpdateStr, metadata, AnalysisRecord.createNonRecordingRecord());
    }
	
    private void helpFailUpdateProcedure(String procedure, String userUpdateStr, String procedureType) {
        helpFailUpdateProcedure(procedure, userUpdateStr, procedureType, null);
    }
    
	private void helpFailUpdateProcedure(String procedure, String userUpdateStr, String procedureType, String msg) {
        metadata = FakeMetadataFactory.exampleUpdateProc(procedureType, procedure);

        Command userCommand;
		try {
			userCommand = QueryParser.getQueryParser().parseCommand(userUpdateStr);
		} catch (QueryParserException e) {
			throw new RuntimeException(e);
		}

        // resolve
        try {
            QueryResolver.resolveCommand(userCommand, metadata);
            fail("Expected a QueryResolverException but got none."); //$NON-NLS-1$
        } catch(QueryResolverException ex) {
        	if (msg != null) {
                assertEquals(msg, ex.getMessage());
            }
        } catch (MetaMatrixComponentException e) {
        	throw new RuntimeException(e);
		} 
	}	

    private Command helpResolve(Command command) {    
        return helpResolve(command, this.metadata, AnalysisRecord.createNonRecordingRecord());  
    }	

	static Command helpResolve(Command command, QueryMetadataInterface queryMetadataInterface, AnalysisRecord analysis) {		
        // resolve
        try { 
            QueryResolver.resolveCommand(command, queryMetadataInterface, analysis);
        } catch(MetaMatrixException e) {
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
		} catch(MetaMatrixException e) {
			fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		assertNotNull("Expected a QueryResolverException but got none.", exception); //$NON-NLS-1$
	}

    private Criteria helpResolveCriteria(String sql) { 
        Criteria criteria = null;
        
        // parse
        try { 
            criteria = QueryParser.getQueryParser().parseCriteria(sql);
           
        } catch(MetaMatrixException e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
        }   
   
   		// resolve
        try { 
            QueryResolver.resolveCriteria(criteria, metadata);
        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 

        CheckSymbolsAreResolvedVisitor vis = new CheckSymbolsAreResolvedVisitor();
        DeepPreOrderNavigator.doVisit(criteria, vis);
        Collection unresolvedSymbols = vis.getUnresolvedSymbols();
        assertTrue("Found unresolved symbols: " + unresolvedSymbols, unresolvedSymbols.isEmpty()); //$NON-NLS-1$
        return criteria;
    }
    
    private Command helpResolve(String sql, List bindings) { 
       
        // parse
        Command command = helpParse(sql);
        
        // apply bindings
        if(bindings != null) {
            try { 
                BindVariableVisitor.bindReferences(command, bindings, metadata);
            } catch(MetaMatrixException e) { 
                fail("Exception during binding (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
            }   
        }
        
        // resolve
        try { 
            QueryResolver.resolveCommand(command, metadata);
        } catch(MetaMatrixException e) { 
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 

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
        } catch(MetaMatrixComponentException e) {
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
    
    private void helpTestIsXMLQuery(String sql, boolean isXML) throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {
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
    private void helpResolveExec(String sql, Object[] expectedParameterExpressions) {

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
    }
        
    
	// ################################## ACTUAL TESTS ################################
	
	
	public void testElementSymbolForms() {
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 AS a, e4 AS b FROM pm1.g1"; //$NON-NLS-1$
		Query resolvedQuery = (Query) helpResolve(sql);
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "pm1.g1.e2", "a", "b" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
	}

	public void testElementSymbolFormsWithAliasedGroup() {
        String sql = "SELECT x.e1, e2, x.e3 AS a, e4 AS b FROM pm1.g1 AS x"; //$NON-NLS-1$
		Query resolvedQuery = (Query) helpResolve(sql);
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x.e1", "x.e2", "a", "b" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
	}

    public void testGroupWithVDB() {
        String sql = "SELECT e1 FROM myvdb.pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
    }

    public void testAliasedGroupWithVDB() {
        String sql = "SELECT e1 FROM myvdb.pm1.g1 AS x"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString());         //$NON-NLS-1$
    }
    
    public void testPartiallyQualifiedGroup1() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }    
    
    public void testPartiallyQualifiedGroup2() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.g2" }); //$NON-NLS-1$
    }
    
    public void testPartiallyQualifiedGroup3() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }
    
    public void testPartiallyQualifiedGroup4() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.cat2.g2" }); //$NON-NLS-1$
    }
    
    public void testPartiallyQualifiedGroup5() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.g3"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat2.g3" }); //$NON-NLS-1$
    }    
    
    public void testPartiallyQualifiedGroup6() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.cat1.g1" }); //$NON-NLS-1$
    }    
    
    public void testPartiallyQualifiedGroup7() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM g4"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.g4" }); //$NON-NLS-1$
    }    
    
    public void testPartiallyQualifiedGroup8() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT e1 FROM pm2.g3"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.g3" }); //$NON-NLS-1$
    }
    
    public void testPartiallyQualifiedGroupWithAlias() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT X.e1 FROM cat2.cat3.g1 as X"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    } 
    
    public void testPartiallyQualifiedElement1() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat2.cat3.g1.e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }

    /** defect 12536 */
    public void testPartiallyQualifiedElement2() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }
    
    /** defect 12536 */
    public void testPartiallyQualifiedElement3() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1 FROM cat2.cat3.g1, cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }
    
    /** defect 12536 */
    public void testPartiallyQualifiedElement4() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat1.g2.e1 FROM cat2.cat3.g1, cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.g2.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    public void testPartiallyQualifiedElement5() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat1.g2.e1 FROM myvdb.pm1.cat1.cat2.cat3.g1, pm1.cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.g2.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    /** defect 12536 */
    public void testPartiallyQualifiedElement6() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, e2 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
	    helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.cat2.cat3.g1.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    public void testPartiallyQualifiedElement7() {
    	metadata = FakeMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat2.cat3.g1.e2, g1.e3 FROM pm1.cat1.cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.cat2.cat3.g1.e2", "pm1.cat1.cat2.cat3.g1.e3" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    } 
    
    public void testFailPartiallyQualifiedGroup1() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM cat3.g1"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedGroup2() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g1"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedGroup3() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g2"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedGroup4() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g3"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedGroup5() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT e1 FROM g5");		 //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedElement1() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM pm1.cat1.cat2.cat3.g1, pm2.cat3.g1"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedElement2() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT g1.e1 FROM pm1.cat1.cat2.cat3.g1, pm2.cat3.g1"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedElement3() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM pm2.cat2.g2, pm1.cat2.g3"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedElement4() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM pm2.cat2.g2"); //$NON-NLS-1$
    }
    
    public void testFailPartiallyQualifiedElement5() {
    	metadata = FakeMetadataFactory.example3();
		helpResolveException("SELECT cat3.g1.e1 FROM g1"); //$NON-NLS-1$
    }    

    public void testElementWithVDB() {
        String sql = "SELECT myvdb.pm1.g1.e1 FROM pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
    }

    public void testAliasedElementWithVDB() {
        Query resolvedQuery = (Query) helpResolve("SELECT myvdb.pm1.g1.e1 AS x FROM pm1.g1"); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
    }

	public void testSelectStar() {
		Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testSelectStarFromAliasedGroup() {
		Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1 as x"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testSelectStarFromMultipleAliasedGroups() {
		Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1 as x, pm1.g1 as y"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1", "pm1.g1" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4", "y.e1", "y.e2", "y.e3", "y.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4", "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
	}

    public void testSelectStarWhereSomeElementsAreNotSelectable() {
        Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g4"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g4" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "*" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g4.e1", "pm1.g4.e3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "pm1.g4.e1", "pm1.g4.e3" } ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelectGroupStarWhereSomeElementsAreNotSelectable() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g4.* FROM pm1.g4"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g4" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g4.*" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g4.e1", "pm1.g4.e3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "pm1.g4.e1", "pm1.g4.e3" } ); //$NON-NLS-1$ //$NON-NLS-2$
    }

	public void testFullyQualifiedSelectStar() {
		Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.* FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testSelectAllInAliasedGroup() {
		Query resolvedQuery = (Query) helpResolve("SELECT x.* FROM pm1.g1 as x"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x.*" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testSelectExpressions() {
		Query resolvedQuery = (Query) helpResolve("SELECT e1, concat(e1, 's'), concat(e1, 's') as c FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "expr", "c" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		helpCheckElements(resolvedQuery.getSelect(),
			new String[] { "pm1.g1.e1", "pm1.g1.e1", "pm1.g1.e1" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new String[] { "pm1.g1.e1", "pm1.g1.e1", "pm1.g1.e1" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testSelectCountStar() {
		Query resolvedQuery = (Query) helpResolve("SELECT count(*) FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "count" }); //$NON-NLS-1$
		helpCheckElements(resolvedQuery.getSelect(), new String[] { }, new String[] { } );
	}
	
	public void testMultipleIdenticalElements() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1, e1 FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e1" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMultipleIdenticalElements2() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1, pm1.g1.e1 FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e1" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMultipleIdenticalElements3() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1, e1 as x FROM pm1.g1"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "x" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g1.e1", "pm1.g1.e1" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g1.e1", "pm1.g1.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testDifferentElementsSameName() { 
		Query resolvedQuery = (Query) helpResolve("SELECT e1 as x, e2 as x FROM pm1.g2"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g2" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x", "x" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { "pm1.g2.e1", "pm1.g2.e2" },  //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { "pm1.g2.e1", "pm1.g2.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testDifferentConstantsSameName() { 
		Query resolvedQuery = (Query) helpResolve("SELECT 1 as x, 2 as x FROM pm1.g2"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g2" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "x", "x" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery.getSelect(), 
			new String[] { }, 
			new String[] { });
	}
	
	public void testFailSameGroupsWithSameNames() { 
		helpResolveException("SELECT * FROM pm1.g1 as x, pm1.g1 as x"); //$NON-NLS-1$
	}

	public void testFailDifferentGroupsWithSameNames() { 
		helpResolveException("SELECT * FROM pm1.g1 as x, pm1.g2 as x"); //$NON-NLS-1$
	}

	public void testFailAmbiguousElement() { 
		helpResolveException("SELECT e1 FROM pm1.g1, pm1.g2"); //$NON-NLS-1$
	}

	public void testFailAmbiguousElementAliasedGroup() { 
		helpResolveException("SELECT e1 FROM pm1.g1 as x, pm1.g1"); //$NON-NLS-1$
	}

	public void testFailFullyQualifiedElementUnknownGroup() { 
		helpResolveException("SELECT pm1.g1.e1 FROM pm1.g2"); //$NON-NLS-1$
	}

	public void testFailUnknownGroup() { 
		helpResolveException("SELECT x.e1 FROM x"); //$NON-NLS-1$
	}

	public void testFailUnknownElement() { 
		helpResolveException("SELECT x FROM pm1.g1"); //$NON-NLS-1$
	}

    public void testFailFunctionOfAggregatesInSelect() {        
        helpResolveException("SELECT (SUM(e0) * COUNT(e0)) FROM test.group GROUP BY e0"); //$NON-NLS-1$
    }
	
	/*
	 * per defect 4404 
	 */
	public void testFailGroupNotReferencedByAlias() { 
		helpResolveException("SELECT pm1.g1.x FROM pm1.g1 as H"); //$NON-NLS-1$
	}

	/*
	 * per defect 4404 - this one reproduced the defect,
	 * then succeeded after the fix
	 */
	public void testFailGroupNotReferencedByAliasSelectAll() { 
		helpResolveException("SELECT pm1.g1.* FROM pm1.g1 as H"); //$NON-NLS-1$
	}

	public void testComplicatedQuery() {
		Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e2 as y, pm1.g1.E3 as z, CONVERT(pm1.g1.e1, integer) * 1000 as w  FROM pm1.g1 WHERE e1 <> 'x'"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
		helpCheckSelect(resolvedQuery, new String[] { "y", "z", "w" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		helpCheckElements(resolvedQuery, 
			new String[] { "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e1", "pm1.g1.e1" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e1", "pm1.g1.e1" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	public void testJoinQuery() {
		Query resolvedQuery = (Query) helpResolve("SELECT pm3.g1.e2, pm3.g2.e2 FROM pm3.g1, pm3.g2 WHERE pm3.g1.e2=pm3.g2.e2"); //$NON-NLS-1$
		helpCheckFrom(resolvedQuery, new String[] { "pm3.g1", "pm3.g2" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckSelect(resolvedQuery, new String[] { "pm3.g1.e2", "pm3.g2.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
		helpCheckElements(resolvedQuery, 
			new String[] { "pm3.g1.e2", "pm3.g2.e2", "pm3.g1.e2", "pm3.g2.e2" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { "pm3.g1.e2", "pm3.g2.e2", "pm3.g1.e2", "pm3.g2.e2" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
    public void testHavingRequiringConvertOnAggregate1() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING MAX(e2) > 1.2"); //$NON-NLS-1$
    }

    public void testHavingRequiringConvertOnAggregate2() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING MIN(e2) > 1.2"); //$NON-NLS-1$
    }

    public void testHavingRequiringConvertOnAggregate3() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING 1.2 > MAX(e2)"); //$NON-NLS-1$
    }

    public void testHavingRequiringConvertOnAggregate4() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING 1.2 > MIN(e2)"); //$NON-NLS-1$
    }

    public void testHavingWithAggsOfDifferentTypes() {
        helpResolve("SELECT * FROM pm1.g1 GROUP BY e4 HAVING MIN(e1) = MIN(e2)"); //$NON-NLS-1$
    }
    
    public void testCaseInGroupBy() {
        String sql = "SELECT SUM(e2) FROM pm1.g1 GROUP BY CASE WHEN e2 = 0 THEN 1 ELSE 2 END"; //$NON-NLS-1$
        Command command = helpResolve(sql);
        assertEquals(sql, command.toString());
        
        helpCheckElements(command, new String[] {"pm1.g1.e2", "pm1.g1.e2"}, new String[] {"pm1.g1.e2", "pm1.g1.e2"});  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
    }

    public void testFunctionInGroupBy() {
        String sql = "SELECT SUM(e2) FROM pm1.g1 GROUP BY (e2 + 1)"; //$NON-NLS-1$
        Command command = helpResolve(sql);
        assertEquals(sql, command.toString());
        
        helpCheckElements(command, new String[] {"pm1.g1.e2", "pm1.g1.e2"}, new String[] {"pm1.g1.e2", "pm1.g1.e2"});  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$//$NON-NLS-4$
    }

	public void testUnknownFunction() {	    
		helpResolveException("SELECT abc(e1) FROM pm1.g1", "Error Code:ERR.015.008.0039 Message:The function 'abc(e1)' is an unknown form.  Check that the function name and number of arguments is correct."); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testConversionNotPossible() {	    
		helpResolveException("SELECT dayofmonth('2002-01-01') FROM pm1.g1", "Error Code:ERR.015.008.0040 Message:The function 'dayofmonth('2002-01-01')' is a valid function form, but the arguments do not match a known type signature and cannot be converted using implicit type conversions."); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
    public void testResolveParameters() {
        List bindings = new ArrayList();
        bindings.add("pm1.g2.e1"); //$NON-NLS-1$
        bindings.add("pm1.g2.e2"); //$NON-NLS-1$
        
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, ? FROM pm1.g1 WHERE pm1.g1.e1 = ?", bindings); //$NON-NLS-1$

        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "expr" }); //$NON-NLS-1$ //$NON-NLS-2$
        helpCheckElements(resolvedQuery.getCriteria(), 
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
            
    }

    public void testResolveParametersInsert() {
        List bindings = new ArrayList();
        bindings.add("pm1.g2.e1"); //$NON-NLS-1$
        
        helpResolve("INSERT INTO pm1.g1 (e1) VALUES (?)", bindings); //$NON-NLS-1$
    }
    
    public void testResolveParametersExec() {
        List bindings = new ArrayList();
        bindings.add("pm1.g2.e1"); //$NON-NLS-1$
        
        Query resolvedQuery = (Query)helpResolve("SELECT * FROM (exec pm1.sq2(?)) as a", bindings); //$NON-NLS-1$
        //verify the type of the reference is resolved
        List refs = ReferenceCollectorVisitor.getReferences(resolvedQuery);
        Reference ref = (Reference)refs.get(0);
        assertNotNull(ref.getType());
    }

    public void testUseNonExistentAlias() {
        helpResolveException("SELECT portfoliob.e1 FROM ((pm1.g1 AS portfoliob JOIN pm1.g2 AS portidentb ON portfoliob.e1 = portidentb.e1) RIGHT OUTER JOIN pm1.g3 AS identifiersb ON portidentb.e1 = 'ISIN' and portidentb.e2 = identifiersb.e2) RIGHT OUTER JOIN pm1.g1 AS issuesb ON a.identifiersb.e1 = issuesb.e1"); //$NON-NLS-1$
    }       

    public void testCriteria1() {                  
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
        
    public void testSubquery1() {
        Query resolvedQuery = (Query) helpResolve("SELECT e1 FROM pm1.g1, (SELECT pm1.g2.e1 AS x FROM pm1.g2) AS y WHERE e1 = x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1", "y" }); //$NON-NLS-1$ //$NON-NLS-2$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
        
    }
    
    public void testStoredQuery1() {                
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
	public void testStoredQueryParamOrdering_8211() {                
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
    
    public void testStoredSubQuery1() {
        Query resolvedQuery = (Query) helpResolve("select x.e1 from (EXEC pm1.sq1()) as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x.e1" });         //$NON-NLS-1$
    }
    
    public void testStoredSubQuery2() {
        Query resolvedQuery = (Query) helpResolve("select x.e1 from (EXEC pm1.sq3('abc', 5)) as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x.e1" });         //$NON-NLS-1$
    }

    public void testStoredSubQuery3() {
        Query resolvedQuery = (Query) helpResolve("select * from (EXEC pm1.sq2('abc')) as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        
        List elements = (List) ElementCollectorVisitor.getElements(resolvedQuery.getSelect(), false);
        
        ElementSymbol elem1 = (ElementSymbol)elements.get(0);
        assertEquals("Did not get expected element", "X.E1", elem1.getName() ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Did not get expected type", DataTypeManager.DefaultDataClasses.STRING, elem1.getType()); //$NON-NLS-1$

        ElementSymbol elem2 = (ElementSymbol)elements.get(1);
        assertEquals("Did not get expected element", "X.E2", elem2.getName() ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Did not get expected type", DataTypeManager.DefaultDataClasses.INTEGER, elem2.getType()); //$NON-NLS-1$
    }

    public void testStoredQueryTransformationWithVariable() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("SELECT * FROM pm1.g1 WHERE pm1.sq5.in1 = 5"); //$NON-NLS-1$
        
        // Construct command metadata 
        GroupSymbol sqGroup = new GroupSymbol("pm1.sq5");  //$NON-NLS-1$
        ArrayList sqParams = new ArrayList();
        ElementSymbol in = new ElementSymbol("pm1.sq5.in1"); //$NON-NLS-1$
        in.setType(DataTypeManager.DefaultDataClasses.STRING);
        sqParams.add(in);
        Map externalMetadata = new HashMap();
        externalMetadata.put(sqGroup, sqParams);
        
        QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());

        // Verify results        
        helpCheckFrom((Query)command, new String[] { "pm1.g1" });         //$NON-NLS-1$
        Collection vars = VariableCollectorVisitor.getVariables(command, false);
        assertEquals("Did not find variable in resolved query", 1, vars.size()); //$NON-NLS-1$
    }

    public void testStoredQueryTransformationWithVariable2() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("SELECT * FROM pm1.g1 WHERE in1 = 5"); //$NON-NLS-1$
        
        // Construct command metadata
        GroupSymbol sqGroup = new GroupSymbol("pm1.sq5");  //$NON-NLS-1$
        ArrayList sqParams = new ArrayList();
        ElementSymbol in = new ElementSymbol("pm1.sq5.in1"); //$NON-NLS-1$
        in.setType(DataTypeManager.DefaultDataClasses.STRING);
        sqParams.add(in);
        Map externalMetadata = new HashMap();
        externalMetadata.put(sqGroup, sqParams);
                    
        QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());

        // Verify results        
        helpCheckFrom((Query)command, new String[] { "pm1.g1" });         //$NON-NLS-1$
        Collection vars = VariableCollectorVisitor.getVariables(command, false);
        assertEquals("Did not find variable in resolved query", 1, vars.size()); //$NON-NLS-1$
    }

    public void testStoredQueryTransformationWithVariable3() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("SELECT * FROM pm1.g1 WHERE in1 = 5 UNION SELECT * FROM pm1.g1"); //$NON-NLS-1$

        // Construct command metadata
        GroupSymbol sqGroup = new GroupSymbol("pm1.sq5"); //$NON-NLS-1$
        ArrayList sqParams = new ArrayList();
        ElementSymbol in = new ElementSymbol("pm1.sq5.in1"); //$NON-NLS-1$
        in.setType(DataTypeManager.DefaultDataClasses.STRING);
        sqParams.add(in);
        Map externalMetadata = new HashMap();
        externalMetadata.put(sqGroup, sqParams);

        QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());

        // Verify results
        Collection vars = VariableCollectorVisitor.getVariables(command, false);
        assertEquals("Did not find variable in resolved query", 1, vars.size()); //$NON-NLS-1$
    }
    
    public void testStoredQueryTransformationWithVariable4() throws Exception {
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

            QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());
            
            fail("Expected exception on invalid variable pm1.sq2.in"); //$NON-NLS-1$
        } catch(QueryResolverException e) {
        	assertEquals("Symbol pm1.sq2.\"in\" is specified with an unknown group context", e.getMessage()); //$NON-NLS-1$
        } 
    }

    public void testExec1() {
        helpResolve("EXEC pm1.sq2('xyz')"); //$NON-NLS-1$
    }

    public void testExec2() {
        // implicity convert 5 to proper type
        helpResolve("EXEC pm1.sq2(5)"); //$NON-NLS-1$
    }
    
    public void testExecNamedParam() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz")};//$NON-NLS-1$
        helpResolveExec("EXEC pm1.sq2(\"in\" = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }

    /** Should get exception because param name is wrong. */
    public void testExecWrongParamName() {
        helpResolveException("EXEC pm1.sq2(in1 = 'xyz')");//$NON-NLS-1$
    }

    public void testExecNamedParams() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(new Integer(5))};//$NON-NLS-1$
        helpResolveExec("EXEC pm1.sq3(\"in\" = 'xyz', in2 = 5)", expectedParameterExpressions);//$NON-NLS-1$
    }   
    
    /** try entering params out of order */
    public void testExecNamedParamsReversed() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(new Integer(5))};//$NON-NLS-1$
        helpResolveExec("EXEC pm1.sq3(in2 = 5, \"in\" = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }    
    
    /** test omitting an optional parameter */
    public void testExecNamedParamsOptionalParam() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(null), new Constant("something")};//$NON-NLS-1$ //$NON-NLS-2$
        helpResolveExec("EXEC pm1.sq3b(\"in\" = 'xyz', in3 = 'something')", expectedParameterExpressions);//$NON-NLS-1$
    }    

    /** test omitting a required parameter that has a default value */
    public void testExecNamedParamsOmitRequiredParamWithDefaultValue() {
        Object[] expectedParameterExpressions = new Object[] {new Constant("xyz"), new Constant(new Integer(666)), new Constant("YYZ")};//$NON-NLS-1$ //$NON-NLS-2$
        helpResolveExec("EXEC pm1.sq3b(\"in\" = 'xyz', in2 = 666)", expectedParameterExpressions);//$NON-NLS-1$
    }    
    
    public void testExecNamedParamsOptionalParamWithDefaults() {
        Object[] expectedParameterExpressions = helpGetStoredProcDefaultValues();
        //override the default value for the first parameter
        expectedParameterExpressions[0] = new Constant("xyz"); //$NON-NLS-1$
        helpResolveExec("EXEC pm1.sqDefaults(inString = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }    

    public void testExecNamedParamsOptionalParamWithDefaultsCaseInsensitive() {
        Object[] expectedParameterExpressions = helpGetStoredProcDefaultValues();
        //override the default value for the first parameter
        expectedParameterExpressions[0] = new Constant("xyz"); //$NON-NLS-1$
        helpResolveExec("EXEC pm1.sqDefaults(iNsTrInG = 'xyz')", expectedParameterExpressions);//$NON-NLS-1$
    }    

    /** try just a few named parameters, in no particular order */
    public void testExecNamedParamsOptionalParamWithDefaults2() {
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
    public void testExecNamedParamsOptionalParamWithAllDefaults() {
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
    public void testExceptionNotSupplyingRequiredParam() {
        helpResolveException("EXEC pm1.sq3(in2 = 5)");//$NON-NLS-1$
    }
    
    /** Should get exception because the default value in metadata is bad for input param */
    public void testExceptionBadDefaultValue() {
        helpResolveException("EXEC pm1.sqBadDefault()");//$NON-NLS-1$
    }
    
    public void testExecWithForcedConvertOfStringToCorrectType() {
        // force conversion of '5' to proper type (integer)
        helpResolve("EXEC pm1.sq3('x', '5')"); //$NON-NLS-1$
    }

    /**
     * True/false are consistently representable by integers
     */
    public void testExecBadType() {
        helpResolve("EXEC pm1.sq3('xyz', {b'true'})"); //$NON-NLS-1$
    }
    
    public void testSubqueryInUnion() {
        String sql = "SELECT IntKey, FloatNum FROM BQT1.MediumA WHERE (IntKey >= 0) AND (IntKey < 15) " + //$NON-NLS-1$
            "UNION ALL " + //$NON-NLS-1$
            "SELECT BQT2.SmallB.IntKey, y.FloatNum " +  //$NON-NLS-1$
            "FROM BQT2.SmallB INNER JOIN " + //$NON-NLS-1$
            "(SELECT IntKey, FloatNum FROM BQT1.MediumA ) AS y ON BQT2.SmallB.IntKey = y.IntKey " + //$NON-NLS-1$
            "WHERE (y.IntKey >= 10) AND (y.IntKey < 30) " + //$NON-NLS-1$
            "ORDER BY IntKey, FloatNum";  //$NON-NLS-1$

        helpResolve(sql, FakeMetadataFactory.exampleBQTCached(), null);
    }

    public void testSubQueryINClause1(){
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
	public void testSubQueryINClauseImplicitConversion(){
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
	public void testSubQueryINClauseNoConversionFails(){
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

    public void testSubQueryINClauseTooManyColumns(){
        String sql = "select e1 from pm1.g1 where e1 in (select e1, e2 from pm4.g1)"; //$NON-NLS-1$

        //test
        this.helpResolveException(sql);
    }

	public void testStoredQueryInFROMSubquery() {
		String sql = "select X.e1 from (EXEC pm1.sq3('abc', 123)) as X"; //$NON-NLS-1$

        helpResolve(sql);
	}
	
	public void testStoredQueryInINSubquery() throws Exception {
		String sql = "select * from pm1.g1 where e1 in (EXEC pm1.sqsp1())"; //$NON-NLS-1$

        helpResolve(sql);
	}	

	// variable resolution
    public void testCreateUpdateProcedure1() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1=1"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variable used in if statement
    public void testCreateUpdateProcedure3() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variable used in if statement, variable comapred against
	// differrent datatype element
    public void testCreateUpdateProcedure4() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1);\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variable used in if statement, invalid operation on variable
    public void testCreateUpdateProcedure5() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 whwre var1 = var1+var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// variable resolution, variables declared in different blocks local variables
	// should not override
    public void testCreateUpdateProcedure6() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Variable var1 was previously declared."); //$NON-NLS-1$
    }
    
	// variable resolution, variables declared in different blocks local variables
	// inner block using outer block variables
    public void testCreateUpdateProcedure7() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var2;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variables declared in differrent blocks local variables
	// outer block cannot use inner block variables
    public void testCreateUpdateProcedure8() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "var2 = 1\n";                 //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }     
    
	// variable resolution, variables declared in differrent blocks local variables
	// should override, outer block variables still valid afetr inner block is declared
    public void testCreateUpdateProcedure9() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 +1;\n";                 //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// special variable ROWS_UPDATED resolution
    public void testCreateUpdateProcedure10() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable ROWS_UPDATED used with declared variable
    public void testCreateUpdateProcedure11() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable INPUT used with declared variable
    public void testCreateUpdateProcedure12() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING used with declared variable
    public void testCreateUpdateProcedure14() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e1 = 'true')\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING and INPUT used in conpound criteria
    public void testCreateUpdateProcedure15() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e1=\"false\" and INPUT.e1=1)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING and INPUT used in conpound criteria, with declared variables
    public void testCreateUpdateProcedure16() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e4 ='true' and INPUT.e2=1 or var1 < 30)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    public void testCreateUpdateProcedure17() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e4 = {d'2000-01-01'})\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Error Code:ERR.015.008.0027 Message:The expressions in this criteria are being compared but are of differing types (boolean and date) and no implicit conversion is available:  CHANGING.e4 = {d'2000-01-01'}"); //$NON-NLS-1$
    }       
    
	// virtual group elements used in procedure(HAS CRITERIA)
    public void testCreateUpdateProcedure18() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// virtual group elements used in procedure in if statement(HAS CRITERIA)
    public void testCreateUpdateProcedure19() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1))\n";                 //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// virtual group elements used in procedure(TRANSLATE CRITERIA)
    public void testCreateUpdateProcedure20() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// virtual group elements used in procedure(TRANSLATE CRITERIA)
    public void testCreateUpdateProcedure21() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using undefined variable should fail
    public void testCreateUpdateProcedure22() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
//        procedure = procedure + "DECLARE integer var1;\n";
        procedure = procedure + "var3 = var2+var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var2 = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using undefined variable declared is of invalid datatype
    public void testCreateUpdateProcedure23() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE struct var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = INPUT.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using declare variable that has parts
    public void testCreateUpdateProcedure24() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var2.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using declare variable is qualified
    public void testCreateUpdateProcedure26() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using declare variable is qualified but has more parts
    public void testCreateUpdateProcedure27() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1.var2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using a variable that has not been declared in an assignment stmt
    public void testCreateUpdateProcedure28() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using a variable that has not been declared in an assignment stmt
    public void testCreateUpdateProcedure29() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = 1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using invalid function in assignment expr
    public void testCreateUpdateProcedure30() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var1 = 'x' + ROWS_UPDATED;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// using invalid function in assignment expr
    public void testCreateUpdateProcedure31() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var1 = 'x' + ROWS_UPDATED;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using a variable being used inside a subcomand
    public void testCreateUpdateProcedure32() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select var1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variables declared in differrent blocks local variables
	// should override, outer block variables still valid afetr inner block is declared
	// fails as variable being compared against incorrect type
    public void testCreateUpdateProcedure33() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE timestamp var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 +1;\n";                 //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// physical elements used on criteria of the if statement
    public void testCreateUpdateProcedure34() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Symbol pm1.g1.e2 is specified with an unknown group context"); //$NON-NLS-1$
    }
    
	// virtual elements used on criteria of the if statement
    public void testCreateUpdateProcedure35() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.e1) and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// physical elements used on criteria of the if statement
    public void testCreateUpdateProcedure36() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }          
    
	// TranslateCriteria on criteria of the if statement
    public void testCreateUpdateProcedure37() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(TRANSLATE CRITERIA ON (vm1.g1.e1) WITH (vm1.g1.e1 = 1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// validating Translate CRITERIA, elements on it should be virtual group elements
	// but can use variables
    public void testCreateUpdateProcedure38() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (pm1.g1.e2 = var1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// physical elements used on criteria of the if statement
    public void testCreateUpdateProcedure39() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    public void testCreateUpdateProcedure40() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    public void testCreateUpdateProcedure41() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (e1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    public void testCreateUpdateProcedure42() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (e1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    public void testCreateUpdateProcedure43() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1);\n";         //$NON-NLS-1$
//        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n";
//        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e2;\n";
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        
        metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);

        Command procCommand = QueryParser.getQueryParser().parseCommand(procedure);
		GroupSymbol virtualGroup = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
		virtualGroup.setMetadataID(metadata.getGroupID("vm1.g1")); //$NON-NLS-1$
		Map externalMetadata = getProcedureExternalMetadata(virtualGroup, metadata);        	
        QueryResolver.resolveCommand(procCommand, externalMetadata, true, metadata, AnalysisRecord.createNonRecordingRecord());
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    public void testCreateUpdateProcedure44() {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(INPUT.e1 = 10)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "INSERT into vm1.g1 (e1) values('x')"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.INSERT_PROCEDURE);
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    public void testCreateUpdateProcedure45() throws Exception {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(INPUT.e1 = 10)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        Command procCommand = QueryParser.getQueryParser().parseCommand(procedure);
        
        metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.INSERT_PROCEDURE, procedure);        
        
		GroupSymbol virtualGroup = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
		virtualGroup.setMetadataID(metadata.getGroupID("vm1.g1")); //$NON-NLS-1$
		
    	Map externalMetadata = getProcedureExternalMetadata(virtualGroup, metadata);        	
        QueryResolver.resolveCommand(procCommand, externalMetadata, true, metadata, AnalysisRecord.createNonRecordingRecord());
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    public void testCreateUpdateProcedure46() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        Command procCommand = QueryParser.getQueryParser().parseCommand(procedure);
        
        metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);        
        
		GroupSymbol virtualGroup = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
		virtualGroup.setMetadataID(metadata.getGroupID("vm1.g1")); //$NON-NLS-1$

		Map externalMetadata = getProcedureExternalMetadata(virtualGroup, metadata);        	
        QueryResolver.resolveCommand(procCommand, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());
    }

	// TranslateCriteria on criteria of the if statement
	public void testCreateUpdateProcedure47() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "if(HAS CRITERIA ON (e1))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (vm1.g1.e1 = pm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// validating Translate CRITERIA, elements(left elements on  on it should be virtual group elements
	public void testCreateUpdateProcedure48() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, INPUT.e2 = 2);\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving Translate CRITERIA, right element should be present on the command
	public void testCreateUpdateProcedure49() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = pm1.g2.e1);\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving criteria selector(on HAS CRITERIA), elements on it should be virtual group elements
	public void testCreateUpdateProcedure50() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.E1, vm1.g1.e1, INPUT.e1))\n";                 //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving Translate CRITERIA, right side expression in the translate criteria should be elements on the command
	public void testCreateUpdateProcedure51() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1=1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e2 = var1+vm1.g1.e2, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// validating Translate CRITERIA, elements on it should be virtual group elements
	// but can use variables, gut left exprs should always be virtual elements
	public void testCreateUpdateProcedure52() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (var1 = vm1.g1.e2, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving AssignmentStatement, variable type and assigned type 
	// do not match and no implicit conversion available
	public void testCreateUpdateProcedure53() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = INPUT.e4;"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving AssignmentStatement, variable type and assigned type 
	// do not match, but implicit conversion available
	public void testCreateUpdateProcedure54() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = 1+1;"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
    
	// resolving AssignmentStatement, variable type and assigned type 
	// do not match, but implicit conversion available
	public void testCreateUpdateProcedure55() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = 1+ROWS_UPDATED;"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}	

    // no user command provided - should throw resolver exception
    public void testCreateUpdateProcedure56() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = 1+ROWS_UPDATED;"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        helpResolveException(procedure, FakeMetadataFactory.example1Cached(), "Error Code:ERR.015.008.0012 Message:Unable to resolve update procedure as the virtual group context is ambiguous."); //$NON-NLS-1$
    }
    
    public void testDefect14912_CreateUpdateProcedure57_FunctionWithElementParamInAssignmentStatement() {
        // Tests that the function params are resolved before the function for assignment statements
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = badFunction(badElement);"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userCommand = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userCommand, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Element \"badElement\" is not defined by any relevant group."); //$NON-NLS-1$
    }
    
	// addresses Cases 4624.  Before change to UpdateProcedureResolver,
    // this case failed with assertion exception.
    public void testCase4624() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "VARIABLES.ROWS_UPDATED = 0;\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = {b'false'};\n"; //$NON-NLS-1$
        procedure = procedure + "IF(var1 = {b 'true'})\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT Rack_ID, RACK_MDT_TYPE INTO #racks FROM Bert_MAP.BERT3.RACK;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userCommand = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userCommand, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Group does not exist: Bert_MAP.BERT3.RACK"); //$NON-NLS-1$
    }

	// addresses Cases 5474.  
    public void testCase5474() {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.NLEVELS;\n"; //$NON-NLS-1$
        procedure = procedure + "VARIABLES.NLEVELS = SELECT COUNT(*) FROM (SELECT oi.e1 AS Col1, oi.e2 AS Col2, oi.e3 FROM pm1.g2 AS oi) AS TOBJ, pm2.g2 AS TModel WHERE TModel.e3 = TOBJ.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpResolve(procedure, FakeMetadataFactory.example1Cached(), null);
    }
    
    public void testIssue174102() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string crit = 'WHERE pm1.sq2.in = \"test\"';\n"; //$NON-NLS-1$
        procedure = procedure + "CREATE LOCAL TEMPORARY TABLE #TTable (e1 string);"; //$NON-NLS-1$
        procedure = procedure + "EXECUTE STRING ('SELECT e1 FROM pm1.sq2 ' || crit ) AS e1 string INTO #TTable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        helpResolve(procedure, FakeMetadataFactory.example1Cached(), null);
    }
    
    // Address Issue 174519.
    // Expected result is resolver failure, but with different error.
    public void testIssue174519() {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.l_in = pm1.sq1.in;\n"; //$NON-NLS-1$
        procedure = procedure + "INSERT INTO #temp \n"; //$NON-NLS-1$
        procedure = procedure + "SELECT pm1.sq3.e1 FROM pm1.sq3 WHERE pm1.sq3.in = VARIABLES.l_in;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = exampleStoredProcedure(procedure);
        helpResolveException("EXEC pm1.sq1(1)", metadata, "Error Code:ERR.015.008.0010 Message:INSERT statement must have the same number of elements and values specified.  This statement has 0 elements and 0 values."); //$NON-NLS-1$ //$NON-NLS-2$
    }

 	private QueryMetadataInterface exampleStoredProcedure(String procedure) {
		FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", procedure); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        return metadata;
	}
    
    public void testIsXMLQuery1() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM pm1.g1", false);     //$NON-NLS-1$
    }

    public void testIsXMLQuery2() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM xmltest.doc1", true); //$NON-NLS-1$
    }

    /**
     * Must be able to resolve XML query if short doc name
     * is used (assuming short doc name isn't ambiguous in a
     * VDB).  Defect 11479.
     */
    public void testIsXMLQuery3() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM doc1", true); //$NON-NLS-1$
    }

    public void testIsXMLQueryFail1() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM xmltest.doc1, xmltest.doc2", false); //$NON-NLS-1$
    }

    public void testIsXMLQueryFail2() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM xmltest.doc1, pm1.g1", false); //$NON-NLS-1$
    }

    public void testIsXMLQueryFail3() throws Exception {
        helpTestIsXMLQuery("SELECT * FROM pm1.g1, xmltest.doc1", false); //$NON-NLS-1$
    }

    /**
     * "docA" is ambiguous as there exist two documents called
     * xmlTest2.docA and xmlTest3.docA.  Defect 11479.
     */
    public void testIsXMLQueryFail4() throws Exception {
        Query query = (Query) helpParse("SELECT * FROM docA"); //$NON-NLS-1$

        try {
            QueryResolver.isXMLQuery(query, metadata);
            fail("expected exception"); //$NON-NLS-1$
        } catch (QueryResolverException e) {
            assertEquals("Group specified is ambiguous, resubmit the query by fully qualifying the group name: docA", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testStringConversion1() {
		// Expected left expression
        ElementSymbol e1 = new ElementSymbol("pm3.g1.e2"); //$NON-NLS-1$
        e1.setType(DataTypeManager.DefaultDataClasses.DATE);
      
        // Expected right expression
        Class srcType = DataTypeManager.DefaultDataClasses.STRING;
        String tgtTypeName = DataTypeManager.DefaultDataTypes.DATE;
        Expression expression = new Constant("2003-02-27"); //$NON-NLS-1$
        
		FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();                          
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
		
	public void testStringConversion2() {
		// Expected left expression
		ElementSymbol e1 = new ElementSymbol("pm3.g1.e2"); //$NON-NLS-1$
		e1.setType(DataTypeManager.DefaultDataClasses.DATE);
      
		// Expected right expression
		Class srcType = DataTypeManager.DefaultDataClasses.STRING;
		String tgtTypeName = DataTypeManager.DefaultDataTypes.DATE;
		Expression expression = new Constant("2003-02-27"); //$NON-NLS-1$
        
		FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();                          
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
	public void testStringConversion3() {
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

    public void testDateToTimestampConversion_defect9747() {
        // Expected left expression
        ElementSymbol e1 = new ElementSymbol("pm3.g1.e4"); //$NON-NLS-1$
        e1.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
               
        // Expected right expression
        Constant e2 = new Constant(new TimestampUtil().createDate(96, 0, 31), DataTypeManager.DefaultDataClasses.DATE);
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
        
    public void testFailedConversion_defect9725() throws Exception{
    	helpResolveException("select * from pm3.g1 where pm3.g1.e4 > {b 'true'}", "Error Code:ERR.015.008.0027 Message:The expressions in this criteria are being compared but are of differing types (timestamp and boolean) and no implicit conversion is available:  pm3.g1.e4 > TRUE"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     *  Constants will now auto resolve if they are consistently representable in the target type
     */
    public void testDefect23257() throws Exception{
    	StoredProcedure command = (StoredProcedure)helpResolve("EXEC pm5.vsp59()"); //$NON-NLS-1$
        
        CommandStatement cs = (CommandStatement)((CreateUpdateProcedureCommand)command.getSubCommand()).getBlock().getStatements().get(1);
        
        Insert insert = (Insert)cs.getCommand();
        
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, ((Expression)insert.getValues().get(1)).getType());
    }  
            
    public void testLookupFunction() {     
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

    public void testLookupFunctionFailBadElement() {     
        String sql = "SELECT lookup('nosuch', 'elementhere', 'e2', e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }

    public void testLookupFunctionFailNotConstantArg1() {     
        String sql = "SELECT lookup(e1, 'e1', 'e2', e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }

    public void testLookupFunctionFailNotConstantArg2() {     
        String sql = "SELECT lookup('pm1.g1', e1, 'e2', e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }
   		
    public void testLookupFunctionFailNotConstantArg3() {     
        String sql = "SELECT lookup('pm1.g1', 'e1', e1, e2) AS x FROM pm1.g1"; //$NON-NLS-1$
        helpResolveException(sql);
    }
 
	public void testLookupFunctionVirtualGroup() throws Exception {     
		String sql = "SELECT lookup('vm1.g1', 'e1', 'e2', e2)  FROM vm1.g1 "; //$NON-NLS-1$
		Query command = (Query) helpParse(sql);
		QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());  		
	}
	
	public void testLookupFunctionPhysicalGroup() throws Exception {     
		String sql = "SELECT lookup('pm1.g1', 'e1', 'e2', e2)  FROM pm1.g1 "; //$NON-NLS-1$
		Query command = (Query) helpParse(sql);
		QueryResolver.resolveCommand(command, FakeMetadataFactory.example1Cached());
	}
	
    public void testLookupFunctionFailBadKeyElement() throws Exception {
    	String sql = "SELECT lookup('pm1.g1', 'e1', 'x', e2) AS x, lookup('pm1.g1', 'e4', 'e3', e3) AS y FROM pm1.g1"; //$NON-NLS-1$
    	Command command = QueryParser.getQueryParser().parseCommand(sql);
    	try {
    		QueryResolver.resolveCommand(command, metadata);
    		fail("exception expected"); //$NON-NLS-1$
    	} catch (QueryResolverException e) {
    		
    	}
    }
    
    // special test for both sides are String
    public void testSetCriteriaCastFromExpression_9657() {
        // parse
        Criteria expected = null;
        Criteria actual = null;
        try { 
            actual = QueryParser.getQueryParser().parseCriteria("bqt1.smalla.shortvalue IN (1, 2)"); //$NON-NLS-1$
            expected = QueryParser.getQueryParser().parseCriteria("convert(bqt1.smalla.shortvalue, integer) IN (1, 2)"); //$NON-NLS-1$
           
        } catch(MetaMatrixException e) { 
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
        }   
   
        // resolve
        try { 
            QueryResolver.resolveCriteria(expected, FakeMetadataFactory.exampleBQTCached());
            QueryResolver.resolveCriteria(actual, FakeMetadataFactory.exampleBQTCached());
        } catch(MetaMatrixException e) { 
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 
        
        // Tweak expected to hide convert function - this is expected
        ((Function) ((SetCriteria)expected).getExpression()).makeImplicit();
        
        assertEquals("Did not match expected criteria", expected, actual); //$NON-NLS-1$
    }    
    
    public void testVirtualProcedure(){
        helpResolve("EXEC pm1.vsp1()");   //$NON-NLS-1$
    }
    
    public void testVirtualProcedure2(){
        helpResolve("EXEC pm1.vsp14()");   //$NON-NLS-1$
    }
    
    public void testVirtualProcedurePartialParameterReference() {
        helpResolve("EXEC pm1.vsp58(5)"); //$NON-NLS-1$
    }
    
    //cursor starts with "#" Defect14924
    public void testVirtualProcedureInvalid1(){
    	helpResolveException("EXEC pm1.vsp32()","Cursor names cannot begin with \"#\" as that indicates the name of a temporary table: #mycursor.");   //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testVirtualProcedureWithOrderBy() {
        helpResolve("EXEC pm1.vsp29()");   //$NON-NLS-1$
    }
    
    public void testVirtualProcedureWithTempTableAndOrderBy() {
        helpResolve("EXEC pm1.vsp33()");   //$NON-NLS-1$
    }
    
    public void testVirtualProcedureWithConstAndOrderBy() {
        helpResolve("EXEC pm1.vsp34()");   //$NON-NLS-1$
    }
    
    public void testVirtualProcedureWithNoFromAndOrderBy() {
        helpResolve("EXEC pm1.vsp28()");   //$NON-NLS-1$
    }

    /** select e1 from pm1.g1 where e2 BETWEEN 1000 AND 2000 */
    public void testBetween1(){
        String sql = "select e1 from pm1.g1 where e2 BETWEEN 1000 AND 2000"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where e2 NOT BETWEEN 1000 AND 2000 */
    public void testBetween2(){
        String sql = "select e1 from pm1.g1 where e2 NOT BETWEEN 1000 AND 2000"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e2 from pm1.g1 where e4 BETWEEN 1000 AND e2 */
    public void testBetween3(){
        String sql = "select e2 from pm1.g1 where e4 BETWEEN 1000 AND e2"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e2 from pm1.g1 where e2 BETWEEN 1000 AND e4 */
    public void testBetween4(){
        String sql = "select e2 from pm1.g1 where e2 BETWEEN 1000 AND e4"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where 1000 BETWEEN e1 AND e2 */
    public void testBetween5(){
        String sql = "select e1 from pm1.g1 where 1000 BETWEEN e1 AND e2"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where 1000 BETWEEN e2 AND e1 */
    public void testBetween6(){
        String sql = "select e1 from pm1.g1 where 1000 BETWEEN e2 AND e1"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm3.g1 where e2 BETWEEN e3 AND e4 */
    public void testBetween7(){
        String sql = "select e1 from pm3.g1 where e2 BETWEEN e3 AND e4"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select pm3.g1.e1 from pm3.g1, pm3.g2 where pm3.g1.e4 BETWEEN pm3.g1.e2 AND pm3.g2.e2 */
    public void testBetween8(){
        String sql = "select pm3.g1.e1 from pm3.g1, pm3.g2 where pm3.g1.e4 BETWEEN pm3.g1.e2 AND pm3.g2.e2"; //$NON-NLS-1$
        helpResolve(sql);
    } 

    /** select e1 from pm1.g1 where e2 = any (select e2 from pm4.g1) */
    public void testCompareSubQuery1(){

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
    public void testCompareSubQuery2(){
        String sql = "select e1 from pm1.g1 where e2 = all (select e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    /** select e1 from pm1.g1 where e2 < (select e2 from pm4.g1 where e1 = '3') */
    public void testCompareSubQuery3(){
        String sql = "select e1 from pm1.g1 where e2 < (select e2 from pm4.g1 where e1 = '3')"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    /** select e1 from pm1.g1 where e2 < (select e2 from pm4.g1 where e1 = '3') */
    public void testCompareSubQueryImplicitConversion(){
        String sql = "select e1 from pm1.g1 where e1 < (select e2 from pm4.g1 where e1 = '3')"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    public void testExistsSubQuery(){
        String sql = "select e1 from pm1.g1 where exists (select e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    public void testExistsSubQuery2(){
        String sql = "select e1 from pm1.g1 where exists (select e1, e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    public void testScalarSubQueryInSelect(){
        String sql = "select e1, (select e2 from pm4.g1 where e1 = '3') from pm1.g1"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    public void testScalarSubQueryInSelect2(){
        String sql = "select (select e2 from pm4.g1 where e1 = '3'), e1 from pm1.g1"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    public void testScalarSubQueryInSelectWithAlias(){
        String sql = "select e1, (select e2 from pm4.g1 where e1 = '3') as X from pm1.g1"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    } 

    public void testSelectWithNoFrom() {
        String sql = "SELECT 5"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testSelectWithNoFrom_Alias() {
        String sql = "SELECT 5 AS INTKEY"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testSelectWithNoFrom_Alias_OrderBy() {
        String sql = "SELECT 5 AS INTKEY ORDER BY INTKEY"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testSubqueryCorrelatedInCriteria(){
        String sql = "select e2 from pm1.g1 where e2 = all (select e2 from pm4.g1 where pm1.g1.e1 = pm4.g1.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm1.g1.e1"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInCriteria2(){
        String sql = "select e1 from pm1.g1 where e2 = all (select e2 from pm4.g1 where pm1.g1.e1 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm1.g1.e1"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInCriteria3(){
        String sql = "select e1 from pm1.g1 X where e2 = all (select e2 from pm4.g1 where X.e1 = pm4.g1.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }
    
    public void testSubqueryCorrelatedInCriteria4(){
        String sql = "select e2 from pm1.g1 X where e2 in (select e2 from pm1.g1 Y where X.e1 = Y.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }    

    public void testSubqueryCorrelatedInCriteria5(){
        String sql = "select e1 from pm1.g1 X where e2 = all (select e2 from pm1.g1 Y where X.e1 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }    

    /* 'e5' is only in pm4.g2 */
    public void testSubqueryCorrelatedInCriteria6(){
        String sql = "select e1 from pm4.g2 where e2 = some (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    /* 'e5' is only in pm4.g2 */
    public void testSubqueryCorrelatedInCriteria7(){
        String sql = "select e1 from pm4.g2 where exists (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInHaving(){
        String sql = "select e1, e2 from pm4.g2 group by e2 having e2 in (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInHaving2(){
        String sql = "select e1, e2 from pm4.g2 group by e2 having e2 <= all (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    /* 'e5' is only in pm4.g2 */
    public void testSubqueryCorrelatedInSelect(){
        String sql = "select e1, (select e2 from pm4.g1 where e5 = e1) from pm4.g2"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInSelect2(){
        String sql = "select e1, (select e2 from pm4.g1 where pm4.g2.e5 = e1) from pm4.g2"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInSelect3(){
        String sql = "select e1, (select e2 from pm4.g1 Y where X.e5 = Y.e1) from pm4.g2 X"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e5"}); //$NON-NLS-1$
    }

    /* 'e5' is only in pm4.g2 */
    public void testNestedCorrelatedSubqueries(){
        String sql = "select e1, (select e2 from pm1.g1 where e2 = all (select e2 from pm4.g1 where e5 = e1)) from pm4.g2"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"pm4.g2.e5"}); //$NON-NLS-1$
    }

    /**
     * 'e5' is in pm4.g2, so it will be resolved to the group aliased as 'Y'
     */
    public void testNestedCorrelatedSubqueries2(){
        String sql = "select e1, (select e2 from pm4.g2 Y where e2 = all (select e2 from pm4.g1 where e5 = e1)) from pm4.g2 X"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"Y.e5"}); //$NON-NLS-1$
    }

    /**
     *  'e5' is in pm4.g2; it will be resolved to the group aliased as 'X' 
     */
    public void testNestedCorrelatedSubqueries3(){
        String sql = "select e1, (select e2 from pm4.g2 Y where e2 = all (select e2 from pm4.g1 where X.e5 = e1)) from pm4.g2 X"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e5"}); //$NON-NLS-1$
    }

    /**
     *  'e5' is in X and Y 
     */
    public void testNestedCorrelatedSubqueries4(){
        String sql = "select X.e2 from pm4.g2 Y, pm4.g2 X where X.e2 = all (select e2 from pm4.g1 where e5 = e1)"; //$NON-NLS-1$
        helpResolveException(sql, metadata, "Element \"e5\" is ambiguous, it exists in two or more groups."); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInCriteriaVirtualLayer(){
        String sql = "select e2 from vm1.g1 where e2 = all (select e2 from vm1.g2 where vm1.g1.e1 = vm1.g2.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"vm1.g1.e1"}); //$NON-NLS-1$
    }

    public void testSubqueryCorrelatedInCriteriaVirtualLayer2(){
        String sql = "select e2 from vm1.g1 X where e2 = all (select e2 from vm1.g2 where X.e1 = vm1.g2.e1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[]{"X.e1"}); //$NON-NLS-1$
    }

    /** 
     * Although this query makes no sense, the "e1" in the nested criteria is
     * NOT a correlated reference 
     */
    public void testSubqueryNonCorrelatedInCriteria(){
        String sql = "select e2 from pm1.g1 where e2 = all (select e2 from pm4.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /** 
     * Although this query makes no sense, the "e1" in the nested criteria is
     * NOT a correlated reference 
     */
    public void testSubqueryNonCorrelatedInCriteria2(){
        String sql = "SELECT e1 FROM pm1.g1 WHERE e2 IN (SELECT e2 FROM pm2.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1))"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /** 
     * Although this query makes no sense, the "e1" in the nested criteria is
     * NOT a correlated reference 
     */
    public void testSubqueryNonCorrelatedInCriteria3(){
        String sql = "SELECT e2 FROM pm2.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /** 
     * The group pm1.g1 in the FROM clause of the subquery should resolve to the 
     * group in metadata, not the temporary child metadata group defined by the
     * outer query.
     */
    public void testSubquery_defect10090(){
        String sql = "select pm1.g1.e1 from pm1.g1 where pm1.g1.e2 in (select pm1.g1.e2 from pm1.g1 where pm1.g1.e4 = 2.0)";  //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    /**
     * Workaround is to alias group in FROM of outer query (aliasing subquery group doesn't work)
     */
    public void testSubquery_defect10090Workaround(){
        String sql = "select X.e1 from pm1.g1 X where X.e2 in (select pm1.g1.e2 from pm1.g1 where pm1.g1.e4 = 2.0)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }

    public void testSubquery2_defect10090(){
        String sql = "select pm1.g1.e1 from pm1.g1 where pm1.g1.e2 in (select X.e2 from pm1.g1 X where X.e4 = 2.0)"; //$NON-NLS-1$
        this.helpResolveSubquery(sql, new String[0]);
    }
    
    /** test jdbc USER method */
    public void testUser() {
        //String sql = "select intkey from SmallA where user() = 'bqt2'";

        // Expected left expression
        FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();                          
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
    
    public void testCaseExpression1() {
        String sql = "SELECT e1, CASE e2 WHEN 0 THEN 20 WHEN 1 THEN 21 WHEN 2 THEN 500 END AS testElement FROM pm1.g1" //$NON-NLS-1$
                    +" WHERE e1 = CASE WHEN e2 = 0 THEN 'a' WHEN e2 = 1 THEN 'b' ELSE 'c' END"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    
    public void testCaseExpression2() {
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
    
    public void testCaseExpressionWithNestedFunction() {
        String sql = "SELECT CASE WHEN e2 < 0 THEN abs(CASE WHEN e2 < 0 THEN -1 ELSE e2 END)" + //$NON-NLS-1$
                           " ELSE e2 END FROM pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testFunctionWithNestedCaseExpression() {
        String sql = "SELECT abs(CASE e1 WHEN 'testString1' THEN -13" + //$NON-NLS-1$
                                       " WHEN 'testString2' THEN -5" + //$NON-NLS-1$
                                       " ELSE abs(e2)" + //$NON-NLS-1$
                               " END) AS absVal FROM pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }
 
    public void testDefect10809(){
        String sql = "select * from LOB_TESTING_ONE where CLOB_COLUMN LIKE '%fff%'"; //$NON-NLS-1$
        helpResolve(helpParse(sql), FakeMetadataFactory.exampleLOB(), AnalysisRecord.createNonRecordingRecord());
    }
    
    public void testNonAutoConversionOfLiteralIntegerToShort() throws Exception {       
        // parse
        Query command = (Query) QueryParser.getQueryParser().parseCommand("SELECT intkey FROM bqt1.smalla WHERE shortvalue = 5"); //$NON-NLS-1$
        
        // resolve
        QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQTCached());
        
        // Check whether an implicit conversion was added on the correct side
        CompareCriteria crit = (CompareCriteria) command.getCriteria();
         
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, crit.getRightExpression().getType());
        assertEquals("Sql is incorrect after resolving", "SELECT intkey FROM bqt1.smalla WHERE shortvalue = 5", command.toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testNonAutoConversionOfLiteralIntegerToShort2() throws Exception {       
        // parse
        Query command = (Query) QueryParser.getQueryParser().parseCommand("SELECT intkey FROM bqt1.smalla WHERE 5 = shortvalue"); //$NON-NLS-1$
        
        // resolve
        QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQTCached());
        
        // Check whether an implicit conversion was added on the correct side
        CompareCriteria crit = (CompareCriteria) command.getCriteria();
         
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, crit.getLeftExpression().getType());
        assertEquals("Sql is incorrect after resolving", "SELECT intkey FROM bqt1.smalla WHERE 5 = shortvalue", command.toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }               

    public void testAliasedOrderBy() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1 as y FROM pm1.g1 ORDER BY y"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "y" }); //$NON-NLS-1$
    }        
    
    public void testUnaliasedOrderBySucceeds() {
        helpResolve("SELECT pm1.g1.e1 a, pm1.g1.e1 b FROM pm1.g1 ORDER BY pm1.g1.e1"); //$NON-NLS-1$
    }

    /** 
     * the group g1 is not known to the order by clause of a union
     */
    public void testUnionOrderByFail() {
        helpResolveException("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY g1.e1", "Error Code:ERR.015.008.0043 Message:Element 'g1.e1' in ORDER BY was not found in SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }      
    
    public void testUnionOrderByFail1() {
        helpResolveException("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY pm1.g1.e1", "Error Code:ERR.015.008.0043 Message:Element 'pm1.g1.e1' in ORDER BY was not found in SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testOrderByPartiallyQualified() {
        helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY g1.e1"); //$NON-NLS-1$
    }
    
    /** 
     * the group g1 is not known to the order by clause of a union
     */
    public void testUnionOrderBy() {
        helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY e1"); //$NON-NLS-1$
    } 
    
    /** 
     * Test for defect 12087 - Insert with implicit conversion from integer to short
     */
    public void testImplConversionBetweenIntAndShort() throws Exception {       
        Command command = QueryParser.getQueryParser().parseCommand("Insert into pm1.g1(e1) Values(convert(100, short))"); //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example6();
        
        QueryResolver.resolveCommand(command, metadata);
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
        
    public void testDefect12968_union() {
        helpResolve(
            helpParse("SELECT myModel.myTable.myColumn AS myColumn from myModel.myTable UNION " + //$NON-NLS-1$
                "SELECT convert(null, string) AS myColumn From myModel2.mySchema.myTable2"),  //$NON-NLS-1$
            example_12968(), AnalysisRecord.createNonRecordingRecord());
    }

    public void testDefect13029_CorrectlySetUpdateProcedureTempGroupIDs() {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
            .append("\nBEGIN") //$NON-NLS-1$
            .append("\nDECLARE string var1;") //$NON-NLS-1$
            .append("\nvar1 = '';") //$NON-NLS-1$
            .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
            .append("\n  BEGIN") //$NON-NLS-1$
            .append("\n    LOOP ON (SELECT pm1.g2.e1 FROM pm1.g2 WHERE loopCursor.e1 = pm1.g2.e1) AS loopCursor2") //$NON-NLS-1$
            .append("\n    BEGIN") //$NON-NLS-1$
            .append("\n      var1 = CONCAT(var1, CONCAT(' ', loopCursor2.e1));") //$NON-NLS-1$
            .append("\n    END") //$NON-NLS-1$
            .append("\n  END") //$NON-NLS-1$
            .append("\nEND"); //$NON-NLS-1$
            
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, proc.toString());

        // parse
        Command userCommand = helpParse(userUpdateStr);

        // resolve
        try {
            QueryResolver.resolveCommand(userCommand, metadata);
        } catch(MetaMatrixException e) {
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        Command command = ((ProcedureContainer)userCommand).getSubCommand();
        Map tempIDs = command.getTemporaryMetadata();
        assertNotNull(tempIDs);
        assertNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$
        
        Command subCommand = (Command)command.getSubCommands().get(0);
        tempIDs = subCommand.getTemporaryMetadata();
        assertNotNull(tempIDs);
        assertNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$

        subCommand = (Command)command.getSubCommands().get(1);
        tempIDs = subCommand.getTemporaryMetadata();
        assertNotNull(tempIDs);
        assertNotNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$

    }

    public void testUnionQueryWithNull() throws Exception{
    	helpResolve("SELECT NULL, e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, e2 FROM pm1.g1 UNION ALL SELECT NULL, e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL FROM pm1.g2 UNION ALL SELECT e1, NULL FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL as e2 FROM pm1.g2 UNION ALL SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$
    	helpResolve("SELECT e1, NULL as e2 FROM pm1.g1 UNION ALL SELECT e1, e3 FROM pm1.g2"); //$NON-NLS-1$
    }
    
    public void testUnionQueryWithDiffTypes() throws Exception{
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
    
    public void testUnionQueryWithDiffTypesFails() throws Exception{
        helpResolveException("SELECT e1 FROM pm1.g1 UNION (SELECT e2 FROM pm1.g2 UNION SELECT e2 from pm1.g1 order by e2)", "The Expression e2 used in a nested UNION ORDER BY clause cannot be implicitly converted from type integer to type string."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testNestedUnionQueryWithNull() throws Exception{
        SetQuery command = (SetQuery)helpResolve("SELECT e2, e3 FROM pm1.g1 UNION (SELECT null, e3 FROM pm1.g2 UNION SELECT null, e3 from pm1.g1)"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((SingleElementSymbol)command.getProjectedSymbols().get(0)).getType());
    }
    
    public void testSelectIntoNoFrom() {
        helpResolve("SELECT 'a', 19, {b'true'}, 13.999 INTO pm1.g1"); //$NON-NLS-1$
    }
    
    public void testSelectInto() {
        helpResolve("SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2"); //$NON-NLS-1$
    }
    
    public void testSelectIntoTempGroup() {
        helpResolve("SELECT 'a', 19, {b'true'}, 13.999 INTO #myTempTable"); //$NON-NLS-1$
        helpResolve("SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g1"); //$NON-NLS-1$
    }
    
    public void testSelectIntoInProcNoFrom() {
        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                            .append("BEGIN\n") //$NON-NLS-1$
                                            .append("SELECT 'a', 19, {b'true'}, 13.999 INTO pm1.g1;\n") //$NON-NLS-1$
                                            .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                            .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
        
        procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("SELECT 'a', 19, {b'true'}, 13.999 INTO #myTempTable;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    public void testSelectIntoInProc() {
        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                            .append("BEGIN\n") //$NON-NLS-1$
                                            .append("SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2;\n") //$NON-NLS-1$
                                            .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                            .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
        
        procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g2;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    //baseline test to ensure that a declare assignment cannot contain the declared variable
    public void testDeclareStatement() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1 = VARIABLES.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    public void testDynamicIntoInProc() {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("execute string 'SELECT e1, e2, e3, e4 FROM pm1.g2' as e1 string, e2 string, e3 string, e4 string INTO #myTempTable;\n") //$NON-NLS-1$
                                .append("select e1 from #myTempTable;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    public void testDynamicStatement() {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("execute string 'SELECT e1, e2, e3, e4 FROM pm1.g2';\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    public void testDynamicStatementType() {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("DECLARE object VARIABLES.X = null;\n") //$NON-NLS-1$
                                .append("execute string VARIABLES.X;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpFailUpdateProcedure(procedure.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    //procedural relational mapping
    public void testProcInVirtualGroup1(){
        String sql = "select e1 from pm1.vsp26 where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testProcInVirtualGroup2(){
        String sql = "select * from pm1.vsp26 as p where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testProcInVirtualGroup3(){
        String sql = "SELECT P.e1 as ve3 FROM pm1.vsp26 as P, pm1.g2 where P.e1=g2.e1 and param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testProcInVirtualGroup4(){
        String sql = "SELECT P.e1 as ve3 FROM pm1.vsp26 as P, vm1.g1 where P.e1=g1.e1 and param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testProcInVirtualGroup5(){
        String sql = "SELECT * FROM (SELECT p.* FROM pm1.vsp26 as P, vm1.g1 where P.e1=g1.e1) x where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    public void testProcInVirtualGroup6(){
        String sql = "SELECT P.e1 as ve3, P.e2 as ve4 FROM pm1.vsp26 as P where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }

    public void testProcInVirtualGroup7(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param1=1 and param2='a'"; //$NON-NLS-1$
        helpResolve(sql);
    }

    public void testProcInVirtualGroup7a(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param1=1"; //$NON-NLS-1$
        helpResolve(sql);
    }
        
    public void testMaterializedTransformation() {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(false, true, false);
        
        Command command = helpResolve(userSql, metadata, analysis);

        assertEquals("Command different than user sql", "SELECT MATVIEW.E1 FROM MATVIEW", command.toString().toUpperCase()); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Command contains no subcommands", 1 == command.getSubCommands().size()); //$NON-NLS-1$
        
        String expectedTransformationSql = "SELECT * FROM MatTable.MatTable"; //$NON-NLS-1$
        Command transCommand = (Command) command.getSubCommands().get(0);

        assertEquals("Commands don't match", expectedTransformationSql, transCommand.toString()); //$NON-NLS-1$
        
        assertEquals("Wrong number of projected symbols", 1, transCommand.getProjectedSymbols().size()); //$NON-NLS-1$
        assertEquals("wrong projected symbol", "MatTable.MatTable.e1", transCommand.getProjectedSymbols().get(0).toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
        Collection annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", ((QueryAnnotation)annotations.iterator().next()).getCategory(), QueryAnnotation.MATERIALIZED_VIEW); //$NON-NLS-1$

        //System.out.println(annotations);

    }

    public void testMaterializedTransformationLoading() {
        String userSql = "SELECT MATVIEW.E1 INTO MatTable.MatStage FROM MATVIEW"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(false, true, false);
        
        Command command = helpResolve(userSql, metadata, analysis);
        assertEquals("Command different than user sql", "SELECT MATVIEW.E1 INTO MATTABLE.MATSTAGE FROM MATVIEW", command.toString().toUpperCase()); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Command contains no subcommands", 1 == command.getSubCommands().size()); //$NON-NLS-1$
        
        String expectedTransformationSql = "SELECT x FROM MatSrc.MatSrc"; //$NON-NLS-1$
        Command transCommand = (Command) command.getSubCommands().get(0);
        assertEquals("Commands don't match", expectedTransformationSql, transCommand.toString()); //$NON-NLS-1$
        
        assertEquals("Wrong number of projected symbols", 1, transCommand.getProjectedSymbols().size()); //$NON-NLS-1$
        assertEquals("wrong projected symbol", "x", transCommand.getProjectedSymbols().get(0).toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
        Collection annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", ((QueryAnnotation)annotations.iterator().next()).getCategory(), QueryAnnotation.MATERIALIZED_VIEW); //$NON-NLS-1$
        
        //System.out.println(annotations);
        
    }    
    
    public void testMaterializedTransformationNoCache() {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW OPTION NOCACHE MatView.MatView"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(false, true, false);
        
        Command command = helpResolve(userSql, metadata, analysis);
        assertEquals("Command different than user sql", "SELECT MATVIEW.E1 FROM MATVIEW OPTION NOCACHE MATVIEW.MATVIEW", command.toString().toUpperCase()); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Command contains no subcommands", 1 == command.getSubCommands().size()); //$NON-NLS-1$
        
        String expectedTransformationSql = "SELECT x FROM MatSrc.MatSrc OPTION NOCACHE"; //$NON-NLS-1$
        Command transCommand = (Command) command.getSubCommands().get(0);
        assertEquals("Commands don't match", expectedTransformationSql, transCommand.toString()); //$NON-NLS-1$
        
        assertEquals("Wrong number of projected symbols", 1, transCommand.getProjectedSymbols().size()); //$NON-NLS-1$
        assertEquals("wrong projected symbol", "x", transCommand.getProjectedSymbols().get(0).toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
        Collection annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", ((QueryAnnotation)annotations.iterator().next()).getCategory(), QueryAnnotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    //related to defect 14423
    public void testMaterializedTransformationNoCache2() {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW OPTION NOCACHE"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(false, true, false);
        
        Command command = helpResolve(userSql, metadata, analysis);
        assertEquals("Command different than user sql", "SELECT MATVIEW.E1 FROM MATVIEW OPTION NOCACHE", command.toString().toUpperCase()); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Command contains no subcommands", 1 == command.getSubCommands().size()); //$NON-NLS-1$
        
        String expectedTransformationSql = "SELECT x FROM MatSrc.MatSrc OPTION NOCACHE"; //$NON-NLS-1$
        Command transCommand = (Command) command.getSubCommands().get(0);
        assertEquals("Commands don't match", expectedTransformationSql, transCommand.toString()); //$NON-NLS-1$
        
        assertEquals("Wrong number of projected symbols", 1, transCommand.getProjectedSymbols().size()); //$NON-NLS-1$
        assertEquals("wrong projected symbol", "x", transCommand.getProjectedSymbols().get(0).toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
        Collection annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", ((QueryAnnotation)annotations.iterator().next()).getCategory(), QueryAnnotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    public void testNoCacheInTransformation(){
        String userSql = "SELECT VGROUP.E1 FROM VGROUP"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(false, true, false);
        
        Command command = helpResolve(userSql, metadata, analysis);
        assertEquals("Command different than user sql", "SELECT VGROUP.E1 FROM VGROUP", command.toString().toUpperCase()); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Command contains no subcommands", 1 == ((Command)command.getSubCommands().get(0)).getSubCommands().size()); //$NON-NLS-1$
        
        String expectedTransformationSql = "SELECT x FROM MatSrc.MatSrc OPTION NOCACHE"; //$NON-NLS-1$
        Command transCommand = (Command)((Command) command.getSubCommands().get(0)).getSubCommands().get(0);
        assertEquals("Commands don't match", expectedTransformationSql, transCommand.toString()); //$NON-NLS-1$
    }
	
    public void testProcParamComparison_defect13653() {
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
    
    public void testXpathValueValid_defect15088() {
        String userSql = "SELECT xpathValue('<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>', 'a/b/c')"; //$NON-NLS-1$
        helpResolve(userSql, FakeMetadataFactory.exampleBQTCached(), AnalysisRecord.createNonRecordingRecord());        
    }

    public void testXpathValueInvalid_defect15088() throws Exception {
        String userSql = "SELECT xpathValue('<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>', '//*[local-name()=''bookName\"]')"; //$NON-NLS-1$
        Command command = helpParse(userSql);
        
        try {
            QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQTCached(), AnalysisRecord.createNonRecordingRecord());
            fail("Expected resolver exception on invalid xpath value"); //$NON-NLS-1$
        } catch(QueryResolverException e) {
            //System.out.println(e.getMessage());
        }
    }

    public void testNullConstantInSelect() throws Exception {
        String userSql = "SELECT null as x"; //$NON-NLS-1$
        Query query = (Query)helpParse(userSql);
        
        QueryResolver.resolveCommand(query, FakeMetadataFactory.exampleBQTCached(), AnalysisRecord.createNonRecordingRecord());
        
        // Check type of resolved null constant
        SingleElementSymbol symbol = (SingleElementSymbol) query.getSelect().getSymbols().get(0);
        assertNotNull(symbol.getType());
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, symbol.getType());
    }

    public void test11716() throws Exception {
    	String sql = "SELECT e1 FROM pm1.g1 where e1='1'"; //$NON-NLS-1$
    	Map externalMetadata = new HashMap();
    	GroupSymbol inputSet = new GroupSymbol("INPUT"); //$NON-NLS-1$
    	List inputSetElements = new ArrayList();
    	ElementSymbol inputSetElement = new ElementSymbol("INPUT.e1"); //$NON-NLS-1$
    	inputSetElements.add(inputSetElement);
    	externalMetadata.put(inputSet, inputSetElements);
        Query command = (Query)helpParse(sql);
        QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());
        Collection groups = GroupCollectorVisitor.getGroups(command, false);
        assertFalse(groups.contains(inputSet));
    }
    
    public void testDefect15872() throws Exception {
    	String sql = "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
    		+ "BEGIN " //$NON-NLS-1$
			+"SELECT * FROM pm1.g1 where model.table.param=e1; " //$NON-NLS-1$
			+"end "; //$NON-NLS-1$
        Command command = helpParse(sql);
    	Map externalMetadata = new HashMap();
    	GroupSymbol procGroup = new GroupSymbol("model.table"); //$NON-NLS-1$
    	List procPrarms = new ArrayList();
    	ElementSymbol param = new ElementSymbol("model.table.param"); //$NON-NLS-1$
    	param.setType(String.class);
    	procPrarms.add(param);
    	externalMetadata.put(procGroup, procPrarms);
        QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());
        CreateUpdateProcedureCommand proc = (CreateUpdateProcedureCommand)command;
        Query query = (Query)proc.getSubCommands().get(0);
        ElementSymbol inElement = (ElementSymbol)((CompareCriteria)query.getCriteria()).getLeftExpression();
        assertNotNull("Input parameter does not have group", inElement.getGroupSymbol()); //$NON-NLS-1$
    }
    
    public void testDefect16894_resolverException_1() {
        helpResolve("SELECT * FROM (SELECT * FROM Pm1.g1 AS Y) AS X"); //$NON-NLS-1$
    }

    public void testDefect16894_resolverException_2() {
        helpResolve("SELECT * FROM (SELECT * FROM Pm1.g1) AS X"); //$NON-NLS-1$
    }

    public void testDefect17385() throws Exception{  
		String sql = "select e1 as x ORDER BY x"; //$NON-NLS-1$      
		helpResolveException(sql);
	}

// Not support XML query as subquery
//    public void testDefect17743() {
//        CompareCriteria expected = new CompareCriteria();
//        ElementSymbol es = new ElementSymbol("node1"); //$NON-NLS-1$
//        GroupSymbol gs = new GroupSymbol("doc1"); //$NON-NLS-1$
//        es.setGroupSymbol(gs);
//        expected.setLeftExpression(es);
//        expected.setOperator(CompareCriteria.EQ);
//        ScalarSubquery subquery = new ScalarSubquery(helpResolve("select node1 from xmltest.doc1")); //$NON-NLS-1$
//        expected.setRightExpression(subquery); //$NON-NLS-1$
//        Query query = (Query) helpResolve("select * from xmltest.doc1 where node1 = (select node1 from xmltest.doc1)"); //$NON-NLS-1$
//        Criteria actual = query.getCriteria();
//        assertEquals("Did not match expected criteria", expected, actual);     //$NON-NLS-1$
//    }
    
    
    public void testValidFullElementNotInQueryGroups() {
        helpResolveException("select pm1.g1.e1 FROM pm1.g1 g"); //$NON-NLS-1$
    }
    
    
    public void testUnionInSubquery() {
        String sql = "SELECT StringKey FROM (SELECT BQT2.SmallB.StringKey FROM BQT2.SmallB union SELECT convert(BQT2.SmallB.FloatNum, string) FROM BQT2.SmallB) x";  //$NON-NLS-1$

        // parse
        Command command = null;
        try {
            command = QueryParser.getQueryParser().parseCommand(sql);
        } catch(MetaMatrixException e) {
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
        }

        // resolve
        try {
            QueryResolver.resolveCommand(command, FakeMetadataFactory.exampleBQT());
        } catch(MetaMatrixException e) {
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testCommandUpdatingCount1() throws Exception{
        Command command = helpResolve("SELECT * FROM pm1.g1 as x, pm1.g1 as y"); //$NON-NLS-1$
        assertEquals(0, command.updatingModelCount(metadata));
    }
    
    public void testCommandUpdatingCount2() throws Exception{
        Command command = helpResolve("SELECT * FROM doc1"); //$NON-NLS-1$
        assertEquals(0, command.updatingModelCount(metadata));
    }
    
    public void testCommandUpdating3() throws Exception{
        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("INSERT INTO pm1.g1 (e1) VALUES (input.e1);\n") //$NON-NLS-1$
        .append("ROWS_UPDATED = INSERT INTO pm1.g2 (e1) VALUES (input.e1);\n") //$NON-NLS-1$
        .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        Command command = helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
        assertEquals(2, command.updatingModelCount(metadata));
    }
    
    public void testCommandUpdatingCount5() throws Exception{
        Command command = helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY e1"); //$NON-NLS-1$
        assertEquals(0, command.updatingModelCount(metadata));
    }

    public void testCommandUpdatingCount6() throws Exception{
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(INPUT.e1 = 10)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "INSERT INTO pm1.g1 (e2) VALUES (Input.e2);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "INSERT into vm1.g1 (e1) values('x')"; //$NON-NLS-1$
        
        Command command = helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.INSERT_PROCEDURE);
        assertEquals(2, command.updatingModelCount(metadata));
    }
    
    /** case 3955 */
    public void testCommandUpdatingCountPhysicalInsert() throws Exception{
        Command command = helpResolve("INSERT INTO pm1.g1 (e2) VALUES (666) "); //$NON-NLS-1$
        assertEquals(1, command.updatingModelCount(metadata));
    }     
    
    /** case 3955 */
    public void testCommandUpdatingCountVirtualInsert() throws Exception{
        Command command = helpResolve("INSERT INTO vm1.g1 (e2) VALUES (666) "); //$NON-NLS-1$
        assertEquals(2, command.updatingModelCount(metadata));
    }    
    
    /** case 3955 */
    public void testCommandUpdatingCountPhysicalUpdate() throws Exception{
        Command command = helpResolve("UPDATE pm1.g1 SET e2=667 WHERE e2=666"); //$NON-NLS-1$
        assertEquals(1, command.updatingModelCount(metadata));
    }     
    
    /** case 3955 */
    public void testCommandUpdatingCountVirtualUpdate() throws Exception{
        Command command = helpResolve("UPDATE vm1.g1 SET e2=667 WHERE e2=666"); //$NON-NLS-1$
        assertEquals(2, command.updatingModelCount(metadata));
    }
    
    /** case 3955 */
    public void testCommandUpdatingCountPhysicalDelete() throws Exception{
        Command command = helpResolve("DELETE FROM pm1.g1 WHERE e2 = 666 "); //$NON-NLS-1$
        assertEquals(1, command.updatingModelCount(metadata));
    }     
    
    /** case 3955 */
    public void testCommandUpdatingCountVirtualDelete() throws Exception{
        Command command = helpResolve("DELETE FROM vm1.g37 WHERE e2 = 666 "); //$NON-NLS-1$
        assertEquals(2, command.updatingModelCount(metadata));
    } 
    
    public void testCommandUpdatingCountEmbeddedExecs() throws Exception {
        Command command = helpResolve("SELECT * FROM pm1.g1 WHERE e1 IN ((select e1 from (EXEC pm1.sp1()) x), (select e1 from (EXEC pm1.sp2(1)) x))"); //$NON-NLS-1$
        
        assertEquals(2, command.updatingModelCount(new TempMetadataAdapter(metadata, new TempMetadataStore())));
    }
    
	public void testCommandUpdatingCountEmbeddedExec() throws Exception {
        Command command = helpResolve("SELECT * FROM pm1.g1 WHERE e1 IN (select e1 from (EXEC pm1.sp1()) x)"); //$NON-NLS-1$
        
        assertEquals(2, command.updatingModelCount(new TempMetadataAdapter(metadata, new TempMetadataStore())));
    }
	
	public void testCommandUpdatingCountFromLastStatement() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "declare integer x = convert(pm1.sq1.in, integer) + 5;\n"; //$NON-NLS-1$
        procedure = procedure + "insert into pm1.g1 values (null, null, null, null);"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = exampleStoredProcedure(procedure);
        Command command = helpResolve(helpParse("exec pm1.sq1(1)"), metadata, null); //$NON-NLS-1$
        
        assertEquals(1, command.updatingModelCount(new TempMetadataAdapter(metadata, new TempMetadataStore())));
	}
		    
    public void testCommandUpdatingCountFromMetadata() throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject proc = metadata.getStore().findObject("pm1.sp1", FakeMetadataObject.PROCEDURE); //$NON-NLS-1$
        proc.putProperty(FakeMetadataObject.Props.UPDATE_COUNT, new Integer(0));
        
        Command command = QueryParser.getQueryParser().parseCommand("EXEC pm1.sp1()"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);
        assertEquals(0, command.updatingModelCount(metadata));
        
        command = QueryParser.getQueryParser().parseCommand("select * from pm1.sp1"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);
        assertEquals(0, command.updatingModelCount(metadata));
        
        command = QueryParser.getQueryParser().parseCommand("select * from pm1.g1 where e1 in (select e1 from (exec pm1.sp1()) x)"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);
        assertEquals(0, command.updatingModelCount(metadata));
    }
    
    public void testParameterError() throws Exception {
        helpResolveException("EXEC pm1.sp2(1, 2)", metadata, "Error Code:ERR.015.008.0007 Message:Incorrect number of parameters specified on the stored procedure pm1.sp2 - expected 1 but got 2"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testUnionOfAliasedLiteralsGetsModified() {
        String sql = "SELECT 5 AS x UNION ALL SELECT 10 AS x"; //$NON-NLS-1$
        Command c = helpResolve(sql); 
        assertEquals(sql, c.toString());
    }
    
    public void testXMLWithProcSubquery() {
        String sql = "SELECT * FROM xmltest.doc4 WHERE node2 IN (SELECT e1 FROM (EXEC pm1.vsp1()) AS x)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
    }
    
    public void testDefect18832() {
        String sql = "SELECT * from (SELECT null as a, e1 FROM pm1.g1) b"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        List projectedSymbols = c.getProjectedSymbols();
        for(int i=0; i< projectedSymbols.size(); i++) {
            ElementSymbol symbol = (ElementSymbol)projectedSymbols.get(i);
            assertTrue(!symbol.getType().equals(DataTypeManager.DefaultDataClasses.NULL));           
        }
    }
    
    public void testDefect18832_2() {
        String sql = "SELECT a.*, b.* from (SELECT null as a, e1 FROM pm1.g1) a, (SELECT e1 FROM pm1.g1) b"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        List projectedSymbols = c.getProjectedSymbols();
        for(int i=0; i< projectedSymbols.size(); i++) {
            ElementSymbol symbol = (ElementSymbol)projectedSymbols.get(i);
            assertTrue(!symbol.getType().equals(DataTypeManager.DefaultDataClasses.NULL));           
        }
    }
    
    public void testDefect20113() {
        String sql = "SELECT g1.* from pm1.g1"; //$NON-NLS-1$
        helpResolve(sql);
    }

    public void testDefect20113_2() {
        String sql = "SELECT g7.* from g7"; //$NON-NLS-1$
        helpResolve(sql);
    }
    
    private void verifyProjectedTypes(Command c, Class[] types) {
        List projSymbols = c.getProjectedSymbols();
        for(int i=0; i<projSymbols.size(); i++) {
            assertEquals("Found type mismatch at column " + i, types[i], ((SingleElementSymbol) projSymbols.get(i)).getType()); //$NON-NLS-1$
        }                
    }
    
    public void testNestedInlineViews() throws Exception {
        String sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM pm1.g1) AS Y) AS X"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
        
        verifyProjectedTypes(c, new Class[] { String.class, Integer.class, Boolean.class, Double.class });
    }

    public void testNestedInlineViewsNoStar() throws Exception {
        String sql = "SELECT e1 FROM (SELECT e1 FROM (SELECT e1 FROM pm1.g1) AS Y) AS X"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());      
        
        verifyProjectedTypes(c, new Class[] { String.class });
    }

    public void testNestedInlineViewsCount() throws Exception {
        String sql = "SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM pm1.g1) AS Y) AS X"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());        
        verifyProjectedTypes(c, new Class[] { Integer.class });
    }
    
    public void testAggOverInlineView() throws Exception {
        String sql = "SELECT SUM(x) FROM (SELECT (e2 + 1) AS x FROM pm1.g1) AS g"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());        
        verifyProjectedTypes(c, new Class[] { Long.class });
        
    }

    public void testCaseOverInlineView() throws Exception {
        String sql = "SELECT CASE WHEN x > 0 THEN 1.0 ELSE 2.0 END FROM (SELECT e2 AS x FROM pm1.g1) AS g"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());        
        verifyProjectedTypes(c, new Class[] { Double.class });
        
    }
    
    //procedure - select * from temp table 
    public void testDefect20083_1 (){
        helpResolve("EXEC pm1.vsp56()");   //$NON-NLS-1$
    }
    
    //procedure - select * from temp table order by
    public void testDefect20083_2 (){
        helpResolve("EXEC pm1.vsp57()");   //$NON-NLS-1$
    }
    
    public void testTypeConversionOverUnion() throws Exception { 
        String sql = "SELECT * FROM (SELECT e2, e1 FROM pm1.g1 UNION SELECT convert(e2, string), e1 FROM pm1.g1) FOO where e2/2 = 1"; //$NON-NLS-1$ 
        helpResolveException(sql); 
    }
    
    public void testVariableDeclarationAfterStatement() throws Exception{
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
    public void testVariableDeclarationAfterStatement1() throws Exception{
        String procedure = "CREATE VIRTUAL PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "select * from xmltest.doc1 where node1 = VARIABLES.X;\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.X = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        helpResolveException(procedure, "Error Code:ERR.015.008.0019 Message:Unable to resolve element: VARIABLES.X"); //$NON-NLS-1$
    }
    
    public void testCreate() {
        String sql = "CREATE LOCAL TEMPORARY TABLE temp_table (column1 string)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());  
    }
    
    public void testCreateQualifiedName() {
        String sql = "CREATE LOCAL TEMPORARY TABLE pm1.g1 (column1 string)"; //$NON-NLS-1$
        helpResolveException(sql, "Cannot create temporary table \"pm1.g1\". Local temporary tables must be created with unqualified names."); //$NON-NLS-1$
    }

    public void testCreateAlreadyExists() {
        String sql = "CREATE LOCAL TEMPORARY TABLE g1 (column1 string)"; //$NON-NLS-1$
        helpResolveException(sql, "Cannot create temporary table \"g1\". A table with the same name already exists."); //$NON-NLS-1$
    }

    public void testCreateImplicitName() {
        String sql = "CREATE LOCAL TEMPORARY TABLE #g1 (column1 string)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
    }
    
    public void testCreateInProc() throws Exception{
        helpResolveException("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table g1(c1 string); end", "Cannot create temporary table \"g1\". A table with the same name already exists.");//$NON-NLS-1$ //$NON-NLS-2$
    }
    
    //this was the old virt.agg procedure.  It was defined in such a way that relied on the scope leak of #temp
    //the exception here is a little weak since there are multiple uses of #temp in the block
    public void testTempTableScope() {
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
    
    public void testDrop() {
        String sql = "DROP TABLE temp_table"; //$NON-NLS-1$
        helpResolveException(sql, "Group does not exist: temp_table"); //$NON-NLS-1$ 
    }
    
    public void testInvalidVirtualProcedure2(){
        helpResolveException("EXEC pm1.vsp12()", FakeMetadataFactory.example1Cached(), "Symbol mycursor.e2 is specified with an unknown group context"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // variable declared is of special type ROWS_RETURNED
    public void testDeclareRowsUpdated() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer rows_updated;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Variable rows_updated was previously declared."); //$NON-NLS-1$
    }
    
    // validating INPUT element assigned
    public void testAssignInput() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "INPUT.e1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Element symbol \"INPUT.e1\" cannot be assigned a value.  Only declared VARIABLES can be assigned values."); //$NON-NLS-1$
    }
    
    // validating CHANGING element assigned
    public void testAssignChanging() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "CHANGING.e1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Element symbol \"CHANGING.e1\" cannot be assigned a value.  Only declared VARIABLES can be assigned values."); //$NON-NLS-1$
    }
    
    // variables cannot be used among insert elements
    public void testVariableInInsert() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (pm1.g1.e2, var1) values (1, 2);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE, "Column variables do not reference columns on group \"pm1.g1\": [Unable to resolve 'var1': Element \"var1\" is not defined by any relevant group.]"); //$NON-NLS-1$
    }
    
    // variables cannot be used among insert elements
    public void testVariableInInsert2() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (pm1.g1.e2, INPUT.x) values (1, 2);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE, "Column variables do not reference columns on group \"pm1.g1\": [Unable to resolve 'INPUT.x': Symbol INPUT.x is specified with an unknown group context]"); //$NON-NLS-1$
    }
    
    //should resolve first to the table's column
    public void testVariableInInsert3() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer e2;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (e2) values (1);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    public void testAmbigousInput() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "select e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Element \"e1\" is ambiguous, it exists in two or more groups."); //$NON-NLS-1$
    }
    
    public void testLoopRedefinition() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  declare string var1;") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        .append("\n    LOOP ON (SELECT pm1.g2.e1 FROM pm1.g2 WHERE loopCursor.e1 = pm1.g2.e1) AS loopCursor") //$NON-NLS-1$
        .append("\n    BEGIN") //$NON-NLS-1$
        .append("\n      var1 = CONCAT(var1, CONCAT(' ', loopCursor.e1));") //$NON-NLS-1$
        .append("\n    END") //$NON-NLS-1$
        .append("\n  END") //$NON-NLS-1$
        .append("\n  END"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Nested Loop can not use the same cursor name as that of its parent."); //$NON-NLS-1$
    }
    
    public void testLoopRedefinition2(){
        helpResolveException("EXEC pm1.vsp11()", FakeMetadataFactory.example1Cached(), "Nested Loop can not use the same cursor name as that of its parent."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testTempGroupElementShouldNotBeResolable() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select 1 as a into #temp;") //$NON-NLS-1$
        .append("\n  select #temp.a from pm1.g1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Symbol #temp.a is specified with an unknown group context"); //$NON-NLS-1$
    }
    
    public void testTempGroupElementShouldNotBeResolable1() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select 1 as a into #temp;") //$NON-NLS-1$
        .append("\n  insert into #temp (a) values (#temp.a);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Symbol #temp.a is specified with an unknown group context"); //$NON-NLS-1$
    }
    
    public void testVariableResolutionWithIntervening() {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  declare string x;") //$NON-NLS-1$
        .append("\n  x = '1';") //$NON-NLS-1$
        .append("\n  declare string y;") //$NON-NLS-1$
        .append("\n  y = '1';") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        helpResolve(proc.toString()); 
    }
    
    public void testProcedureScoping() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        //note that this declare takes presedense over the proc INPUT.e1 and CHANGING.e1 variables
        .append("\n  declare integer e1 = 1;") //$NON-NLS-1$
        .append("\n  e1 = e1;") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        //inside the scope of the loop, an unqualified e1 should resolve to the loop variable group
        .append("\n    variables.e1 = convert(e1, integer);") //$NON-NLS-1$
        .append("\n  END") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        Update command = (Update)helpResolveUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
        
        Block block = ((CreateUpdateProcedureCommand)command.getSubCommand()).getBlock();
        
        AssignmentStatement assStmt = (AssignmentStatement)block.getStatements().get(1);
        assertEquals(ProcedureReservedWords.VARIABLES, assStmt.getVariable().getGroupSymbol().getCanonicalName());
        assertEquals(ProcedureReservedWords.VARIABLES, ((ElementSymbol)assStmt.getValue()).getGroupSymbol().getCanonicalName());
        
        Block inner = ((LoopStatement)block.getStatements().get(2)).getBlock();
        
        assStmt = (AssignmentStatement)inner.getStatements().get(0);
        
        ElementSymbol value = ElementCollectorVisitor.getElements(assStmt.getValue(), false).iterator().next();
        
        assertEquals("LOOPCURSOR", value.getGroupSymbol().getCanonicalName()); //$NON-NLS-1$
    }
    
    public void testResolveUnqualifiedCriteria() throws Exception{
        Criteria criteria = QueryParser.getQueryParser().parseCriteria("e1 = 1"); //$NON-NLS-1$
           
        // resolve
        try { 
            QueryResolver.resolveCriteria(criteria, metadata);
            fail("Exception expected"); //$NON-NLS-1$
        } catch(QueryResolverException e) {
            assertEquals("Symbol e1 is specified with an unknown group context", e.getMessage()); //$NON-NLS-1$
        } 
    }
    
    public void testSameNameRoot() {
        String sql = "select p.e1 from pm1.g1 as pp, pm1.g1 as p"; //$NON-NLS-1$
        
        helpResolve(sql);
    }
    
    public void testDefect23342() throws Exception {
        String sql = "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
            + "BEGIN " //$NON-NLS-1$
            + "IF (param = '1')" //$NON-NLS-1$
            + " BEGIN " //$NON-NLS-1$
            +"SELECT * FROM pm1.g1 where model.table.param=e1; " //$NON-NLS-1$
            +" END " //$NON-NLS-1$
            +"end "; //$NON-NLS-1$
        Command command = helpParse(sql);
        Map externalMetadata = new HashMap();
        GroupSymbol proc = new GroupSymbol("model.table"); //$NON-NLS-1$
        List procPrarms = new ArrayList();
        ElementSymbol param = new ElementSymbol("model.table.param"); //$NON-NLS-1$
        param.setType(DataTypeManager.DefaultDataClasses.STRING);
        procPrarms.add(param);
        externalMetadata.put(proc, procPrarms);
        QueryResolver.resolveCommand(command, externalMetadata, false, metadata, AnalysisRecord.createNonRecordingRecord());
    }
    
    public void testProcedureCreate() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\n  select e1 from t1;") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string, e2 integer);") //$NON-NLS-1$
        .append("\n  select e2 from t1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    /**
     * it is not ok to redefine the loopCursor 
     */
    public void testProcedureCreate1() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table loopCursor (e1 string);") //$NON-NLS-1$
        .append("\nEND") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Cannot create temporary table \"loopCursor\". A table with the same name already exists."); //$NON-NLS-1$
    }
    
    public void testProcedureCreateDrop() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n drop table t1;") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Group does not exist: t1"); //$NON-NLS-1$
    }
    
    public void testProcedureCreateDrop1() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\n  drop table t1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    public void testBatchedUpdateResolver() throws Exception {
        String update1 = "update pm1.g1 set e1 =1"; //$NON-NLS-1$
        String update2 = "update pm2.g1 set e1 =1"; //$NON-NLS-1$
        
        List commands = new ArrayList();
        commands.add(QueryParser.getQueryParser().parseCommand(update1));
        commands.add(QueryParser.getQueryParser().parseCommand(update2));
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        
        helpResolve(command);
    }
    
    public void testAmbiguousAllInGroup() {
        String sql = "SELECT g1.* from pm1.g1, pm2.g1"; //$NON-NLS-1$
        helpResolveException(sql, metadata, "The symbol g1.* refers to more than one group defined in the FROM clause."); //$NON-NLS-1$
    }
    
    public void testRowsUpdatedInProcedure(){
        String sql = "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
            + "BEGIN " //$NON-NLS-1$
            +"SELECT ROWS_UPDATED; " //$NON-NLS-1$
            +"end "; //$NON-NLS-1$
        
        helpResolveException(sql, metadata, "Element \"ROWS_UPDATED\" is not defined by any relevant group."); //$NON-NLS-1$
    }
    
    public void testXMLQueryWithVariable() {
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
    public void testLookupWithoutConstant() throws Exception{
        String sql = "SELECT lookup('pm1.g1', convert('e3', float), 'e2', e2) FROM pm1.g1"; //$NON-NLS-1$
        
        helpResolveException(sql, metadata, "Error Code:ERR.015.008.0063 Message:The first three arguments for the LOOKUP function must be specified as constants."); //$NON-NLS-1$
    }
    
    /**
     * We cannot implicitly convert the argument to double due to lack of precision
     */
    public void testPowerWithBigInteger_Fails() throws Exception {
        String sql = "SELECT power(10, 999999999999999999999999999999999999999999999)"; //$NON-NLS-1$
        
        helpResolveException(sql);
    }
    
    public void testPowerWithLong_Fails() throws Exception {
        String sql = "SELECT power(10, 999999999999)"; //$NON-NLS-1$
        
        helpResolveException(sql);
    }
            
    public void testCreateAfterImplicitTempTable() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select e1 into #temp from pm1.g1;") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    public void testInsertAfterCreate() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string, e2 string);") //$NON-NLS-1$
        .append("\n  insert into #temp (e1) values ('a');") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    public void testUpdateError() {
        String userUpdateStr = "UPDATE vm1.g2 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveException(userUpdateStr, metadata, "Error Code:ERR.015.008.0009 Message:Update is not allowed on the virtual group vm1.g2: no Update procedure was defined."); //$NON-NLS-1$
    }
    
    public void testInsertError() {
        String userUpdateStr = "INSERT into vm1.g2 (e1) values ('x')"; //$NON-NLS-1$
        
        helpResolveException(userUpdateStr, metadata, "Error Code:ERR.015.008.0009 Message:Insert is not allowed on the virtual group vm1.g2: no Insert procedure was defined."); //$NON-NLS-1$
    }
    
    public void testDeleteError() {
        String userUpdateStr = "DELETE from vm1.g2 where e1='x'"; //$NON-NLS-1$
        
        helpResolveException(userUpdateStr, metadata, "Error Code:ERR.015.008.0009 Message:Delete is not allowed on the virtual group vm1.g2: no Delete procedure was defined."); //$NON-NLS-1$
    }
    
    public void testResolveXMLSelect() {
        String procedure = "CREATE VIRTUAL PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.X = 1;\n";         //$NON-NLS-1$
        procedure = procedure + "select VARIABLES.X from xmltest.doc1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        helpResolveException(procedure, "Error Code:ERR.015.008.0019 Message:Unable to resolve element: VARIABLES.X"); //$NON-NLS-1$
    }
    
    public void testXMLJoinFail() {
        String query = "select * from xmltest.doc1, xmltest.doc1"; //$NON-NLS-1$
         
        helpResolveException(query, "Error Code:ERR.015.008.0003 Message:Only one XML document may be specified in the FROM clause of a query."); //$NON-NLS-1$
    }
    
    public void testExecProjectedSymbols() {
        String query = "exec pm1.sq1()"; //$NON-NLS-1$
         
        StoredProcedure proc = (StoredProcedure)helpResolve(query); 
        
        List projected = proc.getProjectedSymbols();
        
        assertEquals(2, projected.size());
        
        for (Iterator i = projected.iterator(); i.hasNext();) {
            ElementSymbol symbol = (ElementSymbol)i.next();
            assertNotNull(symbol.getGroupSymbol());
        }
    }
    
    public void testExecWithDuplicateNames() {
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
    
    public void testInlineViewNullLiteralInUnion() {
        String sql = "select e2 from pm1.g1 union all (select x from (select null as x) y)"; //$NON-NLS-1$
        
        helpResolve(sql);
    }
    
    public void testSelectIntoWithDuplicateNames() {
        String sql = "select 1 as x, 2 as x into #temp"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot create group '#temp' with multiple columns named 'x'"); //$NON-NLS-1$
    }
    
    public void testCreateWithDuplicateNames() {
        String sql = "CREATE LOCAL TEMPORARY TABLE temp_table (column1 string, column1 string)"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot create group \'temp_table\' with multiple columns named \'column1\'"); //$NON-NLS-1$
    }
    
    public void testValidateScalarSubqueryTooManyColumns() {        
        helpResolveException("SELECT e2, (SELECT e1, e2 FROM pm1.g1 WHERE e2 = '3') FROM pm1.g2", "There must be exactly one projected symbol of the subquery: (SELECT e1, e2 FROM pm1.g1 WHERE e2 = '3')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testXMLQuery4() {
        helpResolveException("SELECT * FROM xmltest.doc1 group by a2", "Queries against XML documents can not have a GROUP By clause"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testXMLQuery5() {
        helpResolveException("SELECT * FROM xmltest.doc1 having a2='x'", "Queries against XML documents can not have a HAVING clause"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSelectIntoWithOrderBy() {
        String sql = "select e1, e2 into #temp from pm1.g1 order by e1 limit 10"; //$NON-NLS-1$
        
        helpResolve(sql);
    }
    
    public void testUnionBranchesWithDifferentElementCounts() {
        helpResolveException("SELECT e2, e3 FROM pm1.g1 UNION SELECT e2 FROM pm1.g2","Queries combined with the set operator UNION must have the same number of output elements."); //$NON-NLS-1$ //$NON-NLS-2$
        helpResolveException("SELECT e2 FROM pm1.g1 UNION SELECT e2, e3 FROM pm1.g2","Queries combined with the set operator UNION must have the same number of output elements."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSelectIntoWithNullLiteral() {
        String sql = "select null as x into #temp from pm1.g1"; //$NON-NLS-1$
        
        Query query = (Query)helpResolve(sql);
        
        TempMetadataStore store = new TempMetadataStore(query.getTemporaryMetadata());
        
        TempMetadataID id = store.getTempElementID("#temp.x"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, id.getType());
    }
    
    public void testInsertWithNullLiteral() {
        String sql = "insert into #temp (x) values (null)"; //$NON-NLS-1$
        
        Insert insert = (Insert)helpResolve(sql);
        
        TempMetadataStore store = new TempMetadataStore(insert.getTemporaryMetadata());
        
        TempMetadataID id = store.getTempElementID("#temp.x"); //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, id.getType());
    }
    
    public void testInsertWithoutColumnsFails() {
        String sql = "Insert into pm1.g1 values (1, 2)"; //$NON-NLS-1$
        
        helpResolveException(sql, "Error Code:ERR.015.008.0010 Message:INSERT statement must have the same number of elements and values specified.  This statement has 4 elements and 2 values."); //$NON-NLS-1$
    }
    
    public void testInsertWithoutColumnsFails1() {
        String sql = "Insert into pm1.g1 values (1, 2, 3, 4)"; //$NON-NLS-1$
        
        helpResolveException(sql, "Error Code:ERR.003.029.0013 Message:Exception converting value 3 of type class java.lang.Integer to expected type class java.lang.Boolean"); //$NON-NLS-1$
    }
    
    public void testInsertWithQueryFails() {
        String sql = "Insert into pm1.g1 select 1, 2, 3, 4"; //$NON-NLS-1$
        
        helpResolveException(sql, "Cannot convert insert query expression projected symbol '3' of type java.lang.Integer to insert column 'pm1.g1.e3' of type java.lang.Boolean"); //$NON-NLS-1$
    }

    public void testInsertWithoutColumnsPasses() {
        String sql = "Insert into pm1.g1 values (1, 2, true, 4)"; //$NON-NLS-1$
        
        helpResolve(sql);
        Insert command = (Insert)helpResolve(sql);
        assertEquals(4, command.getVariables().size());
    }

    public void testInsertWithoutColumnsUndefinedTemp() {
        String sql = "Insert into #temp values (1, 2)"; //$NON-NLS-1$

        Insert command = (Insert)helpResolve(sql);
        assertEquals(2, command.getVariables().size());
    }
    
    public void testImplicitTempInsertWithNoColumns() {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table #matt (x integer);") //$NON-NLS-1$
        .append("\n  insert into #matt values (1);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        Command cmd = helpResolve(proc.toString()); 

        String sExpected = "CREATE VIRTUAL PROCEDURE\nBEGIN\nCREATE LOCAL TEMPORARY TABLE #matt (x integer);\nINSERT INTO #matt (#MATT.X) VALUES (1);\nEND\n\tCREATE LOCAL TEMPORARY TABLE #matt (x integer)\n\tINSERT INTO #matt (#MATT.X) VALUES (1)\n";   //$NON-NLS-1$
        String sActual = cmd.printCommandTree(); 
        Assert.assertEquals( sExpected, sActual );
    }

    public void testCase6319() throws QueryResolverException, MetaMatrixComponentException {
        String sql = "select floatnum from bqt1.smalla group by floatnum having sum(floatnum) between 51.0 and 100.0 "; //$NON-NLS-1$
        Query query = (Query)helpParse(sql);
        QueryResolver.resolveCommand(query, FakeMetadataFactory.exampleBQTCached());
    }

    public void testUniqeNamesWithInlineView() {
        helpResolveException("select * from (select count(intNum) a, count(stringKey) b, bqt1.smalla.intkey as b from bqt1.smalla group by bqt1.smalla.intkey) q1 order by q1.a", FakeMetadataFactory.exampleBQTCached(), "Cannot create group 'q1' with multiple columns named 'b'"); //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    public void testNumberedOrderBy1_4_fails() throws Exception {
        helpResolveException("SELECT pm1.g1.e1 as a, avg(e2) as a FROM pm1.g1 ORDER BY 1", "Error Code:ERR.015.008.0042 Message:Element 'a' in ORDER BY is ambiguous and may refer to more than one element of SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testNumberedOrderBy6_fails() throws Exception {
        helpResolveException("SELECT a.e1, b.e1 FROM pm1.g1 AS a, pm1.g1 AS b ORDER BY 2", "Error Code:ERR.015.008.0042 Message:Element 'e1' in ORDER BY is ambiguous and may refer to more than one element of SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testResolveOldProcRelational() {
        helpResolveException("SELECT * FROM pm1.g1, (exec pm1.sq2(pm1.g1.e1)) as a", "Symbol pm1.g1.e1 is specified with an unknown group context"); //$NON-NLS-1$  //$NON-NLS-2$
    }
    
    public void testResolverOrderOfPrecedence() {
        helpResolveException("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 CROSS JOIN (pm1.g2 LEFT OUTER JOIN pm2.g1 on pm1.g1.e1 = pm2.g1.e1)", "Symbol pm1.g1.e1 is specified with an unknown group context"); //$NON-NLS-1$  //$NON-NLS-2$
    }
    
    /**
     * The cross join should parse/resolve with higher precedence
     */
    public void testResolverOrderOfPrecedence_1() {
        helpResolve("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 CROSS JOIN pm1.g2 LEFT OUTER JOIN pm2.g1 on pm1.g1.e1 = pm2.g1.e1"); //$NON-NLS-1$ 
    }
    
    /**
     * should be the same as exec with too many params
     */
	public void testCallableStatementTooManyParameters() throws Exception {
		String sql = "{call pm4.spTest9(?, ?)}"; //$NON-NLS-1$
		
		TestResolver.helpResolveException(sql, FakeMetadataFactory.exampleBQTCached(), "Error Code:ERR.015.008.0007 Message:Incorrect number of parameters specified on the stored procedure pm4.spTest9 - expected 1 but got 2"); //$NON-NLS-1$
	}	
	
	/**
	 * delete procedures should not reference input or changing vars.
	 */
	public void testDefect16451() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "Select pm1.g1.e2 from pm1.g1 where e1 = input.e1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = 0;"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$
        
        String userUpdateStr = "delete from vm1.g1 where e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.DELETE_PROCEDURE, "Symbol input.e1 is specified with an unknown group context"); //$NON-NLS-1$
	}
	
    public void testInvalidVirtualProcedure3() throws Exception {
    	helpResolveException("EXEC pm1.vsp18()", "Group does not exist: temptable"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // variable resolution, variable comapred against
    // differrent datatype element for which there is no implicit transformation)
    public void testCreateUpdateProcedure2() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = UPDATE pm1.g1 SET pm1.g1.e4 = convert(var1, string), pm1.g1.e1 = var1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1=1"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
				 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Error Code:ERR.015.008.0041 Message:Cannot set symbol 'pm1.g1.e4' with expected type double to expression 'convert(var1, string)'"); //$NON-NLS-1$
    }
    
    // special variable INPUT compared against invalid type
    public void testInvalidInputInUpdate() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure += "Select pm1.g1.e2, Input.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = UPDATE pm1.g1 SET pm1.g1.e1 = INPUT.e1, pm1.g1.e2 = INPUT.e1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
				 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Error Code:ERR.015.008.0041 Message:Cannot set symbol 'pm1.g1.e2' with expected type integer to expression 'INPUT.e1'"); //$NON-NLS-1$
    }
    
    public void testUpdateSetClauseReferenceType() {
    	String sql = "UPDATE pm1.g1 SET pm1.g1.e1 = 1, pm1.g1.e2 = ?;"; //$NON-NLS-1$
    	
    	Update update = (Update)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	
    	Expression ref = update.getChangeList().getClauses().get(1).getValue();
    	assertTrue(ref instanceof Reference);
    	assertNotNull(ref.getType());
    }
    
    public void testNoTypeCriteria() {
    	String sql = "select * from pm1.g1 where ? = ?"; //$NON-NLS-1$
    	
    	helpResolveException(sql, FakeMetadataFactory.example1Cached(), "Error Code:ERR.015.008.0026 Message:Expression '? = ?' has a parameter with non-determinable type information.  The use of an explicit convert may be necessary."); //$NON-NLS-1$
    }
    
    public void testReferenceInSelect() {
    	String sql = "select ?, e1 from pm1.g1"; //$NON-NLS-1$
    	Query command = (Query)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	assertEquals(DataTypeManager.DefaultDataClasses.STRING, ((SingleElementSymbol)command.getProjectedSymbols().get(0)).getType());
    }
    
    public void testReferenceInSelect1() {
    	String sql = "select convert(?, integer), e1 from pm1.g1"; //$NON-NLS-1$
    	
    	Query command = (Query)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((SingleElementSymbol)command.getProjectedSymbols().get(0)).getType());
    }
    
    public void testUnionWithObjectTypeConversion() {
    	String sql = "select convert(null, xml) from pm1.g1 union all select 1"; //$NON-NLS-1$
    	
    	SetQuery query = (SetQuery)helpResolve(sql, FakeMetadataFactory.example1Cached(), null);
    	assertEquals(DataTypeManager.DefaultDataClasses.OBJECT, ((SingleElementSymbol)query.getProjectedSymbols().get(0)).getType());
    }
    
    public void testUnionWithSubQuery() {
    	String sql = "select 1 from pm1.g1 where exists (select 1) union select 2"; //$NON-NLS-1$

        SetQuery command = (SetQuery)helpResolve(sql);
        
        assertEquals(1, command.getSubCommands().size());
    }
    public void testOrderBy_J658a() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2, e3 as x, (5+2) as y FROM pm1.g1 ORDER BY e3"); //$NON-NLS-1$
        OrderBy orderBy = resolvedQuery.getOrderBy();
        int[] expectedPositions = new int[] {2};
        helpTestOrderBy(orderBy, expectedPositions);
    }

	private void helpTestOrderBy(OrderBy orderBy, int[] expectedPositions) {
		assertEquals(expectedPositions.length, orderBy.getVariableCount());
        for (int i = 0; i < expectedPositions.length; i++) {
        	ElementSymbol symbol = (ElementSymbol)orderBy.getVariable(i);
        	TempMetadataID tid = (TempMetadataID)symbol.getMetadataID();
        	assertEquals(expectedPositions[i], tid.getPosition());
        }
	}
    public void testOrderBy_J658b() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2, e3 as x, (5+2) as y FROM pm1.g1 ORDER BY e2, e3 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});
    }
    public void testOrderBy_J658c() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as y FROM pm1.g1 ORDER BY x, e3 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});    
    }
    
    // ambiguous, should fail
    public void testOrderBy_J658d() {
        helpResolveException("SELECT pm1.g1.e1, e2 as x, e3 as x FROM pm1.g1 ORDER BY x, e1 ", "Error Code:ERR.015.008.0042 Message:Element 'x' in ORDER BY is ambiguous and may refer to more than one element of SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testOrderBy_J658e() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as e2 FROM pm1.g1 ORDER BY x, e2 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});
    }
    
    public void testSPOutParamWithExec() {
    	StoredProcedure proc = (StoredProcedure)helpResolve("exec pm2.spTest8(1)", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(2, proc.getProjectedSymbols().size());
    }

    /**
     * Note that the call syntax is not quite correct, the output parameter is not in the arg list.
     * That hack is handled by the PreparedStatementRequest
     */
    public void testSPOutParamWithCallableStatement() {
    	StoredProcedure proc = (StoredProcedure)helpResolve("{call pm2.spTest8(1)}", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(3, proc.getProjectedSymbols().size());
    }
    
    public void testProcRelationalWithOutParam() {
    	Query proc = (Query)helpResolve("select * from pm2.spTest8 where inkey = 1", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(3, proc.getProjectedSymbols().size());
    }
    
    public void testSPReturnParamWithNoResultSet() {
    	StoredProcedure proc = (StoredProcedure)helpResolve("exec pm4.spTest9(1)", FakeMetadataFactory.exampleBQTCached(), null);
    	assertEquals(1, proc.getProjectedSymbols().size());
    }
    
    public void testSecondPassFunctionResolving() {
    	helpResolve("SELECT pm1.g1.e1 FROM pm1.g1 where lower(?) = e1 "); //$NON-NLS-1$
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
    public void testAggregateWithBetweenInCaseInSelect() {
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
    public void testBetweenInCaseInSelect() {
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
    public void testBetweenInCase() {
    	String sql = "SELECT * FROM pm1.g1 WHERE e3 = CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END"; //$NON-NLS-1$
    	helpResolve(sql);
    }
    
    public void testOrderByUnrelated() {
        helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as y FROM pm1.g1 ORDER BY e4"); //$NON-NLS-1$
    }

    public void testOrderByUnrelated1() {
        helpResolveException("SELECT distinct pm1.g1.e1, e2 as x, e3 as y FROM pm1.g1 ORDER BY e4"); //$NON-NLS-1$
    }

    public void testOrderByUnrelated2() {
        helpResolveException("SELECT max(e2) FROM pm1.g1 group by e1 ORDER BY e4"); //$NON-NLS-1$
    }

}