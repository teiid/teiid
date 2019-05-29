/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;

@SuppressWarnings("nls")
public class TestResolver {

    private QueryMetadataInterface metadata;

    @Before public void setUp() {
        metadata = RealMetadataFactory.example1Cached();
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
        Iterator<ElementSymbol> variablesIter = variables.iterator();
        for (int i=0; variablesIter.hasNext(); i++) {
            ElementSymbol variable = variablesIter.next();
            assertTrue("Expected variable name " + variableNames[i] + " but was " + variable.getName(),  //$NON-NLS-1$ //$NON-NLS-2$
                       variable.getName().equalsIgnoreCase(variableNames[i]));
        }

        if (variableNames.length == 0){
            //There should be no TempMetadataIDs
            Collection<Symbol> symbols = CheckNoTempMetadataIDsVisitor.checkSymbols(query);
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

    public static Command helpResolve(String sql, QueryMetadataInterface queryMetadata){
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
        Collection<LanguageObject> unresolvedSymbols = vis.getUnresolvedSymbols();
        assertTrue("Found unresolved symbols: " + unresolvedSymbols, unresolvedSymbols.isEmpty()); //$NON-NLS-1$
        return command;
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
        Collection<LanguageObject> unresolvedSymbols = vis.getUnresolvedSymbols();
        assertTrue("Found unresolved symbols: " + unresolvedSymbols, unresolvedSymbols.isEmpty()); //$NON-NLS-1$
        return criteria;
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
        List<GroupSymbol> groups = from.getGroups();
        assertEquals("Wrong number of group IDs: ", groupIDs.length, groups.size()); //$NON-NLS-1$

        for(int i=0; i<groups.size(); i++) {
            GroupSymbol group = groups.get(i);
            assertNotNull(group.getMetadataID());
            assertEquals("Group ID does not match: ", groupIDs[i].toUpperCase(), group.getNonCorrelationName().toUpperCase()); //$NON-NLS-1$
        }
    }

    private void helpCheckSelect(Query query, String[] elementNames) {
        Select select = query.getSelect();
        List<Expression> elements = select.getProjectedSymbols();
        assertEquals("Wrong number of select symbols: ", elementNames.length, elements.size()); //$NON-NLS-1$

        for(int i=0; i<elements.size(); i++) {
            Expression symbol = elements.get(i);
            String name = Symbol.getShortName(symbol);
            if (symbol instanceof ElementSymbol) {
                name = ((ElementSymbol)symbol).getName();
            }
            assertEquals("Element name does not match: ", elementNames[i].toUpperCase(), name.toString().toUpperCase()); //$NON-NLS-1$
        }
    }

    private void helpCheckElements(LanguageObject langObj, String[] elementNames, String[] elementIDs) {
        List<ElementSymbol> elements = new ArrayList<ElementSymbol>();
        ElementCollectorVisitor.getElements(langObj, elements);
        assertEquals("Wrong number of elements: ", elementNames.length, elements.size()); //$NON-NLS-1$

        for(int i=0; i<elements.size(); i++) {
            ElementSymbol symbol = elements.get(i);
            assertEquals("Element name does not match: ", elementNames[i].toUpperCase(), symbol.getName().toUpperCase()); //$NON-NLS-1$

            Object elementID = symbol.getMetadataID();
            try {
                String name = metadata.getFullName(elementID);
                assertNotNull("ElementSymbol " + symbol + " was not resolved and has no metadataID", elementID); //$NON-NLS-1$ //$NON-NLS-2$
                assertEquals("ElementID name does not match: ", elementIDs[i].toUpperCase(), name.toUpperCase()); //$NON-NLS-1$
            } catch (TeiidComponentException e) {
                throw new RuntimeException(e);
            }
        }
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

        Collection<SPParameter> params = proc.getParameters();

        // Check remaining params against expected expressions
        int i = 0;
        for (SPParameter param : params) {
            if (param.getParameterType() != SPParameter.IN && param.getParameterType() != SPParameter.INOUT) {
                continue;
            }
            if (expectedParameterExpressions[i] == null) {
                assertNull(param.getExpression());
            } else {
                assertEquals(expectedParameterExpressions[i], param.getExpression());
            }
            i++;
        }
        assertEquals(expectedParameterExpressions.length, i);

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
        String sql = "SELECT e1 FROM example1.pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
    }

    @Test public void testAliasedGroupWithVDB() {
        String sql = "SELECT e1 FROM example1.pm1.g1 AS x"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString());         //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup1() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup2() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.g2" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup3() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup4() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.cat2.g2" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup5() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat2.g3"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat2.g3" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup6() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM cat1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.cat1.g1" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup7() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM g4"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm3.g4" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroup8() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT e1 FROM pm2.g3"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm2.g3" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedGroupWithAlias() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT X.e1 FROM cat2.cat3.g1 as X"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckFrom(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1" }); //$NON-NLS-1$
    }

    @Test public void testPartiallyQualifiedElement1() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat2.cat3.g1.e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }

    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement2() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }

    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement3() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1 FROM cat2.cat3.g1, cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1" }); //$NON-NLS-1$
    }

    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement4() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat1.g2.e1 FROM cat2.cat3.g1, cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.g2.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testPartiallyQualifiedElement5() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat1.g2.e1 FROM example3.pm1.cat1.cat2.cat3.g1, pm1.cat1.g2"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.g2.e1" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** defect 12536 */
    @Test public void testPartiallyQualifiedElement6() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, e2 FROM cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.cat2.cat3.g1.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testPartiallyQualifiedElement7() {
        metadata = RealMetadataFactory.example3();
        String sql = "SELECT cat3.g1.e1, cat2.cat3.g1.e2, g1.e3 FROM pm1.cat1.cat2.cat3.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.cat1.cat2.cat3.g1.e1", "pm1.cat1.cat2.cat3.g1.e2", "pm1.cat1.cat2.cat3.g1.e3" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testFailPartiallyQualifiedGroup1() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT e1 FROM cat3.g1"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedGroup2() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT e1 FROM g1"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedGroup3() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT e1 FROM g2"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedGroup4() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT e1 FROM g3"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedGroup5() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT e1 FROM g5");         //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedElement1() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT cat3.g1.e1 FROM pm1.cat1.cat2.cat3.g1, pm2.cat3.g1"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedElement2() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT g1.e1 FROM pm1.cat1.cat2.cat3.g1, pm2.cat3.g1"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedElement3() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT cat3.g1.e1 FROM pm2.cat2.g2, pm1.cat2.g3"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedElement4() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT cat3.g1.e1 FROM pm2.cat2.g2"); //$NON-NLS-1$
    }

    @Test public void testFailPartiallyQualifiedElement5() {
        metadata = RealMetadataFactory.example3();
        helpResolveException("SELECT cat3.g1.e1 FROM g1"); //$NON-NLS-1$
    }

    @Test public void testElementWithVDB() {
        String sql = "SELECT example1.pm1.g1.e1 FROM pm1.g1"; //$NON-NLS-1$
        Query resolvedQuery = (Query) helpResolve(sql);
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
        assertEquals("Resolved string form was incorrect ", sql, resolvedQuery.toString()); //$NON-NLS-1$
    }

    @Test public void testAliasedElementWithVDB() {
        Query resolvedQuery = (Query) helpResolve("SELECT example1.pm1.g1.e1 AS x FROM pm1.g1"); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "x" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1" }, //$NON-NLS-1$
            new String[] { "pm1.g1.e1" } ); //$NON-NLS-1$
    }

    @Test public void testSelectStar() {
        Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testSelectStarFromAliasedGroup() {
        Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1 as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testSelectStarFromMultipleAliasedGroups() {
        Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g1 as x, pm1.g1 as y"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1", "pm1.g1" }); //$NON-NLS-1$ //$NON-NLS-2$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "x.e1", "x.e2", "x.e3", "x.e4", "y.e1", "y.e2", "y.e3", "y.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4", "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
    }

    @Test public void testSelectStarWhereSomeElementsAreNotSelectable() {
        Query resolvedQuery = (Query) helpResolve("SELECT * FROM pm1.g4"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g4" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g4.e1", "pm1.g4.e3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "pm1.g4.e1", "pm1.g4.e3" } ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectGroupStarWhereSomeElementsAreNotSelectable() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g4.* FROM pm1.g4"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g4" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g4.e1", "pm1.g4.e3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "pm1.g4.e1", "pm1.g4.e3" } ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFullyQualifiedSelectStar() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.* FROM pm1.g1"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testSelectAllInAliasedGroup() {
        Query resolvedQuery = (Query) helpResolve("SELECT x.* FROM pm1.g1 as x"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "x.e1", "x.e2", "x.e3", "x.e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { "pm1.g1.e1", "pm1.g1.e2", "pm1.g1.e3", "pm1.g1.e4" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testSelectExpressions() {
        Query resolvedQuery = (Query) helpResolve("SELECT e1, concat(e1, 's'), concat(e1, 's') as c FROM pm1.g1"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "pm1.g1.e1", "expr2", "c" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpCheckElements(resolvedQuery.getSelect(),
            new String[] { "pm1.g1.e1", "pm1.g1.e1", "pm1.g1.e1" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { "pm1.g1.e1", "pm1.g1.e1", "pm1.g1.e1" } ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testSelectCountStar() {
        Query resolvedQuery = (Query) helpResolve("SELECT count(*) FROM pm1.g1"); //$NON-NLS-1$
        helpCheckFrom(resolvedQuery, new String[] { "pm1.g1" }); //$NON-NLS-1$
        helpCheckSelect(resolvedQuery, new String[] { "expr1" }); //$NON-NLS-1$
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

    @Test public void testLateralJoinDirection() {
        helpResolveException("select * from pm1.g1 right outer join lateral(select g1.e1) x on pm1.g1.e1 = x.e1", "TEIID31268 Element g1.e1 cannot be a lateral reference because it does not come from an INNER or LEFT OUTER join."); //$NON-NLS-1$
    }

    @Test public void testLateralJoinDirectionImplicit() {
        helpResolveException("select * from pm1.g1 x1 right join texttable(x1.e1||'' columns a string) x2 on x1.e1=x2.e1", "TEIID31268 Element x1.e1 cannot be a lateral reference because it does not come from an INNER or LEFT OUTER join."); //$NON-NLS-1$
    }

    @Test public void testLateralJoinDirectionImplicit1() {
        helpResolveException("select * from pm1.g1 x1 right join arraytable((x1.e1,) columns a string) x2 on x1.e1=x2.e1", "TEIID31268 Element x1.e1 cannot be a lateral reference because it does not come from an INNER or LEFT OUTER join."); //$NON-NLS-1$
    }

    @Test public void testLateralJoinDirection1() {
        helpResolve("select * from (select 1 e1) g1, ((select 1 e1) g2 right outer join lateral(select g1.e1) x on g2.e1 = x.e1)"); //$NON-NLS-1$
    }

    @Test public void testLateralJoinDirection2() {
        helpResolveException("select * from pm1.g1 full outer join lateral(select pm1.g1.e1) x on pm1.g1.e1 = x.e1"); //$NON-NLS-1$
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
        helpResolveException("SELECT abc(e1) FROM pm1.g1", "TEIID30068 The function 'abc(e1)' is an unknown form.  Check that the function name and number of arguments is correct."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConversionPossible() {
        helpResolve("SELECT dayofmonth('2002-01-01') FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
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
        Map<Integer, SPParameter> params = proc.getMapOfParameters();
        assertEquals("Did not get expected parameter count", 2, params.size()); //$NON-NLS-1$

        // Check resolved parameters
        SPParameter param1 = params.get(2);
        helpCheckParameter(param1, ParameterInfo.RESULT_SET, 2, "pm1.sq2.ret", java.sql.ResultSet.class, null); //$NON-NLS-1$

        SPParameter param2 = params.get(1);
        helpCheckParameter(param2, ParameterInfo.IN, 1, "pm1.sq2.in", DataTypeManager.DefaultDataClasses.STRING, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
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
        Map<Integer, SPParameter> params = proc.getMapOfParameters();
        assertEquals("Did not get expected parameter count", 3, params.size()); //$NON-NLS-1$

        // Check resolved parameters
        SPParameter param1 = params.get(1);
        helpCheckParameter(param1, ParameterInfo.IN, 1, "pm1.sq3a.in", DataTypeManager.DefaultDataClasses.STRING, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$

        SPParameter param2 = params.get(2);
        helpCheckParameter(param2, ParameterInfo.IN, 2, "pm1.sq3a.in2", DataTypeManager.DefaultDataClasses.INTEGER, new Constant(new Integer(123))); //$NON-NLS-1$
    }

    private void helpCheckParameter(SPParameter param, int paramType, int index, String name, Class<?> type, Expression expr) {
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

        List<ElementSymbol> elements = (List<ElementSymbol>) ElementCollectorVisitor.getElements(resolvedQuery.getSelect(), false);

        ElementSymbol elem1 = elements.get(0);
        assertEquals("Did not get expected element", "x.e1", elem1.getName() ); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Did not get expected type", DataTypeManager.DefaultDataClasses.STRING, elem1.getType()); //$NON-NLS-1$

        ElementSymbol elem2 = elements.get(1);
        assertEquals("Did not get expected element", "x.e2", elem2.getName() ); //$NON-NLS-1$ //$NON-NLS-2$
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
            assertEquals("TEIID31119 Symbol pm1.sq2.\"in\" is specified with an unknown group context", e.getMessage()); //$NON-NLS-1$
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

        helpResolve(sql, RealMetadataFactory.exampleBQTCached());
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
        helpResolveException("select e1 from pm1.g1 where e1 in (select e2 from pm4.g1)");
    }

    @Test public void testSubQueryINClauseTooManyColumns(){
        String sql = "select e1 from pm1.g1 where e1 in (select e1, e2 from pm4.g1)"; //$NON-NLS-1$

        //test
        this.helpResolveException(sql);
    }

    @Test public void testSubQueryINArrayComparison(){
        String sql = "select e1 from pm1.g1 where (e1, e2) in (select e1, e2 from pm4.g1)"; //$NON-NLS-1$

        //test
        this.helpResolve(sql);
    }

    @Test public void testStoredQueryInFROMSubquery() {
        String sql = "select X.e1 from (EXEC pm1.sq3('abc', 123)) as X"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testStoredQueryInINSubquery() throws Exception {
        String sql = "select * from pm1.g1 where e1 in (EXEC pm1.sqsp1())"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testStringConversion1() {
        metadata = RealMetadataFactory.exampleBQTCached();
        Criteria crit = helpResolveCriteria("bqt1.smalla.datevalue = '2003-02-27'");
        assertTrue(((CompareCriteria)crit).getRightExpression().getType() == DataTypeManager.DefaultDataClasses.DATE);
        assertEquals("bqt1.smalla.datevalue = {d'2003-02-27'}", crit.toString());
    }

    @Test public void testStringConversion2() {
        metadata = RealMetadataFactory.exampleBQTCached();
        Criteria crit = helpResolveCriteria("'2003-02-27' = bqt1.smalla.datevalue");
        assertTrue(((CompareCriteria)crit).getRightExpression().getType() == DataTypeManager.DefaultDataClasses.DATE);
        assertEquals("{d'2003-02-27'} = bqt1.smalla.datevalue", crit.toString());
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
        CompareCriteria actual = (CompareCriteria) helpResolveCriteria("pm3.g1.e1='2003-02-27'");     //$NON-NLS-1$

        //if (! actual.getLeftExpression().equals(expected.getLeftExpression())) {
        //    System.out.println("left exprs not equal");
        //} else if (!actual.getRightExpression().equals(expected.getRightExpression())) {
        //    System.out.println("right exprs not equal");
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
        helpResolveException("select * from pm3.g1 where pm3.g1.e4 > {b 'true'}", "TEIID30072 The expressions in this criteria are being compared but are of differing types (timestamp and boolean) and no implicit conversion is available: pm3.g1.e4 > TRUE"); //$NON-NLS-1$ //$NON-NLS-2$
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
        assertEquals("Wrong type for first symbol", String.class, ((Expression)projSymbols.get(0)).getType()); //$NON-NLS-1$
        assertEquals("Wrong type for second symbol", Double.class, ((Expression)projSymbols.get(1)).getType()); //$NON-NLS-1$
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
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
    }

    @Test public void testLookupFunctionPhysicalGroup() throws Exception {
        String sql = "SELECT lookup('pm1.g1', 'e1', 'e2', e2)  FROM pm1.g1 "; //$NON-NLS-1$
        Query command = (Query) helpParse(sql);
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
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

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));

        Query command = (Query) helpParse(sql);
        QueryResolver.resolveCommand(command, metadata);

        command = (Query) helpParse("SELECT func('e1')  FROM vm1.g1 ");
        QueryResolver.resolveCommand(command, metadata);
    }

    // special test for both sides are String
    @Test public void testSetCriteriaCastFromExpression_9657() {
        // parse
        Criteria actual = null;
        try {
            actual = QueryParser.getQueryParser().parseCriteria("bqt1.smalla.shortvalue IN (1, 2)"); //$NON-NLS-1$
        } catch(TeiidException e) {
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());    //$NON-NLS-1$ //$NON-NLS-2$
        }

        // resolve
        try {
            QueryResolver.resolveCriteria(actual, RealMetadataFactory.exampleBQTCached());
        } catch(TeiidException e) {
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        }

        assertEquals("Did not match expected criteria", ((SetCriteria)actual).getExpression().getType(), DataTypeManager.DefaultDataClasses.SHORT); //$NON-NLS-1$
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
        helpResolveException(sql, metadata, "TEIID31117 Element \"e5\" is ambiguous and should be qualified, at a single scope it exists in [pm4.g2 AS Y, pm4.g2 AS X]"); //$NON-NLS-1$
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
        FunctionLibrary library = RealMetadataFactory.SFM.getSystemFunctionLibrary();
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
        helpResolve(helpParse(sql), RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testNonAutoConversionOfLiteralIntegerToShort() throws Exception {
        // parse
        Query command = (Query) QueryParser.getQueryParser().parseCommand("SELECT intkey FROM bqt1.smalla WHERE shortvalue = 5"); //$NON-NLS-1$

        // resolve
        QueryResolver.resolveCommand(command, RealMetadataFactory.exampleBQTCached());

        // Check whether an implicit conversion was added on the correct side
        CompareCriteria crit = (CompareCriteria) command.getCriteria();

        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, crit.getRightExpression().getType());
        assertEquals("Sql is incorrect after resolving", "SELECT intkey FROM bqt1.smalla WHERE shortvalue = 5", command.toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNonAutoConversionOfLiteralIntegerToShort2() throws Exception {
        // parse
        Query command = (Query) QueryParser.getQueryParser().parseCommand("SELECT intkey FROM bqt1.smalla WHERE 5 = shortvalue"); //$NON-NLS-1$

        // resolve
        QueryResolver.resolveCommand(command, RealMetadataFactory.exampleBQTCached());

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

    /**
     * the group g1 is not known to the order by clause of a union
     */
    @Test public void testUnionOrderByFail() {
        helpResolveException("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY g1.e1", "TEIID30086 ORDER BY expression 'g1.e1' cannot be used with a set query."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUnionOrderByFail1() {
        helpResolveException("SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY pm1.g1.e1", "TEIID30086 ORDER BY expression 'pm1.g1.e1' cannot be used with a set query."); //$NON-NLS-1$ //$NON-NLS-2$
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

    public static TransformationMetadata example_12968() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = RealMetadataFactory.createPhysicalModel("myModel", metadataStore); //$NON-NLS-1$
        Schema pm2 = RealMetadataFactory.createPhysicalModel("myModel2", metadataStore); //$NON-NLS-1$

        Table pm1g1 = RealMetadataFactory.createPhysicalGroup("myTable", pm1); //$NON-NLS-1$
        Table pm2g1 = RealMetadataFactory.createPhysicalGroup("mySchema.myTable2", pm2); //$NON-NLS-1$

        RealMetadataFactory.createElements(pm1g1,
            new String[] { "myColumn", "myColumn2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        RealMetadataFactory.createElements(pm2g1,
            new String[] { "myColumn", "myColumn2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "12968");
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

        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((Expression)command.getProjectedSymbols().get(0)).getType());
    }

    @Test public void testUnionQueryClone() throws Exception{
        SetQuery command = (SetQuery)helpResolve("SELECT e2, e3 FROM pm1.g1 UNION SELECT e3, e2 from pm1.g1"); //$NON-NLS-1$

        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((Expression)command.getProjectedSymbols().get(1)).getType());

        command = (SetQuery)command.clone();

        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((Expression)command.getProjectedSymbols().get(1)).getType());
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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        AnalysisRecord analysis = AnalysisRecord.createNonRecordingRecord();

        Query query = (Query) helpResolve(userSql, metadata);
        From from = query.getFrom();
        Collection fromClauses = from.getClauses();
        SPParameter params[] = new SPParameter[2];
        Iterator iter = fromClauses.iterator();
        while(iter.hasNext()) {
            SubqueryFromClause clause = (SubqueryFromClause) iter.next();
            StoredProcedure proc = (StoredProcedure) clause.getCommand();
            for (SPParameter param : proc.getParameters()) {
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

        QueryResolver.resolveCommand(query, RealMetadataFactory.exampleBQTCached());

        // Check type of resolved null constant
        Expression symbol = query.getSelect().getSymbols().get(0);
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
        QueryResolver.resolveCommand(command, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testParameterError() throws Exception {
        helpResolveException("EXEC pm1.sp2(1, 2)", metadata, "TEIID31113 1 extra positional parameter(s) passed to pm1.sp2."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUnionOfAliasedLiteralsGetsModified() {
        String sql = "SELECT 5 AS x UNION ALL SELECT 10 AS x"; //$NON-NLS-1$
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
            assertEquals("Found type mismatch at column " + i, types[i], ((Expression) projSymbols.get(i)).getType()); //$NON-NLS-1$
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
        verifyProjectedTypes(c, new Class[] { BigDecimal.class });

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

        helpResolveException(procedure, "TEIID31118 Element \"VARIABLES.X\" is not defined by any relevant group."); //$NON-NLS-1$
    }

    @Test public void testCreate() {
        String sql = "CREATE LOCAL TEMPORARY TABLE temp_table (column1 string)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
    }

    @Test public void testCreateQualifiedName() {
        String sql = "CREATE LOCAL TEMPORARY TABLE \"my.g1\" (column1 string)"; //$NON-NLS-1$
        helpResolve(sql); //$NON-NLS-1$
    }

    @Test public void testProcedureConflict() {
        String sql = "create local temporary table MMSP6 (e1 string, e2 integer)"; //$NON-NLS-1$
        helpResolveException(sql, RealMetadataFactory.exampleBQTCached()); //$NON-NLS-1$
    }

    @Test public void testCreatePk() {
        String sql = "CREATE LOCAL TEMPORARY TABLE foo (column1 string, column2 integer, primary key (column1, column2))"; //$NON-NLS-1$
        helpResolve(sql);
    }

    @Test public void testCreateUnknownPk() {
        String sql = "CREATE LOCAL TEMPORARY TABLE foo (column1 string, primary key (column2))"; //$NON-NLS-1$
        helpResolveException(sql, "TEIID31118 Element \"column2\" is not defined by any relevant group."); //$NON-NLS-1$
    }

    @Test public void testCreateAlreadyExists() {
        String sql = "CREATE LOCAL TEMPORARY TABLE g1 (column1 string)"; //$NON-NLS-1$
        helpResolveException(sql, "TEIID30118 Cannot create temporary table \"g1\". An object with the same name already exists."); //$NON-NLS-1$
    }

    @Test public void testCreateImplicitName() {
        String sql = "CREATE LOCAL TEMPORARY TABLE #g1 (column1 string)"; //$NON-NLS-1$
        Command c = helpResolve(sql);
        assertEquals(sql, c.toString());
    }

    @Test public void testCreateInProc() throws Exception{
        helpResolveException("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table g1(c1 string); end", "TEIID30118 Cannot create temporary table \"g1\". An object with the same name already exists.");//$NON-NLS-1$ //$NON-NLS-2$
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

        helpResolveException(proc, RealMetadataFactory.exampleBitwise(), "Group does not exist: #temp"); //$NON-NLS-1$
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
            assertEquals("TEIID31119 Symbol e1 is specified with an unknown group context", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testSameNameRoot() {
        String sql = "select p.e1 from pm1.g1 as pp, pm1.g1 as p"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testBatchedUpdateResolver() throws Exception {
        String update1 = "update pm1.g1 set e1 =1"; //$NON-NLS-1$
        String update2 = "update pm2.g1 set e1 =1"; //$NON-NLS-1$

        List<Command> commands = new ArrayList<Command>();
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

        helpResolveException(sql, metadata, "TEIID31118 Element \"ROWS_UPDATED\" is not defined by any relevant group."); //$NON-NLS-1$
    }

    /**
     *  We could check to see if the expressions are evaluatable to a constant, but that seems unnecessary
     */
    @Test public void testLookupWithoutConstant() throws Exception{
        String sql = "SELECT lookup('pm1.g1', convert('e3', float), 'e2', e2) FROM pm1.g1"; //$NON-NLS-1$

        helpResolveException(sql, metadata, "TEIID30095 The first three arguments for the LOOKUP function must be specified as constants."); //$NON-NLS-1$
    }

    @Test public void testPowerWithBigInteger() throws Exception {
        String sql = "SELECT power(10, 999999999999999999999999999999999999999999999)"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testPowerWithLong() throws Exception {
        String sql = "SELECT power(10, 999999999999)"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testExecProjectedSymbols() {
        String query = "exec pm1.sq1()"; //$NON-NLS-1$

        StoredProcedure proc = (StoredProcedure)helpResolve(query);

        List<Expression> projected = proc.getProjectedSymbols();

        assertEquals(2, projected.size());

        for (Iterator<Expression> i = projected.iterator(); i.hasNext();) {
            ElementSymbol symbol = (ElementSymbol)i.next();
            assertNotNull(symbol.getGroupSymbol());
        }
    }

    @Test public void testExecWithDuplicateNames() {
        MetadataStore metadataStore = new MetadataStore();

        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore);

        ColumnSet<Procedure> rs2 = RealMetadataFactory.createResultSet("rs2", new String[] { "in", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs2p2 = RealMetadataFactory.createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        Procedure sq2 = RealMetadataFactory.createStoredProcedure("sq2", pm1, Arrays.asList(rs2p2));  //$NON-NLS-1$
        sq2.setResultSet(rs2);

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "example1");

        helpResolveException("select * from pm1.sq2", metadata, "TEIID30114 Cannot access procedure pm1.sq2 using table semantics since the parameter and result set column names are not all unique."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInlineViewNullLiteralInUnion() {
        String sql = "select e2 from pm1.g1 union all (select x from (select null as x) y)"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testSelectIntoWithDuplicateNames() {
        String sql = "select 1 as x, 2 as x into #temp"; //$NON-NLS-1$

        helpResolveException(sql, "TEIID30091 Cannot create group '#temp' with multiple columns named 'x'"); //$NON-NLS-1$
    }

    @Test public void testCreateWithDuplicateNames() {
        String sql = "CREATE LOCAL TEMPORARY TABLE temp_table (column1 string, column1 string)"; //$NON-NLS-1$

        helpResolveException(sql, "TEIID30091 Cannot create group \'temp_table\' with multiple columns named \'column1\'"); //$NON-NLS-1$
    }

    @Test public void testSelectIntoWithOrderBy() {
        String sql = "select e1, e2 into #temp from pm1.g1 order by e1 limit 10"; //$NON-NLS-1$

        helpResolve(sql);
    }

    @Test public void testUnionBranchesWithDifferentElementCounts() {
        helpResolveException("SELECT e2, e3 FROM pm1.g1 UNION SELECT e2 FROM pm1.g2","TEIID30147 Queries combined with the set operator UNION must have the same number of output elements."); //$NON-NLS-1$ //$NON-NLS-2$
        helpResolveException("SELECT e2 FROM pm1.g1 UNION SELECT e2, e3 FROM pm1.g2","TEIID30147 Queries combined with the set operator UNION must have the same number of output elements."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectIntoWithNullLiteral() {
        String sql = "select null as x into #temp from pm1.g1"; //$NON-NLS-1$

        Query query = (Query)helpResolve(sql);

        TempMetadataStore store = query.getTemporaryMetadata();

        TempMetadataID id = store.getTempElementID("#temp.x"); //$NON-NLS-1$

        assertEquals(DataTypeManager.DefaultDataClasses.STRING, id.getType());
    }

    @Test public void testInsertWithNullLiteral() {
        String sql = "insert into #temp (x) values (null)"; //$NON-NLS-1$

        Insert insert = (Insert)helpResolve(sql);

        TempMetadataStore store = insert.getTemporaryMetadata();

        TempMetadataID id = store.getTempElementID("#temp.x"); //$NON-NLS-1$

        assertEquals(DataTypeManager.DefaultDataClasses.STRING, id.getType());
    }

    @Test public void testInsertWithoutColumnsFails() {
        String sql = "Insert into pm1.g1 values (1, 2)"; //$NON-NLS-1$

        helpResolveException(sql, "TEIID30127 INSERT statement must have the same number of elements and values specified.  This statement has 4 elements and 2 values."); //$NON-NLS-1$
    }

    @Test public void testInsertWithoutColumnsFails1() {
        String sql = "Insert into pm1.g1 values (1, 2, 3, 4)"; //$NON-NLS-1$

        helpResolveException(sql, "TEIID30082 Expected value of type 'boolean' but '3' is of type 'integer' and no implicit conversion is available."); //$NON-NLS-1$
    }

    @Test public void testInsertWithQueryFails() {
        String sql = "Insert into pm1.g1 select 1, 2, 3, 4"; //$NON-NLS-1$

        helpResolveException(sql, "TEIID30128 Cannot convert insert query expression projected symbol '3' of type java.lang.Integer to insert column 'pm1.g1.e3' of type java.lang.Boolean"); //$NON-NLS-1$
    }

    @Test public void testInsertWithQueryImplicitWithColumns() {
        String sql = "Insert into #X (x) select 1 as x"; //$NON-NLS-1$
        helpResolve(sql); //$NON-NLS-1$
    }

    @Test public void testInsertWithQueryImplicitWithoutColumns() {
        String sql = "Insert into #X select 1 as x, 2 as y, 3 as z"; //$NON-NLS-1$
        helpResolve(sql); //$NON-NLS-1$
    }

    @Test public void testInsertWithQueryImplicitWithoutColumns1() {
        String sql = "Insert into #X select 1 as x, 2 as y, 3 as y"; //$NON-NLS-1$

        helpResolveException(sql, "TEIID30091 Cannot create group '#X' with multiple columns named 'y'"); //$NON-NLS-1$
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

        String sExpected = "BEGIN\nCREATE LOCAL TEMPORARY TABLE #matt (x integer);\nINSERT INTO #matt (x) VALUES (1);\nEND\n\tCREATE LOCAL TEMPORARY TABLE #matt (x integer)\n\tINSERT INTO #matt (x) VALUES (1)\n";   //$NON-NLS-1$
        String sActual = cmd.printCommandTree();
        assertEquals( sExpected, sActual );
    }

    @Test public void testCase6319() throws QueryResolverException, TeiidComponentException {
        String sql = "select floatnum from bqt1.smalla group by floatnum having sum(floatnum) between 51.0 and 100.0 "; //$NON-NLS-1$
        Query query = (Query)helpParse(sql);
        QueryResolver.resolveCommand(query, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testUniqeNamesWithInlineView() {
        helpResolveException("select * from (select count(intNum) a, count(stringKey) b, bqt1.smalla.intkey as b from bqt1.smalla group by bqt1.smalla.intkey) q1 order by q1.a", RealMetadataFactory.exampleBQTCached(), "TEIID30091 Cannot create group 'q1' with multiple columns named 'b'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testResolveOldProcRelational() {
        helpResolveException("SELECT * FROM pm1.g1, (exec pm1.sq2(pm1.g1.e1)) as a", "TEIID31119 Symbol pm1.g1.e1 is specified with an unknown group context"); //$NON-NLS-1$  //$NON-NLS-2$
    }

    @Test public void testResolverOrderOfPrecedence() {
        helpResolveException("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 CROSS JOIN (pm1.g2 LEFT OUTER JOIN pm2.g1 on pm1.g1.e1 = pm2.g1.e1)", "TEIID31119 Symbol pm1.g1.e1 is specified with an unknown group context"); //$NON-NLS-1$  //$NON-NLS-2$
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

        TestResolver.helpResolveException(sql, RealMetadataFactory.exampleBQTCached(), "TEIID31113 1 extra positional parameter(s) passed to pm4.spTest9."); //$NON-NLS-1$
    }

    @Test public void testUpdateAlias() {
        String sql = "UPDATE pm1.g1 as x SET x.e1 = 1 where x.e2 = 2;"; //$NON-NLS-1$

        Update update = (Update)helpResolve(sql, RealMetadataFactory.example1Cached());

        assertEquals("UPDATE pm1.g1 AS x SET e1 = 1 WHERE x.e2 = 2", update.toString());
    }

    @Test public void testDeleteAlias() {
        String sql = "DELETE from pm1.g1 as x where (select e2 from pm1.g2 where x.e1 = e1) = 2;"; //$NON-NLS-1$

        Delete update = (Delete)helpResolve(sql, RealMetadataFactory.example1Cached());

        assertEquals("DELETE FROM pm1.g1 AS x WHERE (SELECT e2 FROM pm1.g2 WHERE x.e1 = e1) = 2", update.toString());
    }

    @Test public void testUpdateSetClauseReferenceType() {
        String sql = "UPDATE pm1.g1 SET pm1.g1.e1 = 1, pm1.g1.e2 = ?;"; //$NON-NLS-1$

        Update update = (Update)helpResolve(sql, RealMetadataFactory.example1Cached());

        Expression ref = update.getChangeList().getClauses().get(1).getValue();
        assertTrue(ref instanceof Reference);
        assertNotNull(ref.getType());
    }

    @Test public void testNoTypeCriteria() {
        String sql = "select * from pm1.g1 where ? = ?"; //$NON-NLS-1$

        helpResolveException(sql, RealMetadataFactory.example1Cached(), "TEIID30083 Expression '? = ?' has a parameter with non-determinable type information.  The use of an explicit convert may be necessary."); //$NON-NLS-1$
    }

    @Test public void testReferenceInSelect() {
        String sql = "select ?, e1 from pm1.g1"; //$NON-NLS-1$
        Query command = (Query)helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, command.getProjectedSymbols().get(0).getType());
    }

    @Test public void testReferenceInSelect1() {
        String sql = "select convert(?, integer), e1 from pm1.g1"; //$NON-NLS-1$

        Query command = (Query)helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, command.getProjectedSymbols().get(0).getType());
    }

    @Test public void testUnionWithObjectTypeConversion() {
        String sql = "select convert(null, xml) from pm1.g1 union all select 1"; //$NON-NLS-1$

        SetQuery query = (SetQuery)helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.OBJECT, ((Expression)query.getProjectedSymbols().get(0)).getType());
    }

    @Test public void testUnionWithSubQuery() {
        String sql = "select 1 from pm1.g1 where exists (select 1) union select 2"; //$NON-NLS-1$

        SetQuery command = (SetQuery)helpResolve(sql);

        assertEquals(1, CommandCollectorVisitor.getCommands(command).size());
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
        helpResolveException("SELECT pm1.g1.e1, e2 as x, e3 as x FROM pm1.g1 ORDER BY x, e1 ", "TEIID30084 Element 'x' in ORDER BY is ambiguous and may refer to more than one element of SELECT clause."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    @Test public void testOrderBy_J658e() {
        Query resolvedQuery = (Query) helpResolve("SELECT pm1.g1.e1, e2 as x, e3 as e2 FROM pm1.g1 ORDER BY x, e2 "); //$NON-NLS-1$
        helpTestOrderBy(resolvedQuery.getOrderBy(), new int[] {1, 2});
    }

    @Test public void testSPOutParamWithExec() {
        StoredProcedure proc = (StoredProcedure)helpResolve("exec pm2.spTest8(1)", RealMetadataFactory.exampleBQTCached());
        assertEquals(2, proc.getProjectedSymbols().size());
    }

    /**
     * Note that the call syntax is not quite correct, the output parameter is not in the arg list.
     * That hack is handled by the PreparedStatementRequest
     */
    @Test public void testSPOutParamWithCallableStatement() {
        StoredProcedure proc = (StoredProcedure)helpResolve("{call pm2.spTest8(1)}", RealMetadataFactory.exampleBQTCached());
        assertEquals(3, proc.getProjectedSymbols().size());
    }

    @Test public void testOutWithWrongType() {
        helpResolveException("exec pm2.spTest8(inkey=>1, outkey=>{t '12:00:00'})", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testProcRelationalWithOutParam() {
        Query proc = (Query)helpResolve("select * from pm2.spTest8 where inkey = 1", RealMetadataFactory.exampleBQTCached());
        assertEquals(3, proc.getProjectedSymbols().size());
    }

    @Test public void testSPReturnParamWithNoResultSet() {
        StoredProcedure proc = (StoredProcedure)helpResolve("exec pm4.spTest9(1)", RealMetadataFactory.exampleBQTCached());
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
            assertEquals("TEIID30070", qre.getCode());
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

    @Test public void testOrderByExpression() {
        Query query = (Query)helpResolve("select pm1.g1.e1 from pm1.g1 order by e2 || e3 "); //$NON-NLS-1$
        assertEquals(-1, query.getOrderBy().getExpressionPosition(0));
    }

    @Test public void testOrderByExpression1() {
        Query query = (Query)helpResolve("select pm1.g1.e1 || e2 from pm1.g1 order by pm1.g1.e1 || e2 "); //$NON-NLS-1$
        assertEquals(0, query.getOrderBy().getExpressionPosition(0));
    }

    @Test public void testOrderByExpression2() {
        helpResolveException("select pm1.g1.e1 from pm1.g1 union select pm1.g2.e1 from pm1.g2 order by pm1.g1.e1 || 2", "TEIID30086 ORDER BY expression '(pm1.g1.e1 || 2)' cannot be used with a set query."); //$NON-NLS-1$ //$NON-NLS-2$
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

    CreateProcedureCommand helpResolveUpdateProcedure(String procedure,
            String userUpdateStr) throws QueryParserException,
            QueryResolverException, TeiidComponentException,
            QueryMetadataException {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleUpdateProc(TriggerEvent.UPDATE, procedure);

        ProcedureContainer userCommand = (ProcedureContainer)QueryParser.getQueryParser().parseCommand(userUpdateStr);
        QueryResolver.resolveCommand(userCommand, metadata);

        return (CreateProcedureCommand)QueryResolver.expandCommand(userCommand, metadata, AnalysisRecord.createNonRecordingRecord());
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
        Query resolvedQuery = (Query)helpResolve("SELECT * FROM (exec pm4.spRetOut()) as a", RealMetadataFactory.exampleBQTCached()); //$NON-NLS-1$

        assertEquals("a.ret", resolvedQuery.getProjectedSymbols().get(0).toString());
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

    @Test public void testTrim() {
        Query query = (Query)helpResolve("select trim(e1) from pm1.g1");
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, query.getProjectedSymbols().get(0).getType());
    }

    @Test public void testTrim1() {
        helpResolve("select trim('x' from e1) from pm1.g1");
    }

    @Test public void testXmlTableWithParam() {
        //xml dependency not included by default
        helpResolveException("select * from xmltable('/a' passing ?) as x");
    }

    @Test public void testObjectTableWithParam() {
        helpResolve("select * from objecttable('x + 1' passing ? as x columns obj OBJECT '') as y");
    }

    @Test public void testImplicitTempTableWithExplicitColumns() {
        helpResolve("insert into #temp(x, y) select e1, e2 from pm1.g1");
    }

    @Test public void testArrayCase() {
        Command c = helpResolve("select case when e1 is null then array_agg(e4) when e2 is null then array_agg(e4+1) end from pm1.g1 group by e1, e2");
        assertTrue(c.getProjectedSymbols().get(0).getType().isArray());
    }

    @Test public void testArrayCase1() {
        Command c = helpResolve("select case when e1 is null then array_agg(e1) when e2 is null then array_agg(e4+1) end from pm1.g1 group by e1, e2");
        assertTrue(c.getProjectedSymbols().get(0).getType().isArray());
    }

    @Test public void testForeignTempInvalidModel() {
        String sql = "create foreign temporary table x (y string) on x"; //$NON-NLS-1$
        helpResolveException(sql, "TEIID31134 Could not create foreign temporary table, since schema x does not exist."); //$NON-NLS-1$
    }

    @Test public void testForeignTempInvalidModel1() {
        String sql = "create foreign temporary table x (y string) on vm1"; //$NON-NLS-1$
        helpResolveException(sql, "TEIID31135 Could not create foreign temporary table, since schema vm1 is not physical."); //$NON-NLS-1$
    }

    @Test public void testAvgVarchar() {
        String sql = "SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING avg(e1) = '1'";
        helpResolve(sql);
    }

    @Test public void testAvgVarchar1() {
        String sql = "SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING avg(e1) between 1 and 2";
        helpResolve(sql);
    }

    @Test public void testInvalidDateLiteral() {
        helpTestWidenToString("select * from bqt1.smalla where timestampvalue > 'a'");
    }

    @Test public void testInvalidDateLiteral1() {
        helpTestWidenToString("select * from bqt1.smalla where timestampvalue between 'a' and 'b'");
    }

    @Test public void testDateNullBetween() {
        helpResolve("select * from bqt1.smalla where null between timestampvalue and null", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testNullComparison() {
        helpResolve("select * from bqt1.smalla where null > null", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testNullIn() {
        helpResolve("select * from bqt1.smalla where null in (timestampvalue, null)", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testNullIn1() {
        helpResolve("select * from bqt1.smalla where timestampvalue in (null, null)", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testCharToStringComparison() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolve("select * from bqt1.smalla where CharValue = ''", tm);
    }

    @Test public void testCharToStringComparisona() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where CharValue = 'a b'", tm);
        //assertEquals(DataTypeManager.DefaultDataClasses.STRING, ((CompareCriteria)query.getCriteria()).getLeftExpression().getType());
    }

    @Test public void testCharToStringComparisonb() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where CharValue = StringKey", tm);
    }

    @Test public void testCharToStringComparison1() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        Query query = (Query)helpResolve("select * from bqt1.smalla where '' = CharValue", tm);
        assertEquals(DataTypeManager.DefaultDataClasses.CHAR, ((CompareCriteria)query.getCriteria()).getLeftExpression().getType());
    }

    @Test public void testCharToStringComparison1a() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where 'a b' = CharValue", tm);
    }

    @Test public void testCharToStringComparison1b() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where StringKey = CharValue", tm);
    }

    @Test public void testCharToStringComparison2() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolve("select * from bqt1.smalla where CharValue in ('')", tm);
    }

    @Test public void testCharToStringComparison2a() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where CharValue in ('a b')", tm);
        //assertEquals(DataTypeManager.DefaultDataClasses.STRING, ((SetCriteria)query.getCriteria()).getExpression().getType());
    }

    @Test public void testCharToStringComparison3() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        Query query = (Query)helpResolve("select * from bqt1.smalla where '' in (CharValue)", tm);
        assertEquals(DataTypeManager.DefaultDataClasses.CHAR, ((SetCriteria)query.getCriteria()).getExpression().getType());
    }

    @Test public void testCharToStringComparison3a() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        Query query = (Query)helpResolve("select * from bqt1.smalla where 'a  ' in (CharValue)", tm);
        assertEquals(DataTypeManager.DefaultDataClasses.CHAR, ((SetCriteria)query.getCriteria()).getExpression().getType());
    }

    @Test public void testCharToStringComparison3b() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where 'a b' in (CharValue)", tm);
        //assertEquals(DataTypeManager.DefaultDataClasses.CHAR, ((SetCriteria)query.getCriteria()).getExpression().getType());
    }

    @Test public void testCharToStringComparison3c() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where StringKey in (CharValue)", tm);
    }

    @Test public void testCharToStringComparison3d() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where CharValue in (StringKey)", tm);
    }

    @Test public void testCharToStringComparison4() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolve("select * from bqt1.smalla where 'a' = CharValue", tm);
    }

    @Test public void testCharToStringComparison4a() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where 'a b' = CharValue", tm);
        //assertEquals(DataTypeManager.DefaultDataClasses.STRING, ((CompareCriteria)query.getCriteria()).getLeftExpression().getType());
    }

    @Test public void testInvalidComparison() {
        helpTestWidenToString("select * from bqt1.smalla where timestampvalue > stringkey");
    }

    @Test public void testInvalidComparison1() {
        helpTestWidenToString("select * from bqt1.smalla where stringkey > 1000");
    }

    @Test public void testInvalidIn() {
        helpTestWidenToString("select * from bqt1.smalla where stringkey in (timestampvalue, 1)");
    }

    @Test public void testInvalidIn1() {
        helpTestWidenToString("select * from bqt1.smalla where timestampvalue in (stringkey, 1)");
    }

    @Test public void testInvalidIn2() {
        helpTestWidenToString("select * from bqt1.smalla where timestampvalue in (select stringkey from bqt1.smallb)");
    }

    @Test public void testTimestampDateLiteral() {
        metadata = RealMetadataFactory.exampleBQTCached();
        Criteria crit = helpResolveCriteria("bqt1.smalla.timestampvalue = '2000-01-01'");
        assertTrue(((CompareCriteria)crit).getRightExpression().getType() == DataTypeManager.DefaultDataClasses.TIMESTAMP);
        assertEquals("bqt1.smalla.timestampvalue = {ts'2000-01-01 00:00:00.0'}", crit.toString());
    }

    @Test public void testIncompleteTimestampDateLiteral() {
        metadata = RealMetadataFactory.exampleBQTCached();
        Criteria crit = helpResolveCriteria("bqt1.smalla.timestampvalue = '2000-01-01 01:02'");
        assertTrue(((CompareCriteria)crit).getRightExpression().getType() == DataTypeManager.DefaultDataClasses.TIMESTAMP);
        assertEquals("bqt1.smalla.timestampvalue = {ts'2000-01-01 01:02:00.0'}", crit.toString());
    }

    @Test public void testIncompleteTimestampDateLiteral2() {
        metadata = RealMetadataFactory.exampleBQTCached();
        Criteria crit = helpResolveCriteria("bqt1.smalla.datevalue = '2000-01-01 00:00'");
        assertTrue(((CompareCriteria)crit).getRightExpression().getType() == DataTypeManager.DefaultDataClasses.DATE);
        assertEquals("bqt1.smalla.datevalue = {d'2000-01-01'}", crit.toString());
    }

    @Test public void testCharInString() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolve("select * from bqt1.smalla where bqt1.smalla.charValue in ('a', 'b')", tm);
    }

    @Test public void testStringInChar() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolve("select * from bqt1.smalla where 'a' in (bqt1.smalla.charValue, cast('a' as char))", tm);
    }

    @Test public void testCharBetweenString() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolve("select * from bqt1.smalla where bqt1.smalla.charValue between 'a' and 'b'", tm);
    }

    @Test public void testCharBetweenString1() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        Query query = (Query)helpResolve("select * from bqt1.smalla where bqt1.smalla.charValue between 'a ' and 'b'", tm);
        assertEquals(DataTypeManager.DefaultDataClasses.CHAR, ((BetweenCriteria)query.getCriteria()).getExpression().getType());
    }

    @Test public void testCharBetweenString2() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where bqt1.smalla.charValue between bqt1.smalla.stringkey and 'b'", tm);
    }

    @Test public void testCharSubqueryCompareString() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException("select * from bqt1.smalla where bqt1.smalla.charValue = SOME (select stringkey from bqt1.smallb)", tm);
    }

    @Test public void testCharCompareString() {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        Command c = helpResolve("select * from bqt1.smalla where bqt1.smalla.charValue = 'a'", tm);
        Query q = (Query)c;
        assertTrue(((CompareCriteria)q.getCriteria()).getLeftExpression() instanceof ElementSymbol);
    }

    @Test public void testSelectAllOrder() {
        Query q = (Query)helpResolve("select * from pm1.g1, pm1.g2");
        assertEquals("[pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4]", q.getProjectedSymbols().toString());

        q = (Query)helpResolve("select * from pm1.g1 cross join pm1.g2");
        assertEquals("[pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4]", q.getProjectedSymbols().toString());

        q = (Query)helpResolve("select * from pm1.g1, pm1.g2 inner join pm1.g3 on (pm1.g2.e1 = pm1.g3.e1)");
        assertEquals("[pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4, pm1.g3.e1, pm1.g3.e2, pm1.g3.e3, pm1.g3.e4]", q.getProjectedSymbols().toString());
    }

    @Test public void testSelectAllOrderCommonTable() {
        Query q = (Query)helpResolve("with x as (select 1 y) select * from pm1.g1, pm1.g2");
        assertEquals("[pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4]", q.getProjectedSymbols().toString());
    }

    @Test public void testLeadOffset() {
        //must be integer
        String sql = "SELECT LEAD(e1, 'a') over (order by e2) FROM pm1.g1";
        helpResolveException(sql);
    }

    @Test public void testLeadDefault() {
        //must have same type
        String sql = "SELECT LEAD(e2, 1, 'a') over (order by e2) FROM pm1.g1";
        helpResolveException(sql);
    }

    @Test public void testNtileArg() {
        //must be integer
        String sql = "SELECT ntile('a') over (order by e2) FROM pm1.g1";
        helpResolveException(sql);
    }

    @Test public void testNtileArgs() {
        //must be integer
        String sql = "SELECT ntile(1,2) over (order by e2) FROM pm1.g1";
        helpResolveException(sql);
    }

    @Test public void testNthValueArg() {
        //needs two args
        String sql = "SELECT Nth_Value('a') over (order by e2) FROM pm1.g1";
        helpResolveException(sql);
    }

    @Test public void testNthValueArgType() {
        //needs integer
        String sql = "SELECT Nth_Value('a','a') over (order by e2) FROM pm1.g1";
        helpResolveException(sql);
    }

    @Test public void testIsDistinctArray() {
        String sql = "('a', null) is distinct from ('b', null)";
        IsDistinctCriteria c = (IsDistinctCriteria)helpResolveCriteria(sql);
        assertEquals(String.class,((Array)c.getLeftRowValue()).getComponentType());
    }

    @Test public void testIsDistinctTypeMismatch() {
        String sql = "select TIME '07:01:00' is distinct from 2";
        helpResolveException(sql);
    }

    @Test public void testSubqueryReferencingInlineView() throws Exception {
        String sql = "select a.a1 from (select 1 as a1) a where a.a1 in (select a1 from a)";

        helpResolveException(sql);
    }

    @Test public void testTableAliasString() throws Exception {
        String sql = "select \"pm1g2\".* from pm1.g1 as \"pm1g2\"";
        Query query = (Query)helpResolve(sql);
        UnaryFromClause ufc = (UnaryFromClause)query.getFrom().getClauses().get(0);
        GroupSymbol gs = ufc.getGroup();
        assertEquals("pm1g2", gs.getName());
        assertEquals("SELECT pm1g2.* FROM pm1.g1 AS pm1g2", query.toString());
    }

    /**
     * This is not correct behavior, but it is acceptable to prevent other issues
     */
    @Test public void testTableAliasWithPeriodAmbiguous() throws Exception {
        String sql = "select \"pm1.g2\".*, pm1.g2.* from pm1.g1 as \"pm1.g2\", pm1.g2";
        helpResolveException(sql);
    }

    @Test public void testTableAliasWithPeriod() throws Exception {
        String sql = "select \"pm1.g2\".*, e1, \"pm1.g2\".e2, pm1.g2.e2 from pm1.g1 as \"pm1.g2\"";
        Query query = (Query)helpResolve(sql);
        UnaryFromClause ufc = (UnaryFromClause)query.getFrom().getClauses().get(0);
        GroupSymbol gs = ufc.getGroup();
        assertEquals("pm1.g2", gs.getName());
        assertEquals("pm1.g1", gs.getDefinition());
        assertFalse(gs.isTempTable());
        assertEquals("SELECT \"pm1.g2\".*, e1, \"pm1.g2\".e2, \"pm1.g2\".e2 FROM pm1.g1 AS \"pm1.g2\"", query.toString());
        assertEquals("[\"pm1.g2\".e1, \"pm1.g2\".e2, \"pm1.g2\".e3, \"pm1.g2\".e4, e1, \"pm1.g2\".e2, \"pm1.g2\".e2]", query.getProjectedSymbols().toString());
    }

    @Test public void testTableAliasWithMultiplePeriods() throws Exception {
        String sql = "select \"pm1..g2\".e1 from pm1.g1 as \"pm1..g2\"";
        Query query = (Query)helpResolve(sql);
        UnaryFromClause ufc = (UnaryFromClause)query.getFrom().getClauses().get(0);
        GroupSymbol gs = ufc.getGroup();
        assertEquals("pm1..g2", gs.getName());
        assertEquals("pm1.g1", gs.getDefinition());
        assertEquals("SELECT \"pm1..g2\".e1 FROM pm1.g1 AS \"pm1..g2\"", query.toString());
        assertEquals("[\"pm1..g2\".e1]", query.getProjectedSymbols().toString());
    }

    @Test public void testSubqueryAliasWithPeriod() throws Exception {
        String sql = "select \"pm1.g2\".x from (select 1 as x) as \"pm1.g2\"";
        Query query = (Query)helpResolve(sql);
        SubqueryFromClause sfc = (SubqueryFromClause)query.getFrom().getClauses().get(0);
        GroupSymbol gs = sfc.getGroupSymbol();
        assertEquals("pm1.g2", gs.getName());
        assertNull(gs.getDefinition());
        assertEquals("SELECT \"pm1.g2\".x FROM (SELECT 1 AS x) AS \"pm1.g2\"", query.toString());
        assertEquals("SELECT \"pm1.g2\".x FROM (SELECT 1 AS x) AS \"pm1.g2\"", query.clone().toString());
        assertEquals("[\"pm1.g2\".x]", query.getProjectedSymbols().toString());
    }

    @Test public void testArrayFromQuery() throws Exception {
        Command command = helpResolve("select array(select 1)"); //$NON-NLS-1$
        assertEquals(Integer[].class, command.getProjectedSymbols().get(0).getType());
    }

    @Test public void testTextTableAliasWithPeriod() throws Exception {
        Command command = helpResolve("select \"x.y.z\".*, \"x.y.z\".x  from pm1.g1, texttable(e1 COLUMNS x string) \"x.y.z\""); //$NON-NLS-1$
        assertEquals(2, command.getProjectedSymbols().size());
        assertEquals("SELECT \"x.y.z\".*, \"x.y.z\".x FROM pm1.g1, TEXTTABLE(e1 COLUMNS x string) AS \"x.y.z\"", command.toString());
        assertEquals("SELECT \"x.y.z\".*, \"x.y.z\".x FROM pm1.g1, TEXTTABLE(e1 COLUMNS x string) AS \"x.y.z\"", command.clone().toString());
    }

    private void helpTestWidenToString(String sql) {
        TransformationMetadata tm = RealMetadataFactory.exampleBQTCached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        helpResolveException(sql, tm);
        tm.setWidenComparisonToString(true);
        helpResolve(sql, tm);
    }

    @Test public void testHiddenTable() {
        helpTestHiddenNotResolvable("select * from bqt1.smalla");
    }

    @Test public void testHiddenProcedure() {
        helpTestHiddenNotResolvable("call bqt1.native('x')");
    }

    @Test public void testHiddenFunction() {
        helpTestHiddenNotResolvable("select bqt1.reverse('x')");
    }

    private void helpTestHiddenNotResolvable(String sql) {
        TransformationMetadata tm = RealMetadataFactory.exampleBQT();
        tm.getVdbMetaData().getModel("BQT1").setVisible(false);
        tm.setHiddenResolvable(false);
        helpResolveException(sql, tm);
        tm.setHiddenResolvable(true);
        helpResolve(sql, tm);
    }

    @Test public void testJsonTable() throws Exception {
        String sql = "SELECT * from jsontable('{}', '$..*', true columns x for ordinality, y json path '@..*') as x"; //$NON-NLS-1$
        //optional json support not in the core engine
        helpResolveException(sql);
    }

}