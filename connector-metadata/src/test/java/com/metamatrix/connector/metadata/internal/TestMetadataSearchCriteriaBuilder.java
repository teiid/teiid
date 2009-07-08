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

package com.metamatrix.connector.metadata.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.connector.language.ICommand;
import org.teiid.connector.metadata.MetadataLiteralCriteria;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.dqp.internal.datamgr.language.LanguageBridgeFactory;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;


/** 
 * @since 4.3
 */
public class TestMetadataSearchCriteriaBuilder extends TestCase {

    private RuntimeMetadata metadata;
    private CommandBuilder commandBuilder;
    QueryMetadataInterfaceBuilder builder = new QueryMetadataInterfaceBuilder();
    
    /**
     * Constructor for ObjectQueryTest.
     * @param name
     */
    public TestMetadataSearchCriteriaBuilder(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        builder.addPhysicalModel("system"); //$NON-NLS-1$
        builder.addGroup("t"); //$NON-NLS-1$
        builder.addElement("x", String.class); //$NON-NLS-1$
        builder.addElement("y", String.class); //$NON-NLS-1$
        builder.addElement("z", Integer.class);           //$NON-NLS-1$
        metadata = builder.getRuntimeMetadata();
        commandBuilder = new CommandBuilder(builder.getQueryMetadata());
    }
    
    private ObjectQuery getQuery(String queryText) throws Exception {
        return new ObjectQuery(metadata,commandBuilder.getCommand(queryText));
    }
    
    private ObjectQuery getQueryWithOutReWrite(String queryText) throws Exception {
        return new ObjectQuery(metadata,getCommandWithOutReWrite(queryText));
    }
    
    public ICommand getCommandWithOutReWrite(String queryString) throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand(queryString);
        QueryResolver.resolveCommand(command, builder.getQueryMetadata());
        expandAllSymbol(command);            

        ICommand result =  new LanguageBridgeFactory(builder.getQueryMetadata()).translate(command);
        return result;
    }
    
    /**
     * Convert the "*" in "select * from..." to the list of column names for the data source.
     */
    protected void expandAllSymbol(Command command) {
        if (command instanceof Query) {
            Query query = (Query) command;
            Select select = query.getSelect();
            List originalSymbols = select.getSymbols();
            List expandedSymbols = new ArrayList();
            for (Iterator i = originalSymbols.iterator(); i.hasNext(); ) {
                Object next = i.next();
                if (next instanceof AllSymbol) {
                    AllSymbol allSymbol = (AllSymbol) next;
                    expandedSymbols.addAll(allSymbol.getElementSymbols());
                } else {
                    expandedSymbols.add(next);
                }
            }
            select.setSymbols(expandedSymbols);
        }
    }    

    public void testGetBuildCompareCriteria1() throws Exception {
        ObjectQuery query = getQuery("select x from t where x = 'g'"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("g", criteria.getFieldValue()); //$NON-NLS-1$
    }
    
    public void testGetBuildCompareCriteria2() throws Exception {
        ObjectQuery query = getQuery("select x from t where 'g' = x"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("g", criteria.getFieldValue()); //$NON-NLS-1$
    }
    
    public void testGetBuildCompareCriteriaWithFunction1() throws Exception {
        ObjectQuery query = getQueryWithOutReWrite("select x from t where x = UPPER('g')"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("G", criteria.getFieldValue()); //$NON-NLS-1$
        assertEquals("UPPER", criteria.getValueFunction()); //$NON-NLS-1$
    }
    
    public void testGetBuildCompareCriteriaWithFunction2() throws Exception {
        ObjectQuery query = getQueryWithOutReWrite("select x from t where UCASE(x) = UPPER('g')"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("G", criteria.getFieldValue()); //$NON-NLS-1$
        assertEquals("UCASE", criteria.getFieldFunction()); //$NON-NLS-1$
        assertEquals("UPPER", criteria.getValueFunction()); //$NON-NLS-1$
    }
    
    public void testGetBuildLikeCriteria1() throws Exception {
        ObjectQuery query = getQuery("select x from t where x Like 'g'"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("g", criteria.getFieldValue()); //$NON-NLS-1$
    }
    
    public void testGetBuildLikeWildCardCriteria1() throws Exception {
        ObjectQuery query = getQuery("select x from t where x Like '%g'"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("*g", criteria.getFieldValue()); //$NON-NLS-1$
    }
    
    public void testGetBuildLikeWildCardCriteria2() throws Exception {
        ObjectQuery query = getQuery("select x from t where x Like '%g?'"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("*g?", criteria.getFieldValue()); //$NON-NLS-1$
    }    
    
    public void testGetBuildLikeCriteriaWithFunction1() throws Exception {
        ObjectQuery query = getQueryWithOutReWrite("select x from t where x Like UPPER('g')"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("G", criteria.getFieldValue()); //$NON-NLS-1$
        assertEquals("UPPER", criteria.getValueFunction()); //$NON-NLS-1$
    }
    
    public void testGetBuildCompareLikeWithFunction2() throws Exception {
        ObjectQuery query = getQueryWithOutReWrite("select x from t where UCASE(x) Like UPPER('g')"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria.getFieldName()); //$NON-NLS-1$
        assertEquals("G", criteria.getFieldValue()); //$NON-NLS-1$
        assertEquals("UCASE", criteria.getFieldFunction()); //$NON-NLS-1$
        assertEquals("UPPER", criteria.getValueFunction()); //$NON-NLS-1$
    }
    
    public void testGetBuildCompoundCriteria2() throws Exception {
        ObjectQuery query = getQuery("select x from t where x Like '%g?' and y = 1"); //$NON-NLS-1$
        MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(query);
        Map criteriaMap = builder.getCriteria();
        MetadataLiteralCriteria criteria1 = (MetadataLiteralCriteria) criteriaMap.get("x".toUpperCase()); //$NON-NLS-1$
        assertEquals("x", criteria1.getFieldName()); //$NON-NLS-1$
        assertEquals("*g?", criteria1.getFieldValue()); //$NON-NLS-1$
        
        MetadataLiteralCriteria criteria2 = (MetadataLiteralCriteria) criteriaMap.get("y".toUpperCase()); //$NON-NLS-1$
        assertEquals("y", criteria2.getFieldName()); //$NON-NLS-1$
        assertEquals("1", criteria2.getFieldValue()); //$NON-NLS-1$        
    }
    
}