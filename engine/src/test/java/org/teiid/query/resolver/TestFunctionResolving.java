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
import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.unittest.RealMetadataFactory;


public class TestFunctionResolving {

    @Test public void testResolveBadConvert() throws Exception {
        Function function = new Function("convert", new Expression[] {new Constant(new Character('a')), new Constant(DataTypeManager.DefaultDataTypes.DATE)}); //$NON-NLS-1$
        
        try {
            ResolverVisitor.resolveLanguageObject(function, RealMetadataFactory.example1Cached());
            fail("excpetion expected"); //$NON-NLS-1$
        } catch (QueryResolverException err) {
            assertEquals("Error Code:ERR.015.008.0037 Message:The conversion from char to date is not allowed.", err.getMessage()); //$NON-NLS-1$
        } 
    }
    
    @Test public void testResolvesClosestType() throws Exception {
        ElementSymbol e1 = new ElementSymbol("pm1.g1.e1"); //$NON-NLS-1$
        //dummy resolve to a byte
        e1.setType(DataTypeManager.DefaultDataClasses.BYTE);
        e1.setMetadataID(new Object()); 
        Function function = new Function("abs", new Expression[] {e1}); //$NON-NLS-1$
        
        ResolverVisitor.resolveLanguageObject(function, RealMetadataFactory.example1Cached());
        
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, function.getType());
    }
    
    @Test public void testResolveConvertReference() throws Exception {
        Function function = new Function("convert", new Expression[] {new Reference(0), new Constant(DataTypeManager.DefaultDataTypes.BOOLEAN)}); //$NON-NLS-1$
        
        ResolverVisitor.resolveLanguageObject(function, RealMetadataFactory.example1Cached());
        
        assertEquals(DataTypeManager.DefaultDataClasses.BOOLEAN, function.getType());
        assertEquals(DataTypeManager.DefaultDataClasses.BOOLEAN, function.getArgs()[0].getType());
    }
    
    @Test public void testResolveAmbiguousFunction() throws Exception {
        Function function = new Function("LCASE", new Expression[] {new Reference(0)}); //$NON-NLS-1$
        
        try {
        	ResolverVisitor.resolveLanguageObject(function, RealMetadataFactory.example1Cached());
            fail("excpetion expected"); //$NON-NLS-1$
        } catch (QueryResolverException err) {
            assertEquals("Error Code:ERR.015.008.0036 Message:The function 'LCASE(?)' has more than one possible signature.", err.getMessage()); //$NON-NLS-1$
        } 
    }
    
    @Test public void testResolveCoalesce() throws Exception {
    	String sql = "coalesce('', '')"; //$NON-NLS-1$
    	helpResolveFunction(sql);
    }
    
    @Test public void testResolveCoalesce1() throws Exception {
    	String sql = "coalesce('', '', '')"; //$NON-NLS-1$
    	helpResolveFunction(sql);
    }
    
    /**
     * Should resolve using varags logic
     */
    @Test public void testResolveCoalesce1a() throws Exception {
    	String sql = "coalesce('', '', '', '')"; //$NON-NLS-1$
    	helpResolveFunction(sql);
    }
    
    /**
     * Should resolve as 1 is implicitly convertable to string
     */
    @Test public void testResolveCoalesce2() throws Exception {
    	String sql = "coalesce('', 1, '', '')"; //$NON-NLS-1$
    	helpResolveFunction(sql);
    }
    
    @Test public void testResolveCoalesce3() throws Exception {
    	String sql = "coalesce('', 1, null, '')"; //$NON-NLS-1$
    	helpResolveFunction(sql);
    }
    
    @Test public void testResolveCoalesce4() throws Exception {
    	String sql = "coalesce({d'2009-03-11'}, 1)"; //$NON-NLS-1$
    	helpResolveFunction(sql);
    }

	private Function helpResolveFunction(String sql) throws QueryParserException,
			QueryResolverException, TeiidComponentException {
		Function func = (Function)getExpression(sql);
    	assertEquals(DataTypeManager.DefaultDataClasses.STRING, func.getType());
    	return func;
	}

	public static Expression getExpression(String sql) throws QueryParserException,
			TeiidComponentException, QueryResolverException {
		Expression func = QueryParser.getQueryParser().parseExpression(sql);
		ResolverVisitor.resolveLanguageObject(func, RealMetadataFactory.example1Cached());
		return func;
	}
	
	/**
	 * e1 is of type string, so 1 should be converted to string
	 * @throws Exception
	 */
    @Test public void testLookupTypeConversion() throws Exception {
    	String sql = "lookup('pm1.g1', 'e2', 'e1', 1)"; //$NON-NLS-1$
    	Function f = (Function)getExpression(sql);
    	assertEquals(DataTypeManager.DefaultDataClasses.STRING, f.getArg(3).getType());
    }
    
    @Test public void testXMLSerialize() throws Exception {
    	String sql = "xmlserialize(DOCUMENT '<a/>' as clob)"; //$NON-NLS-1$
    	XMLSerialize xs = (XMLSerialize)getExpression(sql);
    	assertEquals(DataTypeManager.DefaultDataClasses.CLOB, xs.getType());
    }

    @Test(expected=QueryResolverException.class) public void testXMLSerialize_1() throws Exception {
    	String sql = "xmlserialize(DOCUMENT 1 as clob)"; //$NON-NLS-1$
    	XMLSerialize xs = (XMLSerialize)getExpression(sql);
    	assertEquals(DataTypeManager.DefaultDataClasses.CLOB, xs.getType());
    }
    
}
