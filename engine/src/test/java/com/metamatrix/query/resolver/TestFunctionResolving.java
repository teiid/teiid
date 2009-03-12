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

import static org.junit.Assert.*;
import org.junit.Test;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.util.ResolverVisitorUtil;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestFunctionResolving {

    @Test public void testResolveBadConvert() throws Exception {
        Function function = new Function("convert", new Expression[] {new Constant(new Character('a')), new Constant(DataTypeManager.DefaultDataTypes.DATE)}); //$NON-NLS-1$
        
        try {
            ResolverVisitorUtil.resolveFunction(function, FakeMetadataFactory.example1Cached());
            fail("excpetion expected"); //$NON-NLS-1$
        } catch (QueryResolverException err) {
            assertEquals("The conversion from char to date is not allowed.", err.getMessage()); //$NON-NLS-1$
        } 
    }
    
    @Test public void testResolvesClosestType() throws Exception {
        ElementSymbol e1 = new ElementSymbol("pm1.g1.e1"); //$NON-NLS-1$
        e1.setType(DataTypeManager.DefaultDataClasses.BYTE);
        Function function = new Function("abs", new Expression[] {e1}); //$NON-NLS-1$
        
        ResolverVisitorUtil.resolveFunction(function, FakeMetadataFactory.example1Cached());
        
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, function.getType());
    }
    
    @Test public void testResolveConvertReference() throws Exception {
        Function function = new Function("convert", new Expression[] {new Reference(0), new Constant(DataTypeManager.DefaultDataTypes.BOOLEAN)}); //$NON-NLS-1$
        
        ResolverVisitorUtil.resolveFunction(function, FakeMetadataFactory.example1Cached());
        
        assertEquals(DataTypeManager.DefaultDataClasses.BOOLEAN, function.getType());
        assertEquals(DataTypeManager.DefaultDataClasses.BOOLEAN, function.getArgs()[0].getType());
    }
    
    @Test public void testResolveAmbiguousFunction() throws Exception {
        Function function = new Function("LCASE", new Expression[] {new Reference(0)}); //$NON-NLS-1$
        
        try {
            ResolverVisitorUtil.resolveFunction(function, FakeMetadataFactory.example1Cached());
            fail("excpetion expected"); //$NON-NLS-1$
        } catch (QueryResolverException err) {
            assertEquals("The function 'LCASE(?)' has more than one possible signature.", err.getMessage()); //$NON-NLS-1$
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
			QueryResolverException, MetaMatrixComponentException {
		Function func = (Function)QueryParser.getQueryParser().parseExpression(sql);
    	ResolverVisitorUtil.resolveFunction(func, FakeMetadataFactory.example1Cached());
    	assertEquals(DataTypeManager.DefaultDataClasses.STRING, func.getType());
    	return func;
	}
	
}
