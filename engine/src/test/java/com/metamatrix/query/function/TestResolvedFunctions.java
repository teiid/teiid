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

package com.metamatrix.query.function;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.symbol.Expression;

public class TestResolvedFunctions extends TestCase {

    public void testPowerIntegers() throws Exception {
        
        String sql = "power(10, 10)"; //$NON-NLS-1$
        
        //BigInteger is a closer match here since the second argument matches
        assertEquals(DataTypeManager.DefaultDataClasses.BIG_INTEGER, getFunctionResult(sql).getClass());
    }
    
    public void testPowerDoubles() throws Exception {
        
        String sql = "power(10.01, 10.01)"; //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.DOUBLE, getFunctionResult(sql).getClass());
    }

    public void testPowerFloats() throws Exception {
        
        String sql = "power(convert(10.01, float), convert(10.01, float))"; //$NON-NLS-1$
        
        //since the second argument cannot be converted to an integer, the result is a double
        assertEquals(DataTypeManager.DefaultDataClasses.DOUBLE, getFunctionResult(sql).getClass());
    }
    
    public void testPowerBigInteger() throws Exception {
        
        String sql = "power(convert(10.01, BigInteger), 10)"; //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.BIG_INTEGER, getFunctionResult(sql).getClass());
    }
    
    //there should only be one signature for ceiling. The float argument will be converted to a double
    public void testCeilingFloat() throws Exception {
        
        String sql = "ceiling(convert(10.01, float))"; //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.DOUBLE, getFunctionResult(sql).getClass());
    }
    
    //same as above
    public void testFloorFloat() throws Exception {
        
        String sql = "floor(convert(10.01, float))"; //$NON-NLS-1$
        
        assertEquals(DataTypeManager.DefaultDataClasses.DOUBLE, getFunctionResult(sql).getClass());
    }

    private Object getFunctionResult(String sql) throws QueryParserException,
                                              ExpressionEvaluationException,
                                              BlockedException,
                                              MetaMatrixComponentException, QueryResolverException {
        Expression expr = QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(expr, null);
        return Evaluator.evaluate(expr);
    }
    
}
