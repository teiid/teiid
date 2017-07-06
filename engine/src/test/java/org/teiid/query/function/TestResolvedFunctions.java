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

package org.teiid.query.function;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


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
                                              TeiidComponentException, QueryResolverException {
        Expression expr = QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(expr, RealMetadataFactory.example1Cached());
        return Evaluator.evaluate(expr);
    }
    
}
