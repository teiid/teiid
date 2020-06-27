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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Clob;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory.DDLHolder;

@SuppressWarnings("nls")
public class TestFunctionResolving {

    @Test public void testResolveBadConvert() throws Exception {
        Function function = new Function("convert", new Expression[] {new Constant(new Character('a')), new Constant(DataTypeManager.DefaultDataTypes.DATE)}); //$NON-NLS-1$

        try {
            ResolverVisitor.resolveLanguageObject(function, RealMetadataFactory.example1Cached());
            fail("excpetion expected"); //$NON-NLS-1$
        } catch (QueryResolverException err) {
            assertEquals("TEIID30071 The conversion from char to date is not allowed.", err.getMessage()); //$NON-NLS-1$
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
            assertEquals("TEIID30069 The function 'LCASE(?)' has more than one possible signature.", err.getMessage()); //$NON-NLS-1$
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
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ResolverVisitor.resolveLanguageObject(func, tm);
        return func;
    }

    /**
     * Helper to verify the result of an expression.
     *
     * @param expr SQL expression.
     * @param result Expected result.
     * @throws Exception
     */
    public static void assertEval(String expr, String result)
            throws Exception {
        Expression ex = TestFunctionResolving.getExpression(expr);
        Object val = Evaluator.evaluate(ex);
        String valStr;
        if (val instanceof Clob) {
            valStr = ClobType.getString((Clob) val);
        } else if (val instanceof XMLType) {
            valStr = ((XMLType) val).getString();
        } else {
            valStr = val.toString();
        }
        assertEquals(result, valStr);
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

    @Test(expected=QueryResolverException.class) public void testStringAggWrongTypes() throws Exception {
        String sql = "string_agg(pm1.g1.e1, pm1.g1.e2)"; //$NON-NLS-1$
        getExpression(sql);
    }

    @Test(expected=QueryResolverException.class) public void testStringAggWrongArgs() throws Exception {
        String sql = "string_agg(pm1.g1.e1)"; //$NON-NLS-1$
        getExpression(sql);
    }

    public static String vararg(Object... vals) {
        return String.valueOf(vals.length);
    }

    @Test public void testVarArgsFunction() throws Exception {
        String ddl = "create foreign function func (VARIADIC z object) returns string options (JAVA_CLASS '"+this.getClass().getName()+"', JAVA_METHOD 'vararg');\n";
        TransformationMetadata tm = RealMetadataFactory.fromDDL(ddl, "x", "y");

        String sql = "func(('a', 'b'))";

        Function func = (Function) QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(func, tm);
        assertEquals(1, func.getArgs().length);

        assertEquals("2", Evaluator.evaluate(func));
    }

    @Test public void testAmbiguousUDF() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x", new DDLHolder("y", "create foreign function f () returns string"),
                new DDLHolder("z", "create foreign function f () returns string"));

        String sql = "f()";
        Function func = (Function) QueryParser.getQueryParser().parseExpression(sql);

        try {
            ResolverVisitor.resolveLanguageObject(func, tm);
            fail();
        } catch(QueryResolverException e) {

        }

        sql = "z.f()";
        func = (Function) QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(func, tm);
    }

    @Test public void testUDFResolveOrder() throws Exception {

        QueryMetadataInterface tm = RealMetadataFactory.fromDDL("create foreign function func(x object) returns object; "
                + " create foreign function func(x string) returns string;"
                + " create foreign function func1(x object) returns double;"
                + " create foreign function func1(x string[]) returns bigdecimal;", "x", "y");

        String sql = "func('a')";

        Function func = (Function) QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(func, tm);
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, func.getArgs()[0].getType());
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, func.getType());

        sql = "func1(('1',))";

        func = (Function) QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(func, tm);
    }

    @Test public void testImportedPushdown() throws Exception {
        RealMetadataFactory.example1Cached();
        QueryMetadataInterface tm = RealMetadataFactory.fromDDL("x", new DDLHolder("y", "create foreign function func(x object) returns object;"), new DDLHolder("z", "create foreign function func(x object) returns object;"));

        String sql = "func('a')";

        Function func = (Function) QueryParser.getQueryParser().parseExpression(sql);
        try {
            ResolverVisitor.resolveLanguageObject(func, tm);
            fail("should be ambiguous");
        } catch (QueryResolverException e) {

        }

        tm = RealMetadataFactory.fromDDL("x", new DDLHolder("y", "create foreign function func(x object) returns object options (\"teiid_rel:system-name\" 'f');"), new DDLHolder("z", "create foreign function func(x object) returns object options (\"teiid_rel:system-name\" 'f');"));

        func = (Function) QueryParser.getQueryParser().parseExpression(sql);
        ResolverVisitor.resolveLanguageObject(func, tm);

        tm = RealMetadataFactory.fromDDL("x", new DDLHolder("y", "create foreign function func() returns object options (\"teiid_rel:system-name\" 'f');"), new DDLHolder("z", "create foreign function func() returns object options (\"teiid_rel:system-name\" 'f');"));

        func = (Function) QueryParser.getQueryParser().parseExpression("func()");
        ResolverVisitor.resolveLanguageObject(func, tm);
    }

    /**
     * e1 is of type string, so 1 should be converted to string
     * @throws Exception
     */
    @Test public void testNumericConversion() throws Exception {
        String sql = "1.0/2"; //$NON-NLS-1$
        Function f = (Function)getExpression(sql);
        assertEquals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, f.getType());
    }

    @Test public void testRankingFunction() throws Exception {
        String sql = "rank() over ()"; //$NON-NLS-1$
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, getExpression(sql).getType());
    }

    @Test public void testAnalytical() throws Exception {
        String sql = "last_value(pm1.g1.e1) over ()"; //$NON-NLS-1$
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, getExpression(sql).getType());
    }

    @Test public void testVirtualFunctionAmbiguity() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("y", new RealMetadataFactory.DDLHolder("v", "CREATE VIRTUAL FUNCTION f1(e1 integer) RETURNS integer as return e1*2;"),
                new RealMetadataFactory.DDLHolder("x", "create foreign function f1(e1 integer)  returns integer; create foreign table t (col integer)"));

        Expression func = QueryParser.getQueryParser().parseExpression("v.f1(1)");
        ResolverVisitor.resolveLanguageObject(func, metadata);
    }

}
