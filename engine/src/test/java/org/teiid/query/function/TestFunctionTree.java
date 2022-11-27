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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.function.source.SystemSource;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestFunctionTree {

    /**
     * Walk through all functions by metadata and verify that we can look
     * each one up by signature
     */
    @Test public void testWalkTree() {
        SystemSource source = new SystemSource();
        FunctionTree ft = new FunctionTree("foo", source);

        Collection<String> categories = ft.getCategories();
        for (String category : categories) {
            assertTrue(ft.getFunctionsInCategory(category).size() > 0);
        }
    }

    public String z() {
        return null;
    }

    protected static String x() {
        return null;
    }

    public static String y() {
        return null;
    }

    public static String toString(byte[] bytes) {
        return new String(bytes);
    }

    @Test public void testLoadErrors() {
        FunctionMethod method = new FunctionMethod(
                "dummy", null, null, PushDown.CAN_PUSHDOWN, null, "noMethod",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                 new ArrayList<FunctionParameter>(0),
                 new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), false, Determinism.DETERMINISTIC); //$NON-NLS-1$

        //allowed, since we're not validating the class
        new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method))));

        //should fail, no class
        try {
            new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
            fail();
        } catch (TeiidRuntimeException e) {
            assertEquals("TEIID31123 Could not load non-FOREIGN UDF \"dummy\", since both invocation class and invocation method are required.", e.getMessage());
        }

        method.setInvocationClass("nonexistantClass");

        //should fail, no class
        try {
            new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
            fail();
        } catch (TeiidRuntimeException e) {

        }

        method.setInvocationClass(TestFunctionTree.class.getName());

        //should fail, no method
        try {
            new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
            fail();
        } catch (TeiidRuntimeException e) {

        }

        method.setInvocationMethod("testLoadErrors");

        //should fail, not void
        try {
            new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
            fail();
        } catch (TeiidRuntimeException e) {

        }

        method.setInvocationMethod("x");

        //should fail, not public
        try {
            new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
            fail();
        } catch (TeiidRuntimeException e) {

        }

        method.setInvocationMethod("z");

        //should fail, not static
        try {
            new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
            fail();
        } catch (TeiidRuntimeException e) {

        }

        method.setInvocationMethod("y");

        //valid!
        new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    }

    @Test public void testNullCategory() {
        FunctionMethod method = new FunctionMethod(
                "dummy", null, null, PushDown.MUST_PUSHDOWN, "nonexistentClass", "noMethod",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new ArrayList<FunctionParameter>(0),
                 new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), //$NON-NLS-1$
                 false, Determinism.DETERMINISTIC);

        Collection<org.teiid.metadata.FunctionMethod> list = Arrays.asList(method);
        FunctionMetadataSource fms = Mockito.mock(FunctionMetadataSource.class);
        Mockito.stub(fms.getFunctionMethods()).toReturn(list);
        FunctionTree ft = new FunctionTree("foo", fms);
        assertEquals(1, ft.getFunctionsInCategory(FunctionCategoryConstants.MISCELLANEOUS).size());
    }

    @Test public void testVarbinary() throws Exception {
        FunctionMethod method = new FunctionMethod(
                "dummy", null, null, PushDown.CANNOT_PUSHDOWN, TestFunctionTree.class.getName(), "toString",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                 Arrays.asList(new FunctionParameter("in", DataTypeManager.DefaultDataTypes.VARBINARY)), //$NON-NLS-1$
                 new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), //$NON-NLS-1$
                 true, Determinism.DETERMINISTIC);
        FunctionTree sys = RealMetadataFactory.SFM.getSystemFunctions();
        FunctionLibrary fl = new FunctionLibrary(sys, new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
        FunctionDescriptor fd = fl.findFunction("dummy", new Class<?>[] {DataTypeManager.DefaultDataClasses.VARBINARY});
        String hello = "hello";
        assertEquals(hello, fd.invokeFunction(new Object[] {new BinaryType(hello.getBytes())}, null, null));
    }

    @Test public void testMultiPartName() throws Exception {
        FunctionMethod method = new FunctionMethod(
                "x.y.dummy", null, null, PushDown.CANNOT_PUSHDOWN, TestFunctionTree.class.getName(), "toString",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                 Arrays.asList(new FunctionParameter("in", DataTypeManager.DefaultDataTypes.VARBINARY)), //$NON-NLS-1$
                 new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), //$NON-NLS-1$
                 true, Determinism.DETERMINISTIC);
        FunctionTree sys = RealMetadataFactory.SFM.getSystemFunctions();
        FunctionLibrary fl = new FunctionLibrary(sys, new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
        assertNotNull(fl.findFunction("dummy", new Class<?>[] {DataTypeManager.DefaultDataClasses.VARBINARY}));
        assertNotNull(fl.findFunction("y.dummy", new Class<?>[] {DataTypeManager.DefaultDataClasses.VARBINARY}));
    }

    @Test public void testMultiPartNameSystemConflict() throws Exception {
        FunctionMethod method = new FunctionMethod(
                "x.concat", null, null, PushDown.MUST_PUSHDOWN, null, null,
                 Arrays.asList(new FunctionParameter("in", DataTypeManager.DefaultDataTypes.STRING), new FunctionParameter("in", DataTypeManager.DefaultDataTypes.STRING)), //$NON-NLS-1$
                 new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), //$NON-NLS-1$
                 true, Determinism.DETERMINISTIC);
        FunctionTree sys = RealMetadataFactory.SFM.getSystemFunctions();
        FunctionLibrary fl = new FunctionLibrary(sys, new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
        fl.determineNecessaryConversions("concat", DataTypeManager.DefaultDataClasses.STRING,
                new Expression[] {new Constant(1),  new Constant(2)}, new Class[] {DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER},false);
    }

/*

//DEBUGGING CODE - this will print out the tree root in readable form
//(This code will either have to be pasted in to FunctionTree class, or
//somehow the Map treeRoot must be gotten from the FunctionTree)

    private static void debugPrintTreeRoot(Map treeRoot){
        System.out.println("<!><!><!><!><!><!><!><!><!><!><!><!>");
        System.out.println("FunctionTree treeRoot");
        StringBuffer s = new StringBuffer();
        debugPrintNode(treeRoot, 0, s);
        System.out.println(s.toString());
        System.out.println("<!><!><!><!><!><!><!><!><!><!><!><!>");
    }

    private static void debugPrintNode(Map node, int depth, StringBuffer s){
        Iterator i = node.entrySet().iterator();
        Map.Entry anEntry = null;
        while(i.hasNext()) {
            anEntry = (Map.Entry)i.next();
            appendLine(s, depth, "Key: " + anEntry.getKey());
            Object value = anEntry.getValue();
            if (value instanceof Map){
                s.append(" Map... ");
                debugPrintNode((Map)value, depth + 1, s);
            } else {
                s.append(" Value: " + value);
            }
        }
    }

    private static void appendLine(StringBuffer s, int depth, String value){
        s.append("\n");
        for (int i = 0; i< depth; i++){
            s.append("  ");
        }
        s.append(value);
    }
*/
}
