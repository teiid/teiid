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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.InvalidFunctionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.types.NullType;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.FunctionLibrary.ConversionResult;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;


@SuppressWarnings("nls")
public class TestFunctionLibrary {

    // These are just used as shorthand convenience to make unit tests more readable below
    private static final Class<String> T_STRING = DataTypeManager.DefaultDataClasses.STRING;
    private static final Class<Integer> T_INTEGER = DataTypeManager.DefaultDataClasses.INTEGER;
    private static final Class<BigInteger> T_BIG_INTEGER = DataTypeManager.DefaultDataClasses.BIG_INTEGER;
    private static final Class<? extends BigDecimal> T_BIG_DECIMAL = DataTypeManager.DefaultDataClasses.BIG_DECIMAL;
    private static final Class<Long> T_LONG = DataTypeManager.DefaultDataClasses.LONG;
    private static final Class<Float> T_FLOAT = DataTypeManager.DefaultDataClasses.FLOAT;
    private static final Class<Double> T_DOUBLE = DataTypeManager.DefaultDataClasses.DOUBLE;
    private static final Class<NullType> T_NULL = DataTypeManager.DefaultDataClasses.NULL;
    private static final Class<Time> T_TIME = DataTypeManager.DefaultDataClasses.TIME;
    private static final Class<Date> T_DATE = DataTypeManager.DefaultDataClasses.DATE;
    private static final Class<Timestamp> T_TIMESTAMP = DataTypeManager.DefaultDataClasses.TIMESTAMP;

    private FunctionLibrary library = new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions());
    private Locale locale;

    @Before public void setUp() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.US);
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
    }

    @After public void tearDown() {
        Locale.setDefault(locale);
        FunctionMethods.dayNames = null;
        FunctionMethods.monthNames = null;
        TimestampWithTimezone.resetCalendar(null);
    }

    // ################################## TEST HELPERS ################################

    @SuppressWarnings("serial")
    private FunctionDescriptor helpCreateDescriptor(String name, Class<?>[] types) {
        final String fname = name;
        final Class<?>[] ftypes = types;
        return new FunctionDescriptor() {
            @Override
            public String getName() {
                return fname;
            }
            @Override
            public PushDown getPushdown() {
                return PushDown.CAN_PUSHDOWN;
            }
            @Override
            public Class<?>[] getTypes() {
                return ftypes;
            }
            @Override
            public Class<?> getReturnType() {
                return null;
            }

            @Override
            public String toString() {
                StringBuffer str = new StringBuffer(fname);
                str.append("("); //$NON-NLS-1$
                for(int i=0; i<ftypes.length; i++) {
                    if(ftypes[i] != null) {
                        str.append(ftypes[i].getName());
                    } else {
                        str.append("null"); //$NON-NLS-1$
                    }
                    if(i<(ftypes.length-1)) {
                        str.append(", "); //$NON-NLS-1$
                    }
                }
                return str.toString();
            }
            @Override
            public boolean requiresContext() {
                return false;
            }
            @Override
            public boolean isNullDependent() {
                return true;
            }

        };
    }

    private void helpFindFunction(String fname, Class<?>[] types, FunctionDescriptor expected) {
        FunctionDescriptor actual =  library.findFunction(fname, types);

        assertEquals("Function names do not match: ", expected.getName().toLowerCase(), actual.getName().toLowerCase());             //$NON-NLS-1$
        assertEquals("Arg lengths do not match: ", expected.getTypes().length, actual.getTypes().length); //$NON-NLS-1$
    }

    private void helpFindFunctionFail(String fname, Class<?>[] types) {
        FunctionDescriptor actual =  library.findFunction(fname, types);
        assertNull("Function was found but should not have been: " + actual, actual); //$NON-NLS-1$
    }

    private void helpFindConversions(String fname, Class<?>[] types, FunctionDescriptor[] expected) {

        FunctionDescriptor[] actual;
        try {
            ConversionResult result = library.determineNecessaryConversions(fname, null, new Expression[types.length], types, false);
            if (result.needsConverion) {
                actual = library.getConverts(result.method, types);
            } else if (result.method != null) {
                actual = new FunctionDescriptor[types.length];
            } else {
                actual = null;
            }
        } catch (InvalidFunctionException e) {
            actual = null;
        }

        if(expected == null) {
            if(actual != null) {
                fail("Expected to find no conversion for " + fname + Arrays.asList(types) + " but found: " + Arrays.asList(actual)); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } else {
            if(actual == null) {
                fail("Expected to find conversion for " + fname + Arrays.asList(types) + " but found none"); //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                // Compare returned descriptors with expected descriptor
                for(int i=0; i<expected.length; i++) {
                    if(expected[i] == null) {
                        if(actual[i] != null) {
                            fail("Expected no conversion at index " + i + ", but found: " + actual[i]); //$NON-NLS-1$ //$NON-NLS-2$
                        }
                    } else {
                        if(actual[i] == null) {
                            fail("Expected conversion at index " + i + ", but found none.");                             //$NON-NLS-1$ //$NON-NLS-2$
                        } else {
                            assertEquals("Expected conversion function names do not match: ", expected[i].getName(), actual[i].getName());             //$NON-NLS-1$
                            assertEquals("Expected conversion arg lengths do not match: ", expected[i].getTypes().length, actual[i].getTypes().length);                         //$NON-NLS-1$
                        }
                    }
                }
            }
        }
    }

    private void helpFindForm(String fname, int numArgs) {
        assertTrue(library.hasFunctionMethod(fname, numArgs));
    }

    void helpInvokeMethod(String fname, Object[] inputs, Object expectedOutput) {
        try {
            helpInvokeMethod(fname, null, inputs, null, expectedOutput);
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    private void helpInvokeMethod(String fname, Class<?>[] types, Object[] inputs, CommandContext context, Object expectedOutput) throws FunctionExecutionException, BlockedException {
        Object actualOutput = helpInvokeMethod(fname, types, inputs, context);
        assertEquals("Actual function output not equal to expected: ", expectedOutput, actualOutput); //$NON-NLS-1$
    }

    Object helpInvokeMethod(String fname, Class<?>[] types,
            Object[] inputs, CommandContext context)
            throws FunctionExecutionException, BlockedException {
        if (types == null) {
            // Build type signature
            types = new Class<?>[inputs.length];
            for(int i=0; i<inputs.length; i++) {
                types[i] = DataTypeManager.determineDataTypeClass(inputs[i]);
            }
        }
        if (context == null) {
            context = new CommandContext();
        }
        Object actualOutput = null;
        // Find function descriptor
        FunctionDescriptor descriptor = library.findFunction(fname, types);
        if (descriptor.requiresContext()) {
            // Invoke function with inputs
            Object[] in = new Object[inputs.length+1];
            in[0] = context;
            for (int i = 0; i < inputs.length; i++) {
                in[i+1] = inputs[i];
            }
            actualOutput = descriptor.invokeFunction(in, null, null);
        }
        else {
            // Invoke function with inputs
            actualOutput = descriptor.invokeFunction(inputs, null, null);
        }
        return actualOutput;
    }

    private void helpInvokeMethodFail(String fname, Object[] inputs) {
           helpInvokeMethodFail(fname, null, inputs);
    }

    private void helpInvokeMethodFail(String fname, Class<?> types[], Object[] inputs) {
        try {
            helpInvokeMethod(fname, types, inputs, null);
            fail("expected exception"); //$NON-NLS-1$
        } catch (FunctionExecutionException err) {
        } catch (BlockedException e) {
        }
    }
    // ################################## ACTUAL TESTS ################################

    @Test public void testFindFunction1() {
        helpFindFunction("convert", new Class<?>[] { T_INTEGER, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_INTEGER, T_STRING }) );
    }

    @Test public void testFindFunction2() {
        helpFindFunction("cast", new Class<?>[] { T_INTEGER, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor(FunctionLibrary.CAST, new Class<?>[] { T_INTEGER, T_STRING }) );
    }

    @Test public void testFindFunction3() {
        helpFindFunction("curdate", new Class<?>[0], //$NON-NLS-1$
            helpCreateDescriptor("curdate", new Class<?>[0])); //$NON-NLS-1$
    }

    @Test public void testFindFunction4() {
        helpFindFunctionFail("curdate", new Class<?>[] { T_INTEGER }); //$NON-NLS-1$
    }

    @Test public void testFindFunction5() {
        helpFindFunction("+", new Class<?>[] { T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            helpCreateDescriptor("+", new Class<?>[] { T_INTEGER, T_INTEGER }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction6() {
        helpFindFunctionFail("+", new Class<?>[] {T_INTEGER, T_FLOAT}); //$NON-NLS-1$
    }

    @Test public void testFindFunction7() {
        helpFindFunctionFail("+", new Class<?>[] {T_INTEGER, T_FLOAT, T_INTEGER}); //$NON-NLS-1$
    }

    @Test public void testFindFunction8() {
        helpFindFunctionFail("+", new Class<?>[] {T_INTEGER}); //$NON-NLS-1$
    }

    @Test public void testFindFunction9() {
        helpFindFunctionFail("+", new Class<?>[] {T_INTEGER, T_NULL });     //$NON-NLS-1$
    }

    @Test public void testFindFunction10() {
        helpFindFunction("substring", new Class<?>[] { T_STRING, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            helpCreateDescriptor("substring", new Class<?>[] { T_STRING, T_INTEGER, T_INTEGER }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction11() {
        helpFindFunction("substring", new Class<?>[] { T_STRING, T_INTEGER }, //$NON-NLS-1$
            helpCreateDescriptor("substring", new Class<?>[] { T_STRING, T_INTEGER }) ); //$NON-NLS-1$
    }

    @Test public void testFind0ArgConversion1() {
        helpFindConversions(
            "curdate", new Class<?>[] {}, //$NON-NLS-1$
            new FunctionDescriptor[0] );
    }

    @Test public void testFind0ArgConversion2() {
        helpFindConversions(
            "curdate", new Class<?>[] { T_INTEGER }, //$NON-NLS-1$
            null );
    }

    @Test public void testFind1ArgConversion1() {
        helpFindConversions(
            "length", new Class<?>[] { T_STRING }, //$NON-NLS-1$
            new FunctionDescriptor[1] );
    }

    @Test public void testFind1ArgConversion2() {
        helpFindConversions(
            "length", new Class<?>[] { T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_INTEGER, T_STRING })
            } );
    }

    @Test public void testFind1ArgConversion3() {
        helpFindConversions(
            "length", new Class<?>[] { DataTypeManager.DefaultDataClasses.TIMESTAMP }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.TIMESTAMP, T_STRING })
            } );
    }

    @Test public void testFind2ArgConversion1() {
        helpFindConversions(
            "+", new Class<?>[] { T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[2] );
    }

    @Test public void testFind2ArgConversion2() {
        helpFindConversions(
            "+", new Class<?>[] { T_INTEGER, T_FLOAT }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_INTEGER, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_FLOAT, T_STRING }) } );
    }

    @Test public void testFind2ArgConversion3() {
        helpFindConversions(
            "+", new Class<?>[] { T_FLOAT, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_FLOAT, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_INTEGER, T_STRING }) } );
    }

    @Test public void testFind2ArgConversion4() {
        helpFindConversions(
            "+", new Class<?>[] { T_STRING, T_FLOAT }, //$NON-NLS-1$
            null );
    }

    @Test public void testFind2ArgConversion5() {
        helpFindConversions(
            "+", new Class<?>[] { T_NULL, T_NULL }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }) } );
    }

    @Test public void testFind2ArgConversion6() {
        helpFindConversions(
            "+", new Class<?>[] { T_NULL, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                null } );
    }

    @Test public void testFind2ArgConversion7() {
        helpFindConversions(
            "+", new Class<?>[] { T_INTEGER, T_NULL }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                null,
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }) } );
    }

    @Test public void testFind3ArgConversion1() {
        helpFindConversions(
            "substring", new Class<?>[] { T_STRING, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[3] );
    }

    @Test public void testFind3ArgConversion2() {
        helpFindConversions(
            "substring", new Class<?>[] { T_INTEGER, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_INTEGER, T_STRING }),
                null,
                null
            } );
    }

    @Test public void testFind3ArgConversion3() {
        helpFindConversions(
            "substring", new Class<?>[] { T_INTEGER, T_INTEGER, DataTypeManager.DefaultDataClasses.SHORT }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_INTEGER, T_STRING }),
                null,
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING })
            } );
    }

    @Test public void testFind3ArgConversion4() {
        helpFindConversions(
            "substring", new Class<?>[] { T_STRING, T_INTEGER, DataTypeManager.DefaultDataClasses.TIMESTAMP }, //$NON-NLS-1$
            null );
    }

    @Test public void testFind3ArgConversion5() {
        helpFindConversions(
            "substring", new Class<?>[] { T_STRING, DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.SHORT }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                null,
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING })
            } );
    }

    @Test public void testFind3ArgConversion6() {
        helpFindConversions(
            "substring", new Class<?>[] { T_INTEGER, DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.SHORT }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.INTEGER, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING })
            } );
    }

    @Test public void testFind3ArgConversion7() {
        helpFindConversions(
            "substring", new Class<?>[] { T_NULL, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                null,
                null }
            );
    }

    @Test public void testFind3ArgConversion8() {
        helpFindConversions(
            "substring", new Class<?>[] { T_NULL, T_NULL, T_INTEGER }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                null }
            );
    }

    @Test public void testFind3ArgConversion9() {
        helpFindConversions(
            "substring", new Class<?>[] { T_NULL, T_NULL, T_NULL }, //$NON-NLS-1$
            new FunctionDescriptor[] {
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }),
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class<?>[] { T_NULL, T_STRING }) }
            );
    }

    @Test public void testFindForm1() {
        helpFindForm("convert", 2); //$NON-NLS-1$
    }

    @Test public void testFindForm2() {
        helpFindForm("locate", 2); //$NON-NLS-1$
    }

    @Test public void testFindForm3() {
        helpFindForm("locate", 3); //$NON-NLS-1$
    }

    @Test public void testFindForm4() {
        helpFindForm("substring", 2); //$NON-NLS-1$
    }

    @Test public void testFindForm5() {
        helpFindForm("substring", 3); //$NON-NLS-1$
    }

    @Test public void testFindForm6() {
        helpFindForm("now", 0); //$NON-NLS-1$
    }

    @Test public void testInvokePlus1() {
        helpInvokeMethod("+", new Object[] { new Integer(3), new Integer(2) }, new Integer(5)); //$NON-NLS-1$
    }

    @Test public void testInvokePlus2() {
        helpInvokeMethod("+", new Object[] { new Long(3), new Long(2) }, new Long(5)); //$NON-NLS-1$
    }

    @Test public void testInvokePlus3() {
        helpInvokeMethod("+", new Object[] { new Float(3), new Float(2) }, new Float(5)); //$NON-NLS-1$
    }

    @Test public void testInvokePlus4() {
        helpInvokeMethod("+", new Object[] { new Double(3), new Double(2) }, new Double(5)); //$NON-NLS-1$
    }

    @Test public void testInvokePlus5() {
        helpInvokeMethod("+", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("5")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokePlus6() {
        helpInvokeMethod("+", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("5")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeMinus1() {
        helpInvokeMethod("-", new Object[] { new Integer(3), new Integer(2) }, new Integer(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeMinus2() {
        helpInvokeMethod("-", new Object[] { new Long(3), new Long(2) }, new Long(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeMinus3() {
        helpInvokeMethod("-", new Object[] { new Float(3), new Float(2) }, new Float(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeMinus4() {
        helpInvokeMethod("-", new Object[] { new Double(3), new Double(2) }, new Double(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeMinus5() {
        helpInvokeMethod("-", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeMinus6() {
        helpInvokeMethod("-", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeMultiply1() {
        helpInvokeMethod("*", new Object[] { new Integer(3), new Integer(2) }, new Integer(6)); //$NON-NLS-1$
    }

    @Test public void testInvokeMultiply2() {
        helpInvokeMethod("*", new Object[] { new Long(3), new Long(2) }, new Long(6)); //$NON-NLS-1$
    }

    @Test public void testInvokeMultiply3() {
        helpInvokeMethod("*", new Object[] { new Float(3), new Float(2) }, new Float(6)); //$NON-NLS-1$
    }

    @Test public void testInvokeMultiply4() {
        helpInvokeMethod("*", new Object[] { new Double(3), new Double(2) }, new Double(6)); //$NON-NLS-1$
    }

    @Test public void testInvokeMultiply5() {
        helpInvokeMethod("*", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("6")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeMultiply6() {
        helpInvokeMethod("*", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("6")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeDivide1() {
        helpInvokeMethod("/", new Object[] { new Integer(3), new Integer(2) }, new Integer(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeDivide2() {
        helpInvokeMethod("/", new Object[] { new Long(3), new Long(2) }, new Long(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeDivide3() {
        helpInvokeMethod("/", new Object[] { new Float(3), new Float(2) }, new Float(1.5)); //$NON-NLS-1$
    }

    @Test public void testInvokeDivide4() {
        helpInvokeMethod("/", new Object[] { new Double(3), new Double(2) }, new Double(1.5)); //$NON-NLS-1$
    }

    @Test public void testInvokeDivide5() {
        helpInvokeMethod("/", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    // one digit precision
    @Test public void testInvokeDivide6() {
        helpInvokeMethod("/", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("1.5"));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeDivide7() throws Exception {
        helpInvokeMethodFail("/", new Object[] { new Float("3"), new Float("0") });   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeDivideMod() {
        helpInvokeMethod("mod", new Object[] { new BigDecimal("3.1"), new BigDecimal("2") }, new BigDecimal("1.1"));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeAbs1() {
        helpInvokeMethod("abs", new Object[] { new Integer(-3) }, new Integer(3)); //$NON-NLS-1$
    }

    @Test public void testInvokeAbs2() {
        helpInvokeMethod("abs", new Object[] { new Long(-3) }, new Long(3)); //$NON-NLS-1$
    }

    @Test public void testInvokeAbs3() {
        helpInvokeMethod("abs", new Object[] { new Float(-3) }, new Float(3)); //$NON-NLS-1$
    }

    @Test public void testInvokeAbs4() {
        helpInvokeMethod("abs", new Object[] { new Double(-3) }, new Double(3)); //$NON-NLS-1$
    }

    @Test public void testInvokeAbs5() {
        helpInvokeMethod("abs", new Object[] { new BigInteger("-3") }, new BigInteger("3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeAbs6() {
        helpInvokeMethod("abs", new Object[] { new BigDecimal("-3") }, new BigDecimal("3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeAcos() {
        helpInvokeMethod("acos", new Object[] { new Double(0.05) }, new Double(1.5207754699891267)); //$NON-NLS-1$
    }

    @Test public void testInvokeAsin() {
        helpInvokeMethod("asin", new Object[] { new Double(0.05) }, new Double(0.050020856805770016)); //$NON-NLS-1$
    }

    @Test public void testInvokeAtan() {
        helpInvokeMethod("atan", new Object[] { new Double(0.05) }, new Double(0.049958395721942765)); //$NON-NLS-1$
    }

    @Test public void testInvokeAtan2() {
        helpInvokeMethod("atan2", new Object[] { new Double(0.05), new Double(0.07) }, new Double(0.6202494859828215)); //$NON-NLS-1$
    }

    @Test public void testInvokeAtanBigDecimal() {
        helpInvokeMethod("atan", new Object[] { new BigDecimal(0.05) }, new Double(0.049958395721942765)); //$NON-NLS-1$
    }

    @Test public void testInvokeAtan2BigDecimal() {
        helpInvokeMethod("atan2", new Object[] { new BigDecimal(0.05), new BigDecimal(0.07) }, new Double(0.6202494859828215)); //$NON-NLS-1$
    }

    @Test public void testInvokeCos() {
        helpInvokeMethod("cos", new Object[] { new Double(1.57) }, new Double(7.963267107332633E-4)); //$NON-NLS-1$
    }

    @Test public void testInvokeCot() {
        helpInvokeMethod("cot", new Object[] { new Double(1.57) }, new Double(7.963269632231926E-4)); //$NON-NLS-1$
    }

    @Test public void testInvokeDegrees() throws Exception {
        Double result = (Double) helpInvokeMethod("degrees", null, new Object[] { new Double(1.57) }, null); //$NON-NLS-1$
        assertEquals(result, 89.95437383553926, .000000001);
    }

    @Test public void testInvokePI() {
        helpInvokeMethod("pi", new Object[] { }, new Double(3.141592653589793)); //$NON-NLS-1$
    }

    @Test public void testInvokeRadians() throws Exception {
        Double result = (Double) helpInvokeMethod("radians", null, new Object[] { new Double(89.95437383553926) }, null); //$NON-NLS-1$
        assertEquals(result, 1.57, .000000001);
    }

    @Test public void testInvokeSin() {
        helpInvokeMethod("sin", new Object[] { new Double(1.57) }, new Double(0.9999996829318346)); //$NON-NLS-1$
    }

    @Test public void testInvokeTan() {
        helpInvokeMethod("tan", new Object[] { new Double(0.785) }, new Double(0.9992039901050427)); //$NON-NLS-1$
    }

    @Test public void testInvokeAscii() {
        helpInvokeMethod("ascii", new Object[] { " " }, new Integer(32)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvokeChr() {
        helpInvokeMethod("chr", new Object[] { new Integer(32) }, new Character(' ')); //$NON-NLS-1$
    }

    @Test public void testInvokeNvl() {
        helpInvokeMethod("nvl", new Object[] { new Integer(5), new Integer(10) }, new Integer(5) ); //$NON-NLS-1$
    }

    @Test public void testInvokeConcatOperator() {
        helpInvokeMethod("||", new Object[] { "a", "b" }, "ab" );         //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeInitcap() {
        helpInvokeMethod("initcap", new Object[] { "my test\ndata" }, "My Test\nData" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeLpad1() {
        helpInvokeMethod("lpad", new Object[] { "x", new Integer(3) }, "  x" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeLpad2() {
        helpInvokeMethod("lpad", new Object[] { "x", new Integer(3), "y" }, "yyx" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeRpad1() {
        helpInvokeMethod("rpad", new Object[] { "x", new Integer(3) }, "x  " ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeRpad2() {
        helpInvokeMethod("rpad", new Object[] { "x", new Integer(3), "y" }, "xyy" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeTranslate() {
        helpInvokeMethod("translate", new Object[] { "ababcd", "ad", "da" }, "dbdbca" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }

    @Test public void testFindFunction13() {
        helpFindFunction("formatTime", new Class<?>[] { T_TIME, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatTime", new Class<?>[] { T_TIME, T_STRING }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction14() {
        helpFindFunction("formatDate", new Class<?>[] { T_DATE, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatDate", new Class<?>[] { T_DATE, T_STRING }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction15() {
        helpFindFunction("formatTimestamp", new Class<?>[] { T_TIMESTAMP, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatTimestamp", new Class<?>[] { T_TIMESTAMP, T_STRING }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction16() {
        helpFindFunction("parseTime", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseTime", new Class<?>[] { T_STRING, T_STRING }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction17() {
        helpFindFunction("parseDate", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseDate", new Class<?>[] { T_STRING, T_STRING }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction18() {
        helpFindFunction("parseTimestamp", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
                helpCreateDescriptor("parseTimestamp", new Class<?>[] { T_STRING, T_STRING }) ); //$NON-NLS-1$
    }

    @Test public void testFindFunction19() {
        helpFindFunction("env", new Class<?>[] {T_STRING}, //$NON-NLS-1$
                         helpCreateDescriptor("env", new Class<?>[] {T_STRING})); //$NON-NLS-1$
    }

    @Test public void testInvokeFormatTimestamp1() {
        helpInvokeMethod("formatTimestamp", new Object[] {TimestampUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("mm/dd/yy h:mm a") }, "04/05/03 3:04 AM");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatTimestamp2() {
        helpInvokeMethod("formatTimestamp", new Object[] {TimestampUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("yyyy-mm-dd k:mm a z") }, "2003-04-05 3:04 AM GMT-06:00");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatTimestamp3() {
            helpInvokeMethod("formatTimestamp", new Object[] {TimestampUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("yyyy-mm-dd hh:mm:ss.SSSS") }, "2003-04-05 03:04:12.0000");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatTimestampFail() throws Exception {
        helpInvokeMethodFail("formatTimestamp", new Object[] {TimestampUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("mm/dd/nn h:mm a") }); //$NON-NLS-1$
    }

    @Test public void testInvokeParseTimestamp1() {
        helpInvokeMethod("parseTimestamp", new Object[] {new String("05 Mar 2003 03:12:23 CST"), new String("dd MMM yyyy HH:mm:ss z") }, TimestampUtil.createTimestamp(103, 2, 5, 3, 12, 23, 0));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeParseTimestamp2() {
        helpInvokeMethod("parseTimestamp", new Object[] {new String("05 Mar 2003 03:12:23.333"), new String("dd MMM yyyy HH:mm:ss.SSS") }, TimestampUtil.createTimestamp(103, 2, 5, 3, 12, 23, 333*1000000));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testFindFormatInteger() {
        helpFindFunction("formatInteger", new Class<?>[] { T_INTEGER, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatInteger", new Class<?>[] { T_INTEGER, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindFormatFloat() {
        helpFindFunction("formatFloat", new Class<?>[] { T_FLOAT, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatFloat", new Class<?>[] { T_FLOAT, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindFormatDouble() {
        helpFindFunction("formatDouble", new Class<?>[] { T_DOUBLE, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatDouble", new Class<?>[] { T_DOUBLE, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindFormatLong() {
        helpFindFunction("formatLong", new Class<?>[] { T_LONG, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatLong", new Class<?>[] { T_LONG, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindFormatBigInteger() {
        helpFindFunction("formatBigInteger", new Class<?>[] { T_BIG_INTEGER, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatBigInteger", new Class<?>[] { T_BIG_INTEGER, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindFormatBigDecimal() {
        helpFindFunction("formatBigDecimal", new Class<?>[] { T_BIG_DECIMAL, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("formatBigDecimal", new Class<?>[] { T_BIG_DECIMAL, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindParseInteger() {
        helpFindFunction("parseInteger", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseInteger", new Class<?>[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindParseLong() {
        helpFindFunction("parseLong", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseLong", new Class<?>[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindParseDouble() {
        helpFindFunction("parseDouble", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseDouble", new Class<?>[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
    }
    @Test public void testFindParseFloat() {
        helpFindFunction("parseFloat", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseFloat", new Class<?>[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindParseBigInteger() {
        helpFindFunction("parseBigInteger", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseBigInteger", new Class<?>[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testFindParseBigDecimal() {
        helpFindFunction("parseBigDecimal", new Class<?>[] { T_STRING, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("parseBigDecimal", new Class<?>[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
    }

    @Test public void testInvokeParseInteger() {
        helpInvokeMethod("parseInteger", new Object[] {new String("-1234"), new String("######")}, new Integer(-1234));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeParseLong() {
        helpInvokeMethod("parseLong", new Object[] {new String("123456"), new String("######.##")}, new Long(123456));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeParseDouble() {
        helpInvokeMethod("parseDouble", new Object[] {new String("123456.78"), new String("#####.#")}, new Double(123456.78));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeParseFloat() {
        helpInvokeMethod("parseFloat", new Object[] {new String("1234.56"), new String("####.###")}, new Float(1234.56));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeParseBigInteger() {
        helpInvokeMethod("parseBigInteger", new Object[] {new String("12345678"), new String("###,###")}, new BigInteger("12345678"));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeParseBigDecimal() {
        helpInvokeMethod("parseBigDecimal", new Object[] {new String("1234.56"), new String("#####")}, new BigDecimal("1234.56"));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeFormatInteger() {
        helpInvokeMethod("formatInteger", new Object[] {new Integer(-1234), new String("######")}, "-1234");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatLong() {
        helpInvokeMethod("formatLong", new Object[] {new Long(123456788), new String("##,###,#")}, "1,2,3,4,5,6,7,8,8");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatDouble() {
        helpInvokeMethod("formatDouble", new Object[] {new Double(1234.67), new String("####.##")}, "1234.67");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatFloat() {
        helpInvokeMethod("formatFloat", new Object[] {new Float(1234.67), new String("###.###")}, "1234.67");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFormatBigInteger() {
        helpInvokeMethod("formatBigInteger", new Object[] {new BigInteger("45"), new String("###.###")}, "45");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeFormatBigDecimal() {
        helpInvokeMethod("formatBigDecimal", new Object[] {new BigDecimal("1234.56"), new String("###.###")}, "1234.56");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokeQuarter1() {
        //        2003-5-15
        helpInvokeMethod("quarter", new Object[] {TimestampUtil.createDate(103, 4, 15)}, new Integer(2));     //$NON-NLS-1$
    }

    @Test public void testInvokeQuarter2() {
        //        2003-5-1
        helpInvokeMethod("quarter", new Object[] {TimestampUtil.createDate(103, 3, 31)}, new Integer(2));     //$NON-NLS-1$
    }

    @Test public void testInvokeQuarter3() {
        //        2003-1-31
        helpInvokeMethod("quarter", new Object[] {TimestampUtil.createDate(103, 0, 31)}, new Integer(1));     //$NON-NLS-1$
    }

    @Test public void testInvokeQuarter4() {
    //        2003-9-30
        helpInvokeMethod("quarter", new Object[] {TimestampUtil.createDate(103, 8, 30)}, new Integer(3));     //$NON-NLS-1$
    }

    @Test public void testInvokeQuarter5() {
    //        2003-12-31
        helpInvokeMethod("quarter", new Object[] {TimestampUtil.createDate(103, 11, 31)}, new Integer(4));     //$NON-NLS-1$
    }

    @Test public void testInvokeQuarter6() {
        //bad date such as 2003-13-45
        helpInvokeMethod("quarter", new Object[] {TimestampUtil.createDate(103, 12, 45)}, new Integer(1));     //$NON-NLS-1$
    }

    @Test public void testInvokeIfNull() {
        helpInvokeMethod("ifnull", new Object[] {new Integer(5), new Integer(10)}, new Integer(5));     //$NON-NLS-1$
    }

    @Test public void testInvokeLower() {
        helpInvokeMethod("lower", new Object[] {new String("LOWER")}, new String("lower"));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeUpper() {
        helpInvokeMethod("upper", new Object[] {new String("upper")}, new String("UPPER"));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeUpperClob() {
        helpInvokeMethod("upper", new Object[] {new ClobType(new ClobImpl("upper"))}, new ClobType(new ClobImpl("UPPER")));  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeRepeat() {
        helpInvokeMethod("repeat", new Object[] {new String("cat"), new Integer(3)}, new String("catcatcat"));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeChar() {
        helpInvokeMethod("char", new Object[] {new Integer(32) }, new Character(' ')); //$NON-NLS-1$
    }

    /** normal input */
    @Test public void testInvokeInsert1() {
        helpInvokeMethod("insert", new Object[] {new String("Downtown"), new Integer(4),  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(2), new String("cat")}, new String("Dowcatown"));     //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** empty string2 */
    @Test public void testInvokeInsert2() {
        helpInvokeMethod("insert", new Object[] {new String("Downtown"), new Integer(4),  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(2), new String("")}, new String("Dowown"));     //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** empty string1 with start = 1 and length = 0, so result is just string2 */
    @Test public void testInvokeInsert3() {
        helpInvokeMethod("insert", new Object[] {new String(""), new Integer(1),  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(0), new String("cat")}, new String("cat"));     //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** should fail, with start > string1.length() */
    @Test public void testInvokeInsert4() throws Exception {
        helpInvokeMethodFail("insert", new Object[] {new String(""), new Integer(2),  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(0), new String("cat")}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** should fail, with length > 0 and input string1.length() = 0 */
    @Test public void testInvokeInsert5() throws Exception {
        helpInvokeMethodFail("insert", new Object[] {new String(""), new Integer(1),  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(1), new String("cat")});     //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**  (length + start) > string1.length(), then just append str2 starting at start position */
    @Test public void testInvokeInsert6() {
        helpInvokeMethod("insert", new Object[] {new String("Downtown"), new Integer(7),  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(5), new String("cat")}, new String("Downtocat"));     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvokeTimestampAddDate_ignore_case() {
        helpInvokeMethod("timestampAdd", new Object[] {"sql_TSI_day",  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(28), new Timestamp(TimestampUtil.createDate(103, 4, 15).getTime())}, new Timestamp(TimestampUtil.createDate(103, 5, 12).getTime()));
    }

    /** date + month --> count=18, inteval=month, result should be 2004-11-15 */
    @Test public void testInvokeTimestampAddDate2() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_MONTH,  //$NON-NLS-1$
            new Integer(18), new Timestamp(TimestampUtil.createDate(103, 4, 15).getTime())}, new Timestamp(TimestampUtil.createDate(104, 10, 15).getTime()));
    }

    /** date + month --> count=-18, inteval=month, result should be 2001-11-15 */
    @Test public void testInvokeTimestampAddDate2a() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_MONTH,  //$NON-NLS-1$
            new Integer(-18), new Timestamp(TimestampUtil.createDate(103, 4, 15).getTime())}, new Timestamp(TimestampUtil.createDate(101, 10, 15).getTime()));
    }

    /** date + week --> count=6, inteval=week, result should be 2003-04-03 */
    @Test public void testInvokeTimestampAddDate3() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_WEEK,  //$NON-NLS-1$
            new Integer(-6), new Timestamp(TimestampUtil.createDate(103, 4, 15).getTime())}, new Timestamp(TimestampUtil.createDate(103, 3, 3).getTime()));
    }

    /** date + quarter --> count=3, inteval=quarter, result should be 2004-2-15 */
    @Test public void testInvokeTimestampAddDate4() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_QUARTER,  //$NON-NLS-1$
            new Integer(3), new Timestamp(TimestampUtil.createDate(103, 4, 15).getTime())}, new Timestamp(TimestampUtil.createDate(104, 1, 15).getTime()));
    }

    /** date + year --> count=-1, inteval=year, result should be 2002-5-15 */
    @Test public void testInvokeTimestampAddDate5() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_YEAR,  //$NON-NLS-1$
            new Integer(-1), new Timestamp(TimestampUtil.createDate(103, 4, 15).getTime())}, new Timestamp(TimestampUtil.createDate(102, 4, 15).getTime()));
    }

    /** time + minute --> count=23, inteval=3, result should be 03:32:12 */
    @Test public void testInvokeTimestampAddTime1() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_MINUTE,  //$NON-NLS-1$
            new Integer(23), new Timestamp(TimestampUtil.createTime(3, 9, 12).getTime())}, new Timestamp(TimestampUtil.createTime(3, 32, 12).getTime()));
    }

    /** time + hour --> count=21, inteval=4, result should be 00:09:12 and overflow */
    @Test public void testInvokeTimestampAddTime2() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_HOUR,  //$NON-NLS-1$
            new Integer(21), new Timestamp(TimestampUtil.createTime(3, 9, 12).getTime())}, TimestampUtil.createTimestamp(70, 0, 2, 0, 9, 12, 0));
    }

    /** time + hour --> count=2, inteval=4, result should be 01:12:12*/
    @Test public void testInvokeTimestampAddTime3() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_HOUR,  //$NON-NLS-1$
            new Integer(2), new Timestamp(TimestampUtil.createTime(23, 12, 12).getTime())}, TimestampUtil.createTimestamp(70, 0, 2, 1, 12, 12, 0));
    }

    /** time + second --> count=23, inteval=2, result should be 03:10:01 */
    @Test public void testInvokeTimestampAddTime4() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_SECOND,  //$NON-NLS-1$
            new Integer(49), new Timestamp(TimestampUtil.createTime(3, 9, 12).getTime())}, new Timestamp(TimestampUtil.createTime(3, 10, 1).getTime()));
    }

    /** timestamp + second --> count=23, inteval=2, result should be 2003-05-15 03:09:35.100  */
    @Test public void testInvokeTimestampAddTimestamp1() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_SECOND,  //$NON-NLS-1$
            new Integer(23), TimestampUtil.createTimestamp(103, 4, 15, 3, 9, 12, 100)},
            TimestampUtil.createTimestamp(103, 4, 15, 3, 9, 35, 100));
    }

    /** timestamp + nanos --> count=1, inteval=1, result should be 2003-05-15 03:09:12.000000101  */
    @Test public void testInvokeTimestampAddTimestamp2() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
            new Integer(1), TimestampUtil.createTimestamp(103, 4, 15, 3, 9, 12, 100)},
            TimestampUtil.createTimestamp(103, 4, 15, 3, 9, 12, 101));
    }

    /** timestamp + nanos --> count=2100000000, inteval=1, result should be 2003-05-15 03:10:01.100000001
     *  with increase in second and minutes, because second already at 59 sec originally
     */
    @Test public void testInvokeTimestampAddTimestamp3() {
        helpInvokeMethod("timestampAdd", new Object[] {NonReserved.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
            new Integer(2100000000), TimestampUtil.createTimestamp(103, 4, 15, 3, 9, 59, 1)},
            TimestampUtil.createTimestamp(103, 4, 15, 3, 10, 1, 100000001));
    }

    /** time --> interval=hour, time1 = 03:04:45, time2= 05:05:36 return = 2  */
    @Test public void testInvokeTimestampDiffTime1() {
        helpInvokeMethod("timestampDiff", new Object[] {NonReserved.SQL_TSI_HOUR,  //$NON-NLS-1$
                new Timestamp(TimestampUtil.createTime(3, 4, 45).getTime()), new Timestamp(TimestampUtil.createTime(5, 5, 36).getTime()) },
            new Long(2));
    }

    @Test public void testInvokeTimestampDiffTime1_ignorecase() {
        helpInvokeMethod("timestampDiff", new Object[] {"SQL_tsi_HOUR",  //$NON-NLS-1$ //$NON-NLS-2$
                new Timestamp(TimestampUtil.createTime(3, 4, 45).getTime()), new Timestamp(TimestampUtil.createTime(5, 5, 36).getTime()) },
            new Long(2));
    }

    /**
     * timestamp --> interval=week, time1 = 2002-06-21 03:09:35.100,
     * time2= 2003-05-02 05:19:35.500 return = 45
     */
    @Test public void testInvokeTimestampDiffTimestamp1() {
        helpInvokeMethod("timestampDiff", new Object[] {NonReserved.SQL_TSI_WEEK,  //$NON-NLS-1$
            TimestampUtil.createTimestamp(102, 5, 21, 3, 9, 35, 100), TimestampUtil.createTimestamp(103, 4, 2, 5, 19, 35, 500) },
            new Long(45));
    }

    /**
     * timestamp --> interval=frac_second, time1 = 2002-06-21 03:09:35.000000001,
     * time2= 2002-06-21 03:09:35.100000000 return = 999999999
     */
    @Test public void testInvokeTimestampDiffTimestamp2() {
        helpInvokeMethod("timestampDiff", new Object[] {NonReserved.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
            TimestampUtil.createTimestamp(102, 5, 21, 3, 9, 35, 1), TimestampUtil.createTimestamp(102, 5, 21, 3, 9, 35, 100000000) },
            new Long(99999999));
    }

    /**
     * timestamp --> interval=frac_second, time1 = 2002-06-21 03:09:35.000000002,
     * time2= 2002-06-22 03:09:35.000000001 return =
     */
    @Test public void testInvokeTimestampDiffTimestamp3() {
        helpInvokeMethod("timestampDiff", new Object[] {NonReserved.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
            TimestampUtil.createTimestamp(102, 5, 21, 3, 9, 35, 2), TimestampUtil.createTimestamp(102, 5, 22, 3, 9, 35, 1) },
            new Long(86399999999999L));
    }

    @Test public void testInvokeTimestampCreate1() {
        helpInvokeMethod("timestampCreate", new Object[] {TimestampUtil.createDate(103, 4, 15), //$NON-NLS-1$
                                                          TimestampUtil.createTime(23, 59, 59)},
                                                          TimestampUtil.createTimestamp(103, 4, 15, 23, 59, 59, 0));
    }

    @Test public void testInvokeBitand() {
        helpInvokeMethod("bitand", new Object[] {new Integer(0xFFF), new Integer(0x0F0)}, new Integer(0x0F0)); //$NON-NLS-1$
    }
    @Test public void testInvokeBitor() {
        helpInvokeMethod("bitor", new Object[] {new Integer(0xFFF), new Integer(0x0F0)}, new Integer(0xFFF)); //$NON-NLS-1$
    }
    @Test public void testInvokeBitxor() {
        helpInvokeMethod("bitxor", new Object[] {new Integer(0xFFF), new Integer(0x0F0)}, new Integer(0xF0F)); //$NON-NLS-1$
    }
    @Test public void testInvokeBitnot() {
        helpInvokeMethod("bitnot", new Object[] {new Integer(0xF0F)}, new Integer(0xFFFFF0F0)); //$NON-NLS-1$
    }

    @Test public void testInvokeRound1() {
        helpInvokeMethod("round", new Object[] {new Integer(123), new Integer(-1)}, new Integer(120)); //$NON-NLS-1$
    }

    @Test public void testInvokeRound2() {
        helpInvokeMethod("round", new Object[] {new Float(123.456), new Integer(2)}, new Float(123.46)); //$NON-NLS-1$
    }

    @Test public void testInvokeRound3() {
        helpInvokeMethod("round", new Object[] {new Double(123.456), new Integer(2)}, new Double(123.46)); //$NON-NLS-1$
    }

    @Test public void testInvokeRound4() {
        helpInvokeMethod("round", new Object[] {new BigDecimal("123.456"), new Integer(2)}, new BigDecimal("123.460")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /** defect 10941 */
    @Test public void testInvokeConvertTime() {
        helpInvokeMethod("convert", new Object[] {"05:00:00", "time"}, TimestampUtil.createTime(5, 0, 0)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeModifyTimeZone() {
        Timestamp ts = Timestamp.valueOf("2004-10-03 23:59:59.123"); //$NON-NLS-1$
        Timestamp out = Timestamp.valueOf("2004-10-03 22:59:59.123"); //$NON-NLS-1$
        helpInvokeMethod("modifyTimeZone", new Object[] {ts, "America/Chicago", "America/New_York" }, out); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // TimestampWithTimezone has a static copy the default timezone object which makes this test not execute properly
    public void defer_testInvokeModifyTimeZoneFromLocal() {
        Timestamp ts = Timestamp.valueOf("2004-10-03 23:59:59.123"); //$NON-NLS-1$
        Timestamp out = Timestamp.valueOf("2004-10-03 21:59:59.123"); //$NON-NLS-1$
        helpInvokeMethod("modifyTimeZone", new Object[] {ts, "America/New_York" }, out); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvokeRand() throws Exception {
        helpInvokeMethod("rand", new Object[] {new Integer(100)}, new Double(0.7220096548596434)); //$NON-NLS-1$
        // this does not actually fail but returns a result
        assertNotNull(helpInvokeMethod("rand", new Class<?>[] {Integer.class}, new Object[] {null}, null)); //$NON-NLS-1$
    }

    @Test public void testInvokeUser() throws Exception {
        CommandContext c = new CommandContext();
        c.setUserName("foodude"); //$NON-NLS-1$
        c.setSession(new SessionMetadata());
        c.getSession().setSecurityDomain("x");
        helpInvokeMethod("user", new Class<?>[] {}, new Object[] {}, c, "foodude@x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvokeEnv() throws Exception {
        helpInvokeMethod("env", new Class<?>[] {String.class}, new Object[] {null}, null); //$NON-NLS-1$
    }

    @Test public void testInvokeCommandPayload() throws Exception {
        CommandContext c = new CommandContext();
        c.setCommandPayload("payload_too heavy"); //$NON-NLS-1$
        helpInvokeMethod("commandpayload", new Class<?>[] {}, new Object[] {}, c, "payload_too heavy"); //$NON-NLS-1$ //$NON-NLS-2$
        helpInvokeMethod("commandpayload", new Class<?>[] {String.class}, new Object[] {null}, c, null); //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("payload", "payload_too heavy"); //$NON-NLS-1$ //$NON-NLS-2$
        c.setCommandPayload(props);
        helpInvokeMethod("commandpayload", new Class<?>[] {String.class}, new Object[] {"payload"}, c, "payload_too heavy"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testNullDependent() {
        FunctionDescriptor actual = library.findFunction("concat2", new Class<?>[] {String.class, String.class}); //$NON-NLS-1$
        assertTrue(actual.isNullDependent());

        actual = library.findFunction("concat", new Class<?>[] {String.class, String.class}); //$NON-NLS-1$
        assertFalse(actual.isNullDependent());
    }

    @Test public void testInvokeCeiling() {
        helpInvokeMethod("ceiling", new Object[] { new Double("3.14") }, new Double("4")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeFloor() {
        helpInvokeMethod("floor", new Object[] { new Double("3.14") }, new Double("3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeExp() {
        helpInvokeMethod("exp", new Object[] { new Double("0") }, new Double("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeLog() {
        helpInvokeMethod("log", new Object[] { new Double("1") }, new Double("0")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeLog10() {
        helpInvokeMethod("log10", new Object[] { new Double("10") }, new Double("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeLog10Error() throws Exception {
        helpInvokeMethodFail("log10", new Object[] { new Double("0") }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokePower() {
        helpInvokeMethod("power", new Object[] { new Double("10"), new Double("2") }, new Double("100")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testInvokePower1() {
        helpInvokeMethod("power", new Object[] { new BigDecimal("10"), new Integer(2) }, new BigDecimal("100")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeSqrt() {
        helpInvokeMethod("sqrt", new Object[] { new Double("4")}, new Double("2")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInvokeDayName() {
        String[] dayNames = FunctionMethods.getDayNames();
        for (int i = 0; i < dayNames.length; i++) {
            Date time = TimestampUtil.createDate(100, 0, i + 2);
            helpInvokeMethod("dayName", new Object[] { time }, dayNames[i]); //$NON-NLS-1$
        }
    }

    @Test public void testInvokeDayOfMonth() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("dayOfMonth", new Object[] { time }, new Integer(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeDayOfWeek() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("dayOfWeek", new Object[] { time }, new Integer(7)); //$NON-NLS-1$
    }

    @Test public void testInvokeDayOfYear() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 2, 1, 2, 3, 4);
        helpInvokeMethod("dayOfYear", new Object[] { time }, new Integer(2)); //$NON-NLS-1$
    }

    @Test public void testInvokeMonth() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("month", new Object[] { time }, new Integer(1)); //$NON-NLS-1$
    }

    @Test public void testInvokeMonthName() {
        String[] monthNames = FunctionMethods.getMonthNames();
        for (int i = 0; i < monthNames.length; i++) {
            Date time = TimestampUtil.createDate(100, i, 1);
            helpInvokeMethod("monthName", new Object[] { time }, monthNames[i]); //$NON-NLS-1$
        }
    }

    @Test public void testInvokeMinute() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("minute", new Object[] { time }, new Integer(2)); //$NON-NLS-1$
    }

    @Test public void testInvokeSecond() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("second", new Object[] { time }, new Integer(3)); //$NON-NLS-1$
    }

    @Test public void testInvokeWeek() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("week", new Object[] { time }, 52); //$NON-NLS-1$
    }

    @Test public void testInvokeYear() {
        Timestamp time = TimestampUtil.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("year", new Object[] { time }, new Integer(2000)); //$NON-NLS-1$
    }

    @Test public void testInvokeCoalesce() {
        helpInvokeMethod(FunctionLibrary.COALESCE, new Object[] { Integer.valueOf(0), Integer.valueOf(1), Integer.valueOf(2) }, Integer.valueOf(0));
    }

    @Test public void testInvokeCoalesce1() {
        helpInvokeMethod(FunctionLibrary.COALESCE, new Object[] { null, null}, null);
    }

    @Test public void testInvokeNull() throws Exception {
        helpInvokeMethod(SourceSystemFunctions.LTRIM, new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING}, new Object[] { null }, null, null);
    }

    @Test public void testInvokeNull1() throws Exception {
        helpInvokeMethod(SourceSystemFunctions.CONCAT, new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING}, new Object[] { null, String.valueOf(1) }, null, null);
    }

    @Test public void testToChars() throws Exception {
        Clob result = (Clob)helpInvokeMethod("to_chars", new Class<?>[] {DefaultDataClasses.BLOB, DefaultDataClasses.STRING}, new Object[] { new BlobType(new SerialBlob("hello world".getBytes("ASCII"))), "ASCII" }, null); //$NON-NLS-1$
        String string = result.getSubString(1, (int)result.length());
        assertEquals("hello world", string);
    }

    @Test public void testToBytes() throws Exception {
        Blob result = (Blob)helpInvokeMethod("to_bytes", new Class<?>[] {DefaultDataClasses.CLOB, DefaultDataClasses.STRING}, new Object[] { new ClobType(new SerialClob("hello world".toCharArray())), "UTF32" }, null); //$NON-NLS-1$
        assertEquals(44, result.length()); //4 bytes / char
    }

    @Test public void testToBytes1() throws Exception {
        helpInvokeMethodFail("to_bytes", new Object[] { new ClobType(new SerialClob("hello\uffffworld".toCharArray())), "ASCII", Boolean.FALSE }); //$NON-NLS-1$
    }

    @Test public void testToChars1() throws Exception {
        Clob result = (Clob)helpInvokeMethod("to_chars", new Class<?>[] {DefaultDataClasses.BLOB, DefaultDataClasses.STRING}, new Object[] { new BlobType(new SerialBlob("hello world".getBytes("ASCII"))), "BASE64" }, null); //$NON-NLS-1$
        String string = result.getSubString(1, (int)result.length());
        assertEquals("hello world", new String(Base64.decode(string), "ASCII"));
    }

    @Test public void testToChars2() throws Exception {
        Clob result = (Clob)helpInvokeMethod("to_chars", new Class<?>[] {DefaultDataClasses.BLOB, DefaultDataClasses.STRING}, new Object[] { new BlobType(new SerialBlob("hello world".getBytes("ASCII"))), "HEX" }, null); //$NON-NLS-1$
        String string = result.getSubString(1, (int)result.length());
        assertEquals("68656C6C6F20776F726C64", string);
    }

    //no bom
    @Test(expected=SQLException.class) public void testToChars3() throws Exception {
        Clob result = (Clob)helpInvokeMethod("to_chars", new Class<?>[] {DefaultDataClasses.BLOB, DefaultDataClasses.STRING}, new Object[] { new BlobType(new SerialBlob("hello world".getBytes("ASCII"))), "UTF-8-BOM" }, null); //$NON-NLS-1$
        result.getSubString(1, (int)result.length());
    }

    @Test public void testToChars4() throws Exception {
        byte[] stringBytes = "hello world".getBytes("UTF-8");
        byte[] bytes = new byte[stringBytes.length+3];
        bytes[0] = (byte)0xef;
        bytes[1] = (byte)0xbb;
        bytes[2] = (byte)0xbf;
        System.arraycopy(stringBytes, 0, bytes, 3, stringBytes.length);
        Clob result = (Clob)helpInvokeMethod("to_chars", new Class<?>[] {DefaultDataClasses.BLOB, DefaultDataClasses.STRING}, new Object[] { new BlobType(new SerialBlob(bytes)), "UTF-8-BOM" }, null); //$NON-NLS-1$
        String string = result.getSubString(1, (int)result.length());
        assertEquals("hello world", string); //bom is stripped
    }

    @Test public void testToBytes2() throws Exception {
        Blob result = (Blob)helpInvokeMethod("to_bytes", new Class<?>[] {DefaultDataClasses.CLOB, DefaultDataClasses.STRING}, new Object[] { new ClobType(new SerialClob("68656C6C6F20776F726C64".toCharArray())), "HEX" }, null); //$NON-NLS-1$
        assertEquals("hello world", new String(ObjectConverterUtil.convertToCharArray(result.getBinaryStream(), -1, "ASCII")));
    }

    @Test(expected=FunctionExecutionException.class) public void testToBytes3() throws Exception {
        helpInvokeMethod("to_bytes", new Class<?>[] {DefaultDataClasses.CLOB, DefaultDataClasses.STRING}, new Object[] { new ClobType(new SerialClob("a".toCharArray())), "BASE64" }, null); //$NON-NLS-1$
    }

    @Test public void testToBytes4() throws Exception {
        Blob result = (Blob)helpInvokeMethod("to_bytes", new Class<?>[] {DefaultDataClasses.CLOB, DefaultDataClasses.STRING}, new Object[] { new ClobType(new SerialClob("hello world 平仮名".toCharArray())), "utf_8_BOM" }, null); //$NON-NLS-1$
        byte[] bytes = ObjectConverterUtil.convertToByteArray(result.getBinaryStream());
        assertEquals(bytes[0], (byte)0xef);
        assertEquals(bytes[1], (byte)0xbb);
        assertEquals(bytes[2], (byte)0xbf);
        assertEquals("﻿hello world 平仮名", new String(bytes, "UTF-8"));
    }

    @Test() public void testUnescape() throws Exception {
        assertEquals("\r\t", helpInvokeMethod("unescape", new Class<?>[] {DefaultDataClasses.STRING}, new Object[] { "\r\\\t" }, null)); //$NON-NLS-1$
    }

    @Test() public void testUuid() throws Exception {
        assertNotNull(helpInvokeMethod("uuid", new Class<?>[] {}, new Object[] {}, null)); //$NON-NLS-1$
    }

    @Test() public void testArrayGet() throws Exception {
        assertEquals(1, helpInvokeMethod("array_get", new Class<?>[] {DefaultDataClasses.OBJECT, DefaultDataClasses.INTEGER}, new Object[] {new Object[] {1}, 1}, null)); //$NON-NLS-1$
    }

    @Test() public void testArrayGetClob() throws Exception {
        assertEquals(new String(new char[5000]), helpInvokeMethod("array_get", new Class<?>[] {DefaultDataClasses.OBJECT, DefaultDataClasses.INTEGER}, new Object[] {new Object[] {new String(new char[5000])}, 1}, null)); //$NON-NLS-1$
    }

    @Test() public void testTrim() throws Exception {
        helpInvokeMethod("trim", new Object[] {"leading", "x", "xaxx"}, "axx"); //$NON-NLS-1$
    }

    @Test() public void testTrim1() throws Exception {
        helpInvokeMethod("trim", new Object[] {"both", " ", "   a   "}, "a"); //$NON-NLS-1$
    }

    @Test() public void testTrim2() throws Exception {
        helpInvokeMethod("trim", new Object[] {"trailing", "x", "xaxx"}, "xa"); //$NON-NLS-1$
    }

    @Test public void testCastWithNonRuntimeTypes() throws Exception {
        helpInvokeMethod("cast", new Object[] {new java.util.Date(0), "time"}, new Time(24*60*60*1000)); //$NON-NLS-1$
        helpInvokeMethod("cast", new Object[] {new byte[0], "blob"}, new BlobType(BlobType.createBlob(new byte[0]))); //$NON-NLS-1$
    }

    @Test public void testSessionVariables() throws Exception {
        CommandContext c = new CommandContext();
        c.setSession(new SessionMetadata());
        Object result = helpInvokeMethod("teiid_session_set", new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.OBJECT}, new Object[] {"key", "value"}, c);
        assertNull(result);
        result = helpInvokeMethod("teiid_session_get", new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING}, new Object[] {"key"}, c);
        assertEquals("value", result);
        result = helpInvokeMethod("teiid_session_set", new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.OBJECT}, new Object[] {"key", "value1"}, c);
        assertEquals("value", result);
        result = helpInvokeMethod("teiid_session_get", new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING}, new Object[] {"key"}, c);
        assertEquals("value1", result);
    }

    @Test public void testTokenize() throws Exception {
        helpInvokeMethod("tokenize", new Object[] {"bxaxxc", 'x'}, new ArrayImpl("b", "axc")); //$NON-NLS-1$
    }

    @Test public void testNodeId() throws Exception {
        System.setProperty("jboss.node.name", "x");
        helpInvokeMethod("node_id", new Object[] {}, "x"); //$NON-NLS-1$
        helpInvokeMethod("sys_prop", new Object[] {"jboss.node.name"}, "x"); //$NON-NLS-1$
    }

    @Test public void testEnvVar() throws Exception {
        helpInvokeMethod("env_var", new Class<?>[] {String.class}, new Object[] {"x"}, null);
    }

    @Test public void testGetBuiltin() throws Exception {
        assertEquals(18, RealMetadataFactory.SFM.getSystemFunctionLibrary().getBuiltInAggregateFunctions(false).size());
    }

    @Test public void testClobConcat() throws Exception {
        CommandContext c = new CommandContext();
        c.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
        ClobType result = (ClobType)helpInvokeMethod("concat", new Class<?>[] {DataTypeManager.DefaultDataClasses.CLOB, DataTypeManager.DefaultDataClasses.CLOB},
                new Object[] {DataTypeManager.transformValue("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Catalog><Items><Item ItemID=\"001\"><Name>Lamp</Name><Quantity>5</Quantity></Item></Items></Catalog></Catalogs>", DataTypeManager.DefaultDataClasses.CLOB),
                DataTypeManager.transformValue("<?xml version=\"1.0\" encoding=\"UTF-8\"?><xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"><xsl:template match=\"@*|node()\"><xsl:copy><xsl:apply-templates select=\"@*|node()\"/></xsl:copy></xsl:template><xsl:template match=\"Quantity\"/></xsl:stylesheet>", DataTypeManager.DefaultDataClasses.CLOB)}, c);

        String xml = ObjectConverterUtil.convertToString(result.getCharacterStream());
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Catalog><Items><Item ItemID=\"001\">"
                + "<Name>Lamp</Name><Quantity>5</Quantity></Item></Items></Catalog></Catalogs><?xml version=\"1.0\" encoding=\"UTF-8\"?><xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
                + "<xsl:template match=\"@*|node()\"><xsl:copy><xsl:apply-templates select=\"@*|node()\"/></xsl:copy></xsl:template><xsl:template match=\"Quantity\"/></xsl:stylesheet>", xml);
    }

}
