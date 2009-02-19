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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.TestCase;

import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.api.exception.query.InvalidFunctionException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.unittest.TimestampUtil;
import com.metamatrix.query.util.CommandContext;

public class TestFunctionLibrary extends TestCase {

	// These are just used as shorthand convenience to make unit tests more readable below
	private static final Class T_STRING = DataTypeManager.DefaultDataClasses.STRING;
	private static final Class T_INTEGER = DataTypeManager.DefaultDataClasses.INTEGER;
	private static final Class T_BIG_INTEGER = DataTypeManager.DefaultDataClasses.BIG_INTEGER;
	private static final Class T_BIG_DECIMAL = DataTypeManager.DefaultDataClasses.BIG_DECIMAL;		
    private static final Class T_LONG = DataTypeManager.DefaultDataClasses.LONG;
	private static final Class T_FLOAT = DataTypeManager.DefaultDataClasses.FLOAT;
    private static final Class T_DOUBLE = DataTypeManager.DefaultDataClasses.DOUBLE;
	private static final Class T_NULL = DataTypeManager.DefaultDataClasses.NULL;
	private static final Class T_TIME = DataTypeManager.DefaultDataClasses.TIME;
	private static final Class T_DATE = DataTypeManager.DefaultDataClasses.DATE;
	private static final Class T_TIMESTAMP = DataTypeManager.DefaultDataClasses.TIMESTAMP;
	
	private FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();

	TimestampUtil tsUtil;

    // ################################## FRAMEWORK ################################
	
	public TestFunctionLibrary(String name) { 
		super(name);
        tsUtil = new TimestampUtil();
	}	
	
	public void setUp() { 
		TimeZone.setDefault(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$ 
	}
	
	public void tearDown() { 
		TimeZone.setDefault(null);
	}
	
	// ################################## TEST HELPERS ################################
	
    private FunctionDescriptor helpCreateDescriptor(String name, Class[] types) { 
        final String fname = name;
        final Class[] ftypes = types;
        return new FunctionDescriptor() {
            public String getName() {
                return fname;
            }
            public int getPushdown() {
                return FunctionMethod.CAN_PUSHDOWN;
            }
            public Class[] getTypes() { 
                return ftypes;
            }
            public Class getReturnType() { 
                return null;
            }
            
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
            public boolean requiresContext() {
                return false;
            }
            public boolean isNullDependent() {
                return true;
            }
            
        };
    }
    
	private void helpFindFunction(String fname, Class[] types, FunctionDescriptor expected) {
		FunctionDescriptor actual =  library.findFunction(fname, types);
	
        assertEquals("Function names do not match: ", expected.getName().toLowerCase(), actual.getName().toLowerCase());             //$NON-NLS-1$
        assertEquals("Arg lengths do not match: ", expected.getTypes().length, actual.getTypes().length); //$NON-NLS-1$
	}

	private void helpFindFunctionFail(String fname, Class[] types) {
		FunctionDescriptor actual =  library.findFunction(fname, types);
		assertNull("Function was found but should not have been: " + actual, actual); //$NON-NLS-1$
	}
    
	private void helpFindConversions(String fname, Class[] types, FunctionDescriptor[] expected) {

		FunctionDescriptor[] actual = library.determineNecessaryConversions(fname, types, false);
		
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
		FunctionForm form = library.findFunctionForm(fname, numArgs);
        assertNotNull("Failed to find function '" + fname + "' with " + numArgs + " args", form); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("Function names do not match: ", fname.toUpperCase(), form.getName().toUpperCase());             //$NON-NLS-1$
        assertEquals("Arg lengths do not match: ", numArgs, form.getArgNames().size()); //$NON-NLS-1$
	}

	private void helpInvokeMethod(String fname, Object[] inputs, Object expectedOutput) {
        // Build type signature
        Class[] types = new Class[inputs.length];
        for(int i=0; i<inputs.length; i++) { 
            types[i] = DataTypeManager.determineDataTypeClass(inputs[i]);   
        }        
        try {
            helpInvokeMethod(fname, types, inputs, new CommandContext(), expectedOutput);
        } catch (Exception err) {
            throw new RuntimeException(err);
        } 
	}
    
    private void helpInvokeMethod(String fname, Class[] types, Object[] inputs, CommandContext context , Object expectedOutput) throws InvalidFunctionException, FunctionExecutionException {
        Object actualOutput = null;
        // Find function descriptor
        FunctionDescriptor descriptor = library.findFunction(fname, types);         
        if (descriptor != null && descriptor.requiresContext()) {
            // Invoke function with inputs
            Object[] in = new Object[inputs.length+1];
            in[0] = context;
            for (int i = 0; i < inputs.length; i++) {
                in[i+1] = inputs[i];
            }
            actualOutput = library.invokeFunction(descriptor, in);
        }
        else {
            // Invoke function with inputs
            actualOutput = library.invokeFunction(descriptor, inputs);                
        }
        assertEquals("Actual function output not equal to expected: ", expectedOutput, actualOutput); //$NON-NLS-1$
    }
    	    
	private void helpInvokeMethodFail(String fname, Object[] inputs, Object expectedException) {
		Object actualOutput = null;
		try {
			// Build type signature
			Class[] types = new Class[inputs.length];
			for(int i=0; i<inputs.length; i++) { 
				types[i] = DataTypeManager.determineDataTypeClass(inputs[i]);   
			}
		    
            // Find function descriptor
            FunctionDescriptor descriptor = library.findFunction(fname, types);         
            if (descriptor != null && descriptor.requiresContext()) {
                // Invoke function with inputs
                Object[] in = new Object[inputs.length+1];
                in[0] = new CommandContext();
                for (int i = 0; i < inputs.length; i++) {
                    in[i+1] = inputs[i];
                }
                actualOutput = library.invokeFunction(descriptor, in);
            }
            else {
                // Invoke function with inputs
                actualOutput = library.invokeFunction(descriptor, inputs);                
            }	  
		    
		} catch(Throwable e) { 
			//e.printStackTrace();
			assertNull(actualOutput);
			assertEquals("Unexpected exception.", e.getClass().getName(), ((FunctionExecutionException)expectedException).getClass().getName());    //$NON-NLS-1$
		}    
	}	
    	
    private void helpInvokeMethodFail(String fname, Class types[], Object[] inputs, Object expectedException) {
        Object actualOutput = null;
        try {            
            // Find function descriptor
            FunctionDescriptor descriptor = library.findFunction(fname, types);         
            if (descriptor != null && descriptor.requiresContext()) {
                // Invoke function with inputs
                Object[] in = new Object[inputs.length+1];
                in[0] = new CommandContext();
                for (int i = 0; i < inputs.length; i++) {
                    in[i+1] = inputs[i];
                }
                actualOutput = library.invokeFunction(descriptor, in);
            }
            else {
                // Invoke function with inputs
                actualOutput = library.invokeFunction(descriptor, inputs);                
            }     
            
        } catch(Throwable e) { 
            //e.printStackTrace();
            assertNull(actualOutput);
            assertEquals("Unexpected exception.", e.getClass().getName(), ((FunctionExecutionException)expectedException).getClass().getName());    //$NON-NLS-1$
        }    
    }    
	// ################################## ACTUAL TESTS ################################
	
	public void testFindFunction1() { 
		helpFindFunction("convert", new Class[] { T_INTEGER, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_INTEGER, T_STRING }) );
	}
	
	public void testFindFunction2() { 
		helpFindFunction("cast", new Class[] { T_INTEGER, T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor(FunctionLibrary.CAST, new Class[] { T_INTEGER, T_STRING }) );
	}
	
    public void testFindFunction3() {
        helpFindFunction("curdate", new Class[0], //$NON-NLS-1$
        	helpCreateDescriptor("curdate", new Class[0])); //$NON-NLS-1$
    }
   
    public void testFindFunction4() {
        helpFindFunctionFail("curdate", new Class[] { T_INTEGER }); //$NON-NLS-1$
    }
    
    public void testFindFunction5() {
        helpFindFunction("+", new Class[] { T_INTEGER, T_INTEGER }, //$NON-NLS-1$
        	helpCreateDescriptor("+", new Class[] { T_INTEGER, T_INTEGER }) ); //$NON-NLS-1$
    }
    
    public void testFindFunction6() {
        helpFindFunctionFail("+", new Class[] {T_INTEGER, T_FLOAT}); //$NON-NLS-1$
    }
    
    public void testFindFunction7() {
        helpFindFunctionFail("+", new Class[] {T_INTEGER, T_FLOAT, T_INTEGER}); //$NON-NLS-1$
    }
    
    public void testFindFunction8() {
        helpFindFunctionFail("+", new Class[] {T_INTEGER}); //$NON-NLS-1$
    }
    
    public void testFindFunction9() {        
		helpFindFunctionFail("+", new Class[] {T_INTEGER, T_NULL });     //$NON-NLS-1$
    }

    public void testFindFunction10() {
        helpFindFunction("substring", new Class[] { T_STRING, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
            helpCreateDescriptor("substring", new Class[] { T_STRING, T_INTEGER, T_INTEGER }) ); //$NON-NLS-1$
    }

    public void testFindFunction11() {
        helpFindFunction("substring", new Class[] { T_STRING, T_INTEGER }, //$NON-NLS-1$
            helpCreateDescriptor("substring", new Class[] { T_STRING, T_INTEGER }) ); //$NON-NLS-1$
    }

    public void testFindFunction12() {
        helpFindFunction("context", new Class[] { T_STRING, T_INTEGER }, //$NON-NLS-1$
            helpCreateDescriptor("context", new Class[] { T_STRING, T_INTEGER }) ); //$NON-NLS-1$
    }

    public void testFindFunction12a() {
        helpFindFunction("rowlimit", new Class[] { T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("rowlimit", new Class[] { T_STRING }) ); //$NON-NLS-1$
    }

    public void testFindFunction12b() {
        helpFindFunction("rowlimitexception", new Class[] { T_STRING }, //$NON-NLS-1$
            helpCreateDescriptor("rowlimitexception", new Class[] { T_STRING }) ); //$NON-NLS-1$
    }
    
	public void testFind0ArgConversion1() { 	
		helpFindConversions(
			"curdate", new Class[] {}, //$NON-NLS-1$
			new FunctionDescriptor[0] );
	}

	public void testFind0ArgConversion2() { 	
		helpFindConversions(
			"curdate", new Class[] { T_INTEGER }, //$NON-NLS-1$
			null );
	}

	public void testFind1ArgConversion1() { 	
		helpFindConversions(
			"length", new Class[] { T_STRING }, //$NON-NLS-1$
			new FunctionDescriptor[1] );
	}

	public void testFind1ArgConversion2() { 	
		helpFindConversions(
			"length", new Class[] { T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[] {
			    helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_INTEGER, T_STRING })
			} );
	}

	public void testFind1ArgConversion3() { 	
		helpFindConversions(
			"length", new Class[] { DataTypeManager.DefaultDataClasses.TIMESTAMP }, //$NON-NLS-1$
			new FunctionDescriptor[] {
			    helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.TIMESTAMP, T_STRING })
			} );
	}

	public void testFind2ArgConversion1() { 	
		helpFindConversions(
			"+", new Class[] { T_INTEGER, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[2] );
	}

	public void testFind2ArgConversion2() { 	
		helpFindConversions(
			"+", new Class[] { T_INTEGER, T_FLOAT }, //$NON-NLS-1$
			new FunctionDescriptor[] { 
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_INTEGER, T_STRING }), 
				null } );
	}

	public void testFind2ArgConversion3() { 	
		helpFindConversions(
			"+", new Class[] { T_FLOAT, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[] { 
			    null, 
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_INTEGER, T_STRING }) } );
	}

	public void testFind2ArgConversion4() { 	
		helpFindConversions(
			"+", new Class[] { T_STRING, T_FLOAT }, //$NON-NLS-1$
			null );
	}

	public void testFind2ArgConversion5() { 	
		helpFindConversions(
			"+", new Class[] { T_NULL, T_NULL }, //$NON-NLS-1$
			new FunctionDescriptor[] { 
			    helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }), 
                helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }) } );
	}

	public void testFind2ArgConversion6() { 	
		helpFindConversions(
			"+", new Class[] { T_NULL, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[] { 
			    helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }), 
				null } );
	}

	public void testFind2ArgConversion7() { 	
		helpFindConversions(
			"+", new Class[] { T_INTEGER, T_NULL }, //$NON-NLS-1$
			new FunctionDescriptor[] { 
			    null, 
			    helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }) } );
	}

	public void testFind3ArgConversion1() { 	
		helpFindConversions(
			"substring", new Class[] { T_STRING, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[3] );
	}

	public void testFind3ArgConversion2() { 	
		helpFindConversions(
			"substring", new Class[] { T_INTEGER, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[] {
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_INTEGER, T_STRING }),
				null,
				null    
			} );
	}

	public void testFind3ArgConversion3() { 	
		helpFindConversions(
			"substring", new Class[] { T_INTEGER, T_INTEGER, DataTypeManager.DefaultDataClasses.SHORT }, //$NON-NLS-1$
			new FunctionDescriptor[] {
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_INTEGER, T_STRING }),
				null,
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING })    
			} );
	}

	public void testFind3ArgConversion4() { 	
		helpFindConversions(
			"substring", new Class[] { T_STRING, T_INTEGER, DataTypeManager.DefaultDataClasses.TIMESTAMP }, //$NON-NLS-1$
			null );
	}

	public void testFind3ArgConversion5() { 	
		helpFindConversions(
			"substring", new Class[] { T_STRING, DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.SHORT }, //$NON-NLS-1$
			new FunctionDescriptor[] {
			    null,
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING }),    
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING })    
			} );
	}

	public void testFind3ArgConversion6() { 	
		helpFindConversions(
			"substring", new Class[] { T_INTEGER, DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.SHORT }, //$NON-NLS-1$
			new FunctionDescriptor[] {
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.INTEGER, T_STRING }),    
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING }),    
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { DataTypeManager.DefaultDataClasses.SHORT, T_STRING })    
			} );
	}

	public void testFind3ArgConversion7() { 	
		helpFindConversions(
			"substring", new Class[] { T_NULL, T_INTEGER, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[] {
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }),    
				null,
				null }
			);
	}

	public void testFind3ArgConversion8() { 	
		helpFindConversions(
			"substring", new Class[] { T_NULL, T_NULL, T_INTEGER }, //$NON-NLS-1$
			new FunctionDescriptor[] {
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }),    
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }),    
				null }
			);
	}

	public void testFind3ArgConversion9() { 	
		helpFindConversions(
			"substring", new Class[] { T_NULL, T_NULL, T_NULL }, //$NON-NLS-1$
			new FunctionDescriptor[] {
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }),    
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }),    
				helpCreateDescriptor(FunctionLibrary.CONVERT, new Class[] { T_NULL, T_STRING }) }
			);
	}

    // Walk through all functions by metadata 
    public void testEnumerateForms() {
        FunctionLibrary lib = FunctionLibraryManager.getFunctionLibrary();
        
        Collection categories = lib.getFunctionCategories();
        Iterator catIter = categories.iterator();
        while(catIter.hasNext()) { 
            String category = (String) catIter.next();
            //System.out.println("Category: " + category);
            
            Collection functions = lib.getFunctionForms(category);
            Iterator functionIter = functions.iterator();
            while(functionIter.hasNext()) { 
                FunctionForm form = (FunctionForm) functionIter.next();
                //System.out.println("\tFunction: " + form.getDisplayString());                
                
                // Lookup this form
                helpFindForm(form.getName(), form.getArgNames().size());
            }            
        }        
    }

	public void testFindForm1() { 
		helpFindForm("convert", 2); //$NON-NLS-1$
	}
 
	public void testFindForm2() { 
		helpFindForm("locate", 2); //$NON-NLS-1$
	}
    
	public void testFindForm3() { 
		helpFindForm("locate", 3); //$NON-NLS-1$
	}

    public void testFindForm4() { 
        helpFindForm("substring", 2); //$NON-NLS-1$
    }
    
    public void testFindForm5() { 
        helpFindForm("substring", 3); //$NON-NLS-1$
    }
 
	public void testFindForm6() { 
		helpFindForm("now", 0); //$NON-NLS-1$
	}
    
    public void testInvokePlus1() { 
    	helpInvokeMethod("+", new Object[] { new Integer(3), new Integer(2) }, new Integer(5)); //$NON-NLS-1$
    }

    public void testInvokePlus2() {
        helpInvokeMethod("+", new Object[] { new Long(3), new Long(2) }, new Long(5)); //$NON-NLS-1$
    }

    public void testInvokePlus3() {
        helpInvokeMethod("+", new Object[] { new Float(3), new Float(2) }, new Float(5)); //$NON-NLS-1$
    }

    public void testInvokePlus4() {
        helpInvokeMethod("+", new Object[] { new Double(3), new Double(2) }, new Double(5)); //$NON-NLS-1$
    }

    public void testInvokePlus5() {
        helpInvokeMethod("+", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("5")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokePlus6() {
        helpInvokeMethod("+", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("5")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeMinus1() {
        helpInvokeMethod("-", new Object[] { new Integer(3), new Integer(2) }, new Integer(1)); //$NON-NLS-1$
    }

    public void testInvokeMinus2() {
        helpInvokeMethod("-", new Object[] { new Long(3), new Long(2) }, new Long(1)); //$NON-NLS-1$
    }

    public void testInvokeMinus3() {
        helpInvokeMethod("-", new Object[] { new Float(3), new Float(2) }, new Float(1)); //$NON-NLS-1$
    }

    public void testInvokeMinus4() {
        helpInvokeMethod("-", new Object[] { new Double(3), new Double(2) }, new Double(1)); //$NON-NLS-1$
    }

    public void testInvokeMinus5() {
        helpInvokeMethod("-", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeMinus6() {
        helpInvokeMethod("-", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeMultiply1() {
        helpInvokeMethod("*", new Object[] { new Integer(3), new Integer(2) }, new Integer(6)); //$NON-NLS-1$
    }

    public void testInvokeMultiply2() {
        helpInvokeMethod("*", new Object[] { new Long(3), new Long(2) }, new Long(6)); //$NON-NLS-1$
    }

    public void testInvokeMultiply3() {
        helpInvokeMethod("*", new Object[] { new Float(3), new Float(2) }, new Float(6)); //$NON-NLS-1$
    }

    public void testInvokeMultiply4() {
        helpInvokeMethod("*", new Object[] { new Double(3), new Double(2) }, new Double(6)); //$NON-NLS-1$
    }

    public void testInvokeMultiply5() {
        helpInvokeMethod("*", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("6")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeMultiply6() {
        helpInvokeMethod("*", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("6")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeDivide1() {
        helpInvokeMethod("/", new Object[] { new Integer(3), new Integer(2) }, new Integer(1)); //$NON-NLS-1$
    }

    public void testInvokeDivide2() {
        helpInvokeMethod("/", new Object[] { new Long(3), new Long(2) }, new Long(1)); //$NON-NLS-1$
    }

    public void testInvokeDivide3() {
        helpInvokeMethod("/", new Object[] { new Float(3), new Float(2) }, new Float(1.5)); //$NON-NLS-1$
    }

    public void testInvokeDivide4() {
        helpInvokeMethod("/", new Object[] { new Double(3), new Double(2) }, new Double(1.5)); //$NON-NLS-1$
    }

    public void testInvokeDivide5() {
        helpInvokeMethod("/", new Object[] { new BigInteger("3"), new BigInteger("2") }, new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    // one digit precision
    public void testInvokeDivide6() {
        helpInvokeMethod("/", new Object[] { new BigDecimal("3"), new BigDecimal("2") }, new BigDecimal("2"));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeAbs1() {
        helpInvokeMethod("abs", new Object[] { new Integer(-3) }, new Integer(3)); //$NON-NLS-1$
    }

    public void testInvokeAbs2() {
        helpInvokeMethod("abs", new Object[] { new Long(-3) }, new Long(3)); //$NON-NLS-1$
    }

    public void testInvokeAbs3() {
        helpInvokeMethod("abs", new Object[] { new Float(-3) }, new Float(3)); //$NON-NLS-1$
    }

    public void testInvokeAbs4() {
        helpInvokeMethod("abs", new Object[] { new Double(-3) }, new Double(3)); //$NON-NLS-1$
    }

    public void testInvokeAbs5() {
        helpInvokeMethod("abs", new Object[] { new BigInteger("-3") }, new BigInteger("3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInvokeAbs6() {
        helpInvokeMethod("abs", new Object[] { new BigDecimal("-3") }, new BigDecimal("3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

	public void testInvokeAcos() {
		helpInvokeMethod("acos", new Object[] { new Double(0.05) }, new Double(1.5207754699891267)); //$NON-NLS-1$
	}

	public void testInvokeAsin() {
		helpInvokeMethod("asin", new Object[] { new Double(0.05) }, new Double(0.050020856805770016)); //$NON-NLS-1$
	}

	public void testInvokeAtan() {
		helpInvokeMethod("atan", new Object[] { new Double(0.05) }, new Double(0.049958395721942765)); //$NON-NLS-1$
	}

	public void testInvokeAtan2() {
		helpInvokeMethod("atan2", new Object[] { new Double(0.05), new Double(0.07) }, new Double(0.6202494859828215)); //$NON-NLS-1$
	}

	public void testInvokeCos() {
		helpInvokeMethod("cos", new Object[] { new Double(1.57) }, new Double(7.963267107332633E-4)); //$NON-NLS-1$
	}

	public void testInvokeCot() {
		helpInvokeMethod("cot", new Object[] { new Double(1.57) }, new Double(7.963269632231926E-4)); //$NON-NLS-1$
	}

	public void testInvokeDegrees() {
		helpInvokeMethod("degrees", new Object[] { new Double(1.57) }, new Double(89.95437383553926)); //$NON-NLS-1$
	}

	public void testInvokePI() {
		helpInvokeMethod("pi", new Object[] { }, new Double(3.141592653589793)); //$NON-NLS-1$
	}

	public void testInvokeRadians() {
		helpInvokeMethod("radians", new Object[] { new Double(89.95437383553926) }, new Double(1.57)); //$NON-NLS-1$
	}
	
	public void testInvokeSin() {
		helpInvokeMethod("sin", new Object[] { new Double(1.57) }, new Double(0.9999996829318346)); //$NON-NLS-1$
	}
	
	public void testInvokeTan() {
		helpInvokeMethod("tan", new Object[] { new Double(0.785) }, new Double(0.9992039901050427)); //$NON-NLS-1$
	}
							
    public void testInvokeAscii() {
        helpInvokeMethod("ascii", new Object[] { " " }, new Integer(32)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInvokeChr() {
        helpInvokeMethod("chr", new Object[] { new Integer(32) }, new Character(' ')); //$NON-NLS-1$
    }
    
    public void testInvokeNvl() {
        helpInvokeMethod("nvl", new Object[] { new Integer(5), new Integer(10) }, new Integer(5) ); //$NON-NLS-1$
    }

    public void testInvokeConcatOperator() {
        helpInvokeMethod("||", new Object[] { "a", "b" }, "ab" );         //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testInvokeInitcap() {
        helpInvokeMethod("initcap", new Object[] { "my test\ndata" }, "My Test\nData" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInvokeLpad1() {
        helpInvokeMethod("lpad", new Object[] { "x", new Integer(3) }, "  x" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInvokeLpad2() {
        helpInvokeMethod("lpad", new Object[] { "x", new Integer(3), "y" }, "yyx" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokeRpad1() {
        helpInvokeMethod("rpad", new Object[] { "x", new Integer(3) }, "x  " ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInvokeRpad2() {
        helpInvokeMethod("rpad", new Object[] { "x", new Integer(3), "y" }, "xyy" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInvokeTranslate() {
        helpInvokeMethod("translate", new Object[] { "ababcd", "ad", "da" }, "dbdbca" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
	public void testFindFunction13() { 
		helpFindFunction("formatTime", new Class[] { T_TIME, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatTime", new Class[] { T_TIME, T_STRING }) ); //$NON-NLS-1$
	}
	
	public void testFindFunction14() { 
		helpFindFunction("formatDate", new Class[] { T_DATE, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatDate", new Class[] { T_DATE, T_STRING }) ); //$NON-NLS-1$
	}
	
	public void testFindFunction15() { 
		helpFindFunction("formatTimestamp", new Class[] { T_TIMESTAMP, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatTimestamp", new Class[] { T_TIMESTAMP, T_STRING }) ); //$NON-NLS-1$
	}
	
	public void testFindFunction16() { 
		helpFindFunction("parseTime", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseTime", new Class[] { T_STRING, T_STRING }) ); //$NON-NLS-1$
	}
	
	public void testFindFunction17() { 
		helpFindFunction("parseDate", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseDate", new Class[] { T_STRING, T_STRING }) ); //$NON-NLS-1$
	}
	
	public void testFindFunction18() { 
		helpFindFunction("parseTimestamp", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
				helpCreateDescriptor("parseTimestamp", new Class[] { T_STRING, T_STRING }) ); //$NON-NLS-1$
	}
	
    public void testFindFunction19() {
        helpFindFunction("env", new Class[] {T_STRING}, //$NON-NLS-1$
                         helpCreateDescriptor("env", new Class[] {T_STRING})); //$NON-NLS-1$
    }

	public void testInvokeFormatTime1() {
		helpInvokeMethod("formatTime", new Object[] {tsUtil.createTime(3,5,12), new String("h:mm a") }, "3:05 AM");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatTime2() {
		helpInvokeMethod("formatTime", new Object[] {tsUtil.createTime(13, 5,12), new String("K:mm a, z") }, "1:05 PM, GMT-06:00");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatTime3() {
		helpInvokeMethod("formatTime", new Object[] {tsUtil.createTime(13, 5,12), new String("HH:mm:ss z") }, "13:05:12 GMT-06:00");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatTime4() {
	    TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago")); //$NON-NLS-1$
		helpInvokeMethod("formatTime", new Object[] {tsUtil.createTime(13, 5,12), new String("hh a, zzzz") }, "01 PM, Central Standard Time");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatTimeFail() {
		helpInvokeMethodFail("formatTime", new Object[] {tsUtil.createTime(13, 5,12), new String("hh i, www") },  //$NON-NLS-1$ //$NON-NLS-2$
			new FunctionExecutionException("")); //$NON-NLS-1$
	}
		
	public void testInvokeFormatDate1() {
		helpInvokeMethod("formatDate", new Object[] {tsUtil.createDate(103, 2, 5), new String("yyyy.MM.dd G") }, "2003.03.05 AD");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatDate2() {
		helpInvokeMethod("formatDate", new Object[] {tsUtil.createDate(103, 2, 5), new String("EEE, MMM d, '' yy") }, "Wed, Mar 5, ' 03");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatDate3() {
		helpInvokeMethod("formatDate", new Object[] {new Date(12345678), new String("yyyy.MMMMM.dd GGG hh:mm aaa") }, "1969.December.31 AD 09:25 PM");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatDateFail() {
		helpInvokeMethodFail("formatTime", new Object[] {tsUtil.createTime(103, 2, 5), new String("yyyy.i.www") },  //$NON-NLS-1$ //$NON-NLS-2$
			new FunctionExecutionException("")); //$NON-NLS-1$
	}
	
	public void testInvokeFormatTimestamp1() {
		helpInvokeMethod("formatTimestamp", new Object[] {tsUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("mm/dd/yy h:mm a") }, "04/05/03 3:04 AM");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatTimestamp2() {
		helpInvokeMethod("formatTimestamp", new Object[] {tsUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("yyyy-mm-dd k:mm a z") }, "2003-04-05 3:04 AM GMT-06:00");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatTimestamp3() {
			helpInvokeMethod("formatTimestamp", new Object[] {tsUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("yyyy-mm-dd hh:mm:ss.SSSS") }, "2003-04-05 03:04:12.0000");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
		
	public void testInvokeFormatTimestampFail() {
		helpInvokeMethodFail("formatTimestamp", new Object[] {tsUtil.createTimestamp(103, 2, 5, 3, 4, 12, 255), new String("mm/dd/nn h:mm a") },  //$NON-NLS-1$ //$NON-NLS-2$
			new FunctionExecutionException("")); //$NON-NLS-1$
	}
	
	public void testInvokeParseTime1() {
		helpInvokeMethod("parseTime", new Object[] {new String("3:12 PM"), new String("h:mm a") }, tsUtil.createTime(15, 12, 0));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseTime2() {
		helpInvokeMethod("parseTime", new Object[] {new String("03:12:23 CST"), new String("hh:mm:ss z") }, tsUtil.createTime(3, 12, 23));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
		
	public void testInvokeParseDate1() {
		helpInvokeMethod("parseDate", new Object[] {new String("03/05/03"), new String("MM/dd/yy") }, tsUtil.createDate(103, 2, 5));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseDate2() {
		helpInvokeMethod("parseDate", new Object[] {new String("05-Mar-03"), new String("dd-MMM-yy") }, tsUtil.createDate(103, 2, 5));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testInvokeParseTimestamp1() {
		helpInvokeMethod("parseTimestamp", new Object[] {new String("05 Mar 2003 03:12:23 CST"), new String("dd MMM yyyy HH:mm:ss z") }, tsUtil.createTimestamp(103, 2, 5, 3, 12, 23, 0));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseTimestamp2() {
		helpInvokeMethod("parseTimestamp", new Object[] {new String("05 Mar 2003 03:12:23.333"), new String("dd MMM yyyy HH:mm:ss.SSS") }, tsUtil.createTimestamp(103, 2, 5, 3, 12, 23, 333*1000000));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testFindFormatInteger() { 
		helpFindFunction("formatInteger", new Class[] { T_INTEGER, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatInteger", new Class[] { T_INTEGER, T_STRING}) ); //$NON-NLS-1$
	}
	
	public void testFindFormatFloat() { 
		helpFindFunction("formatFloat", new Class[] { T_FLOAT, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatFloat", new Class[] { T_FLOAT, T_STRING}) ); //$NON-NLS-1$
	}

	public void testFindFormatDouble() { 
		helpFindFunction("formatDouble", new Class[] { T_DOUBLE, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatDouble", new Class[] { T_DOUBLE, T_STRING}) ); //$NON-NLS-1$
	}
		
	public void testFindFormatLong() { 
		helpFindFunction("formatLong", new Class[] { T_LONG, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatLong", new Class[] { T_LONG, T_STRING}) ); //$NON-NLS-1$
	}
	
	public void testFindFormatBigInteger() { 
		helpFindFunction("formatBigInteger", new Class[] { T_BIG_INTEGER, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatBigInteger", new Class[] { T_BIG_INTEGER, T_STRING}) ); //$NON-NLS-1$
	}

	public void testFindFormatBigDecimal() { 
		helpFindFunction("formatBigDecimal", new Class[] { T_BIG_DECIMAL, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("formatBigDecimal", new Class[] { T_BIG_DECIMAL, T_STRING}) ); //$NON-NLS-1$
	}
			
	public void testFindParseInteger() { 
		helpFindFunction("parseInteger", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseInteger", new Class[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
	}
	
	public void testFindParseLong() { 
		helpFindFunction("parseLong", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseLong", new Class[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
	}

	public void testFindParseDouble() { 
		helpFindFunction("parseDouble", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseDouble", new Class[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
	}	
	public void testFindParseFloat() { 
		helpFindFunction("parseFloat", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseFloat", new Class[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
	}
	
	public void testFindParseBigInteger() { 
		helpFindFunction("parseBigInteger", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseBigInteger", new Class[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
	}

	public void testFindParseBigDecimal() { 
		helpFindFunction("parseBigDecimal", new Class[] { T_STRING, T_STRING }, //$NON-NLS-1$
			helpCreateDescriptor("parseBigDecimal", new Class[] { T_STRING, T_STRING}) ); //$NON-NLS-1$
	}	
			
	public void testInvokeParseInteger() {
		helpInvokeMethod("parseInteger", new Object[] {new String("-1234"), new String("######")}, new Integer(-1234));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseLong() {
		helpInvokeMethod("parseLong", new Object[] {new String("123456"), new String("######.##")}, new Long(123456));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseDouble() {
		helpInvokeMethod("parseDouble", new Object[] {new String("123456.78"), new String("#####.#")}, new Double(123456.78));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseFloat() {
		helpInvokeMethod("parseFloat", new Object[] {new String("1234.56"), new String("####.###")}, new Float(1234.56));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeParseBigInteger() {
		helpInvokeMethod("parseBigInteger", new Object[] {new String("12345678"), new String("###,###")}, new BigInteger("12345678"));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	public void testInvokeParseBigDecimal() {
		helpInvokeMethod("parseBigDecimal", new Object[] {new String("1234.56"), new String("#####")}, new BigDecimal("1234.56"));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testInvokeFormatInteger() {
		helpInvokeMethod("formatInteger", new Object[] {new Integer(-1234), new String("######")}, "-1234");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatLong() {
		helpInvokeMethod("formatLong", new Object[] {new Long(123456788), new String("##,###,#")}, "1,2,3,4,5,6,7,8,8");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatDouble() {
		helpInvokeMethod("formatDouble", new Object[] {new Double(1234.67), new String("####.##")}, "1234.67");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatFloat() {
		helpInvokeMethod("formatFloat", new Object[] {new Float(1234.67), new String("###.###")}, "1234.67");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeFormatBigInteger() {
		helpInvokeMethod("formatBigInteger", new Object[] {new BigInteger("45"), new String("###.###")}, "45");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	public void testInvokeFormatBigDecimal() {
		helpInvokeMethod("formatBigDecimal", new Object[] {new BigDecimal("1234.56"), new String("###.###")}, "1234.56");	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}	

	public void testInvokeQuarter1() {
		//		2003-5-15
		helpInvokeMethod("quarter", new Object[] {tsUtil.createDate(103, 4, 15)}, new Integer(2));	 //$NON-NLS-1$
	}
	
	public void testInvokeQuarter2() {
		//		2003-5-1
		helpInvokeMethod("quarter", new Object[] {tsUtil.createDate(103, 3, 31)}, new Integer(2));	 //$NON-NLS-1$
	}
	
	public void testInvokeQuarter3() {
		//		2003-1-31
		helpInvokeMethod("quarter", new Object[] {tsUtil.createDate(103, 0, 31)}, new Integer(1));	 //$NON-NLS-1$
	}
	
	public void testInvokeQuarter4() {
	//		2003-9-30
		helpInvokeMethod("quarter", new Object[] {tsUtil.createDate(103, 8, 30)}, new Integer(3));	 //$NON-NLS-1$
	}
	
	public void testInvokeQuarter5() {
	//		2003-12-31
		helpInvokeMethod("quarter", new Object[] {tsUtil.createDate(103, 11, 31)}, new Integer(4));	 //$NON-NLS-1$
	}		
	
	public void testInvokeQuarter6() {
		//bad date such as 2003-13-45
		helpInvokeMethod("quarter", new Object[] {tsUtil.createDate(103, 12, 45)}, new Integer(1));	 //$NON-NLS-1$
	}
	
	public void testInvokeIfNull() {
		helpInvokeMethod("ifnull", new Object[] {new Integer(5), new Integer(10)}, new Integer(5));	 //$NON-NLS-1$
	}

	public void testInvokeLower() {
		helpInvokeMethod("lower", new Object[] {new String("LOWER")}, new String("lower"));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testInvokeUpper() {
		helpInvokeMethod("upper", new Object[] {new String("upper")}, new String("UPPER"));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testInvokeRepeat() {
		helpInvokeMethod("repeat", new Object[] {new String("cat"), new Integer(3)}, new String("catcatcat"));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testInvokeChar() {
		helpInvokeMethod("char", new Object[] {new Integer(32) }, new Character(' ')); //$NON-NLS-1$
	}

	/** normal input */
	public void testInvokeInsert1() {
		helpInvokeMethod("insert", new Object[] {new String("Downtown"), new Integer(4),  //$NON-NLS-1$ //$NON-NLS-2$
			new Integer(2), new String("cat")}, new String("Dowcatown"));	 //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** empty string2 */	
	public void testInvokeInsert2() {
		helpInvokeMethod("insert", new Object[] {new String("Downtown"), new Integer(4),  //$NON-NLS-1$ //$NON-NLS-2$
			new Integer(2), new String("")}, new String("Dowown"));	 //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** empty string1 with start = 1 and length = 0, so result is just string2 */	
	public void testInvokeInsert3() {
		helpInvokeMethod("insert", new Object[] {new String(""), new Integer(1),  //$NON-NLS-1$ //$NON-NLS-2$
			new Integer(0), new String("cat")}, new String("cat"));	 //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** should fail, with start > string1.length() */
	public void testInvokeInsert4() {
		helpInvokeMethodFail("insert", new Object[] {new String(""), new Integer(2),  //$NON-NLS-1$ //$NON-NLS-2$
			new Integer(0), new String("cat")}, new FunctionExecutionException("")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** should fail, with length > 0 and input string1.length() = 0 */
	public void testInvokeInsert5() {
		helpInvokeMethodFail("insert", new Object[] {new String(""), new Integer(1),  //$NON-NLS-1$ //$NON-NLS-2$
			new Integer(1), new String("cat")}, new FunctionExecutionException(""));	 //$NON-NLS-1$ //$NON-NLS-2$
	}

	/**  (length + start) > string1.length(), then just append str2 starting at start position */
	public void testInvokeInsert6() {
		helpInvokeMethod("insert", new Object[] {new String("Downtown"), new Integer(7),  //$NON-NLS-1$ //$NON-NLS-2$
			new Integer(5), new String("cat")}, new String("Downtocat"));	 //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** date + day --> count=28, inteval=day, result should be 2003-6-12 */
	public void testInvokeTimestampAddDate1() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_DAY,  //$NON-NLS-1$
			new Integer(28), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(103, 5, 12));	
	}	

    public void testInvokeTimestampAddDate_ignore_case() {
        helpInvokeMethod("timestampAdd", new Object[] {"sql_TSI_day",  //$NON-NLS-1$ //$NON-NLS-2$
            new Integer(28), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(103, 5, 12));    
    }   
    
	/** date + day --> count=-28, inteval=day, result should be 2003-4-17 */
	public void testInvokeTimestampAddDate1a() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_DAY,  //$NON-NLS-1$
			new Integer(-28), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(103, 3, 17));	
	}	
	
	/** date + month --> count=18, inteval=month, result should be 2004-11-15 */
	public void testInvokeTimestampAddDate2() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_MONTH,  //$NON-NLS-1$
			new Integer(18), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(104, 10, 15));	
	}

	/** date + month --> count=-18, inteval=month, result should be 2001-11-15 */
	public void testInvokeTimestampAddDate2a() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_MONTH,  //$NON-NLS-1$
			new Integer(-18), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(101, 10, 15));	
	}
	
	/** date + week --> count=6, inteval=week, result should be 2003-04-03 */
	public void testInvokeTimestampAddDate3() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_WEEK,  //$NON-NLS-1$
			new Integer(-6), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(103, 3, 3));	
	}

	/** date + quarter --> count=3, inteval=quarter, result should be 2004-2-15 */
	public void testInvokeTimestampAddDate4() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_QUARTER,  //$NON-NLS-1$
			new Integer(3), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(104, 1, 15));	
	}

	/** date + year --> count=-1, inteval=year, result should be 2002-5-15 */
	public void testInvokeTimestampAddDate5() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_YEAR,  //$NON-NLS-1$
			new Integer(-1), tsUtil.createDate(103, 4, 15)}, tsUtil.createDate(102, 4, 15));	
	}
			
	/** time + minute --> count=23, inteval=3, result should be 03:32:12 */
	public void testInvokeTimestampAddTime1() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_MINUTE,  //$NON-NLS-1$
			new Integer(23), tsUtil.createTime(3, 9, 12)}, tsUtil.createTime(3, 32, 12));	
	}

	/** time + hour --> count=21, inteval=4, result should be 00:09:12 and overflow */
	public void testInvokeTimestampAddTime2() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_HOUR,  //$NON-NLS-1$
			new Integer(21), tsUtil.createTime(3, 9, 12)}, tsUtil.createTime(0, 9, 12));	
	}

	/** time + hour --> count=2, inteval=4, result should be 01:12:12*/
	public void testInvokeTimestampAddTime3() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_HOUR,  //$NON-NLS-1$
			new Integer(2), tsUtil.createTime(23, 12, 12)}, tsUtil.createTime(1, 12, 12));	
	}
	
	/** time + second --> count=23, inteval=2, result should be 03:10:01 */
	public void testInvokeTimestampAddTime4() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_SECOND,  //$NON-NLS-1$
			new Integer(49), tsUtil.createTime(3, 9, 12)}, tsUtil.createTime(3, 10, 1));	
	}

	/** timestamp + second --> count=23, inteval=2, result should be 2003-05-15 03:09:35.100  */
	public void testInvokeTimestampAddTimestamp1() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_SECOND,  //$NON-NLS-1$
			new Integer(23), tsUtil.createTimestamp(103, 4, 15, 3, 9, 12, 100)}, 
			tsUtil.createTimestamp(103, 4, 15, 3, 9, 35, 100));	
	}

	/** timestamp + nanos --> count=1, inteval=1, result should be 2003-05-15 03:09:12.000000101  */
	public void testInvokeTimestampAddTimestamp2() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
			new Integer(1), tsUtil.createTimestamp(103, 4, 15, 3, 9, 12, 100)}, 
			tsUtil.createTimestamp(103, 4, 15, 3, 9, 12, 101));	
	}

	/** timestamp + nanos --> count=2100000000, inteval=1, result should be 2003-05-15 03:10:01.100000003
	 *  with increase in second and minutes, because second already at 59 sec originally
	 */
	public void testInvokeTimestampAddTimestamp3() {
		helpInvokeMethod("timestampAdd", new Object[] {ReservedWords.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
			new Integer(2100000000), tsUtil.createTimestamp(103, 4, 15, 3, 9, 59, 1)}, 
			tsUtil.createTimestamp(103, 4, 15, 3, 10, 1, 100000003));	
	}
			
	/** time --> interval=hour, time1 = 03:04:45, time2= 05:05:36 return = 2  */
	public void testInvokeTimestampDiffTime1() {
		helpInvokeMethod("timestampDiff", new Object[] {ReservedWords.SQL_TSI_HOUR,  //$NON-NLS-1$
			tsUtil.createTime(3, 4, 45), tsUtil.createTime(5, 5, 36) }, 
			new Long(2));	
	}
	
    public void testInvokeTimestampDiffTime1_ignorecase() {
        helpInvokeMethod("timestampDiff", new Object[] {"SQL_tsi_HOUR",  //$NON-NLS-1$ //$NON-NLS-2$
            tsUtil.createTime(3, 4, 45), tsUtil.createTime(5, 5, 36) }, 
            new Long(2));   
    }
    
	/** 
	 * timestamp --> interval=week, time1 = 2002-06-21 03:09:35.100,
	 * time2= 2003-05-02 05:19:35.500 return = 45
	 */
	public void testInvokeTimestampDiffTimestamp1() {
		helpInvokeMethod("timestampDiff", new Object[] {ReservedWords.SQL_TSI_WEEK,  //$NON-NLS-1$
			tsUtil.createTimestamp(102, 5, 21, 3, 9, 35, 100), tsUtil.createTimestamp(103, 4, 2, 5, 19, 35, 500) }, 
			new Long(45));	
	}

    /** 
     * timestamp --> interval=frac_second, time1 = 2002-06-21 03:09:35.000000001,
     * time2= 2002-06-21 03:09:35.100000000 return = 999999999
     */
    public void testInvokeTimestampDiffTimestamp2() {
        helpInvokeMethod("timestampDiff", new Object[] {ReservedWords.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
            tsUtil.createTimestamp(102, 5, 21, 3, 9, 35, 1), tsUtil.createTimestamp(102, 5, 21, 3, 9, 35, 100000000) }, 
            new Long(99999999));  
    }

    /** 
     * timestamp --> interval=frac_second, time1 = 2002-06-21 03:09:35.000000002,
     * time2= 2002-06-22 03:09:35.000000001 return = 
     */
    public void testInvokeTimestampDiffTimestamp3() {
        helpInvokeMethod("timestampDiff", new Object[] {ReservedWords.SQL_TSI_FRAC_SECOND,  //$NON-NLS-1$
            tsUtil.createTimestamp(102, 5, 21, 3, 9, 35, 2), tsUtil.createTimestamp(102, 5, 22, 3, 9, 35, 1) }, 
            new Long(86399999999999L));  
    }

    public void testInvokeTimestampCreate1() {
        helpInvokeMethod("timestampCreate", new Object[] {tsUtil.createDate(103, 4, 15), //$NON-NLS-1$
                                                          tsUtil.createTime(23, 59, 59)},
                                                          tsUtil.createTimestamp(103, 4, 15, 23, 59, 59, 0));    
    }   
    
    public void testInvokeBitand() {
        helpInvokeMethod("bitand", new Object[] {new Integer(0xFFF), new Integer(0x0F0)}, new Integer(0x0F0)); //$NON-NLS-1$
    }
    public void testInvokeBitor() {
        helpInvokeMethod("bitor", new Object[] {new Integer(0xFFF), new Integer(0x0F0)}, new Integer(0xFFF)); //$NON-NLS-1$
    }
    public void testInvokeBitxor() {
        helpInvokeMethod("bitxor", new Object[] {new Integer(0xFFF), new Integer(0x0F0)}, new Integer(0xF0F)); //$NON-NLS-1$
    }
    public void testInvokeBitnot() {
        helpInvokeMethod("bitnot", new Object[] {new Integer(0xF0F)}, new Integer(0xFFFFF0F0)); //$NON-NLS-1$
    }
    
    public void testInvokeRound1() {
        helpInvokeMethod("round", new Object[] {new Integer(123), new Integer(-1)}, new Integer(120)); //$NON-NLS-1$
    }

    public void testInvokeRound2() {
        helpInvokeMethod("round", new Object[] {new Float(123.456), new Integer(2)}, new Float(123.46)); //$NON-NLS-1$
    }

    public void testInvokeRound3() {
        helpInvokeMethod("round", new Object[] {new Double(123.456), new Integer(2)}, new Double(123.46)); //$NON-NLS-1$
    }

    public void testInvokeRound4() {
        helpInvokeMethod("round", new Object[] {new BigDecimal("123.456"), new Integer(2)}, new BigDecimal("123.460")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /** defect 10941 */
    public void testInvokeConvertTime() {
        helpInvokeMethod("convert", new Object[] {"05:00:00", "time"}, tsUtil.createTime(5, 0, 0)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$                    
    }

    public void testInvokeXpath1() {
        helpInvokeMethod("xpathValue",  //$NON-NLS-1$
                         new Object[] {
                                       "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>", //$NON-NLS-1$
                                       "a/b/c"}, //$NON-NLS-1$ 
                         "test"); //$NON-NLS-1$ 
    }
    
    public void testInvokeXpathWithNill() {
        helpInvokeMethod("xpathValue",  //$NON-NLS-1$
                         new Object[] {
                                       "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><b xsi:nil=\"true\"/></a>", //$NON-NLS-1$
                                       "//*[local-name()='b' and not(@*[local-name()='nil' and string()='true'])]"}, //$NON-NLS-1$ 
                         null);
    }
    
    public void testInvokeXpathWithNill1() {
        helpInvokeMethod("xpathValue",  //$NON-NLS-1$
                         new Object[] {
                                       "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b>value</b></a>", //$NON-NLS-1$
                                       "//*[local-name()='b' and not(@*[local-name()='nil' and string()='true'])]"}, //$NON-NLS-1$ 
                         "value"); //$NON-NLS-1$
    }
    
    public void testInvokeModifyTimeZone() {
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

    public void testInvokeRand() {
        helpInvokeMethod("rand", new Object[] {new Integer(100)}, new Double(0.7220096548596434)); //$NON-NLS-1$ 
        helpInvokeMethodFail("rand", new Class[] {Integer.class}, new Object[] {new Double(100)}, new FunctionExecutionException("")); //$NON-NLS-1$ //$NON-NLS-2$
        // this does not actually fail but returns a result
        helpInvokeMethodFail("rand", new Class[] {Integer.class}, new Object[] {null}, new FunctionExecutionException("")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testInvokeUser() throws Exception {
        CommandContext c = new CommandContext();
        c.setUserName("foodude"); //$NON-NLS-1$
        helpInvokeMethod("user", new Class[] {}, new Object[] {}, c, "foodude"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testInvokeEnv() throws Exception {
        CommandContext c = new CommandContext();
        Properties props = new Properties();
        props.setProperty("env_test", "env_value"); //$NON-NLS-1$ //$NON-NLS-2$
        c.setEnvironmentProperties(props);
        helpInvokeMethod("env", new Class[] {String.class}, new Object[] {"env_test"}, c, "env_value"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$        
        helpInvokeMethod("env", new Class[] {String.class}, new Object[] {null}, c, null); //$NON-NLS-1$ 
    }
    
    public void testInvokeCommandPayload() throws Exception {
        CommandContext c = new CommandContext();        
        c.setCommandPayload("payload_too heavy"); //$NON-NLS-1$
        helpInvokeMethod("commandpayload", new Class[] {}, new Object[] {}, c, "payload_too heavy"); //$NON-NLS-1$ //$NON-NLS-2$ 
        helpInvokeMethod("commandpayload", new Class[] {String.class}, new Object[] {null}, c, null); //$NON-NLS-1$ 
        Properties props = new Properties();
        props.setProperty("payload", "payload_too heavy"); //$NON-NLS-1$ //$NON-NLS-2$
        c.setCommandPayload(props);
        helpInvokeMethod("commandpayload", new Class[] {String.class}, new Object[] {"payload"}, c, "payload_too heavy"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }    
    
    public void testNullDependent() {
        FunctionDescriptor actual = library.findFunction("concat2", new Class[] {String.class, String.class}); //$NON-NLS-1$
        assertTrue(actual.isNullDependent());
        
        actual = library.findFunction("concat", new Class[] {String.class, String.class}); //$NON-NLS-1$
        assertFalse(actual.isNullDependent());
    }
    
    public void testInvokeCeiling() {
        helpInvokeMethod("ceiling", new Object[] { new Double("3.14") }, new Double("4")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokeFloor() {
        helpInvokeMethod("floor", new Object[] { new Double("3.14") }, new Double("3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokeExp() {
        helpInvokeMethod("exp", new Object[] { new Double("0") }, new Double("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokeLog() {
        helpInvokeMethod("log", new Object[] { new Double("1") }, new Double("0")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokeLog10() {
        helpInvokeMethod("log10", new Object[] { new Double("10") }, new Double("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokePower() {
        helpInvokeMethod("power", new Object[] { new Double("10"), new Double("2") }, new Double("100")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    public void testInvokeSqrt() {
        helpInvokeMethod("sqrt", new Object[] { new Double("4")}, new Double("2")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testInvokeDayName() {
        TimestampUtil util = new TimestampUtil();
        for (int i = 0; i < FunctionMethods.dayNames.length; i++) {
            Date time = util.createDate(100, 0, i + 2);
            helpInvokeMethod("dayName", new Object[] { time }, FunctionMethods.dayNames[i]); //$NON-NLS-1$ 
        }
    }
    
    public void testInvokeDayOfMonth() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("dayOfMonth", new Object[] { time }, new Integer(1)); //$NON-NLS-1$ 
    }
    
    public void testInvokeDayOfWeek() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("dayOfWeek", new Object[] { time }, new Integer(7)); //$NON-NLS-1$ 
    }
    
    public void testInvokeDayOfYear() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 2, 1, 2, 3, 4);
        helpInvokeMethod("dayOfYear", new Object[] { time }, new Integer(2)); //$NON-NLS-1$ 
    }
    
    public void testInvokeMonth() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("month", new Object[] { time }, new Integer(1)); //$NON-NLS-1$ 
    }
    
    public void testInvokeMonthName() {
        TimestampUtil util = new TimestampUtil();
        for (int i = 0; i < FunctionMethods.monthNames.length; i++) {
            Date time = util.createDate(100, i, 1);
            helpInvokeMethod("monthName", new Object[] { time }, FunctionMethods.monthNames[i]); //$NON-NLS-1$ 
        }
    }
    
    public void testInvokeMinute() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("minute", new Object[] { time }, new Integer(2)); //$NON-NLS-1$ 
    }
    
    public void testInvokeSecond() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("second", new Object[] { time }, new Integer(3)); //$NON-NLS-1$ 
    }
    
    public void testInvokeWeek() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("week", new Object[] { time }, new Integer(1)); //$NON-NLS-1$ 
    }
    
    public void testInvokeYear() {
        TimestampUtil util = new TimestampUtil();
        Timestamp time = util.createTimestamp(100, 0, 1, 1, 2, 3, 4);
        helpInvokeMethod("year", new Object[] { time }, new Integer(2000)); //$NON-NLS-1$ 
    }
}
