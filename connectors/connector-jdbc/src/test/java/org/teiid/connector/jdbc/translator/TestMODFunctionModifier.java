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

package org.teiid.connector.jdbc.translator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;

/**
 * Test <code>ModFunctionModifier</code> by invoking its methods with varying 
 * parameters to validate it performs as designed and expected. 
 */
public class TestMODFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestModFunctionModifier.
     * @param name
     */
    public TestMODFunctionModifier(String name) {
        super(name);
    }


    /**
     * Create an expression containing a MOD function using <code>args</code> 
     * and pass it to the <code>Translator</code>'s MOD function modifier and 
     * compare the resulting expression to <code>expectedStr</code>.
     * 
     * @param args An array of <code>IExpression</code>'s to use as the 
     *             arguments to the MOD() function
     * @param expectedStr A string representing the modified expression
     * @return On success, the modified expression.
     * @throws Exception
     */
    public IExpression helpTestMod(IExpression[] args, String expectedStr) throws Exception {
    	return this.helpTestMod(null, null, args, expectedStr);
    }

    /**
     * Create an expression containing a MOD function using a function name of 
     * <code>modFunctionName</code> which supports types of <code>supportedTypes</code>
     * and uses the arguments <code>args</code> and pass it to the 
     * <code>Translator</code>'s MOD function modifier and compare the resulting 
     * expression to <code>expectedStr</code>.
     *
     * @param modFunctionName the name to use for the function modifier
     * @param supportedTypes a list of types that the mod function should support
     * @param args an array of <code>IExpression</code>'s to use as the 
     *             arguments to the MOD() function
     * @param expectedStr A string representing the modified expression
     * @return On success, the modified expression.
     * @throws Exception
     */
    public IExpression helpTestMod(final String modFunctionName, final List<Class<?>> supportedTypes, IExpression[] args, String expectedStr) throws Exception {
    	IExpression param1 = null;
    	IExpression param2 = null;
    	
    	if (args.length < 2) {
    		param2 = LANG_FACTORY.createLiteral(null, Short.class);
    		if (args.length < 1) {
        		param1 = LANG_FACTORY.createLiteral(null, Short.class);
    		} else {
    			param1 = args[0];
    		}
    	} else {
    		param1 = args[0];
    		param2 = args[1];
    	}
    	
    	if ( !param1.getType().equals(param2.getType()) ) {
    		if (param2.getType().equals(BigDecimal.class)) {
    			param1.setType(param2.getType());
    		} else if (param1.getType().equals(BigDecimal.class)) {
    			param2.setType(param1.getType());
    		} else if (param2.getType().equals(BigInteger.class)) {
    			param1.setType(param2.getType());
    		} else if (param1.getType().equals(BigInteger.class)) {
    			param2.setType(param1.getType());
    		} else if (param2.getType().equals(Float.class)) {
    			param1.setType(param2.getType());
    		} else if (param1.getType().equals(Float.class)) {
    			param2.setType(param1.getType());
    		} else if (param2.getType().equals(Long.class)) {
    			param1.setType(param2.getType());
    		} else if (param1.getType().equals(Long.class)) {
    			param2.setType(param1.getType());
    		} else if (param2.getType().equals(Integer.class)) {
    			param1.setType(param2.getType());
    		} else if (param1.getType().equals(Integer.class)) {
    			param2.setType(param1.getType());
    		} else if (param2.getType().equals(Short.class)) {
    			param1.setType(param2.getType());
    		} else if (param1.getType().equals(Short.class)) {
    			param2.setType(param1.getType());
    		} else {
    			throw new IllegalArgumentException("Parameters must be of numeric types"); //$NON-NLS-1$
    		}
    	}

    	IFunction func = LANG_FACTORY.createFunction(modFunctionName,
            Arrays.asList(param1, param2), param1.getType());

    	Translator trans = new Translator() {
			@Override
			public void initialize(ConnectorEnvironment env)
					throws ConnectorException {
				super.initialize(env);
				if (modFunctionName == null) {
					registerFunctionModifier(SourceSystemFunctions.MOD, new MODFunctionModifier(getLanguageFactory()));
				} else {
					registerFunctionModifier(SourceSystemFunctions.MOD, new MODFunctionModifier(getLanguageFactory(), modFunctionName, supportedTypes));
				}
			}
    	};
    	
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));

        IExpression expr = trans.getFunctionModifiers().get(SourceSystemFunctions.MOD).modify(func);
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        
        assertEquals("Modified function does not match", expectedStr, sqlVisitor.toString()); //$NON-NLS-1$
        
        return expr;
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Short} constants for both parameters returns MOD(x,y). 
     * {@link MODFunctionModifier} will be constructed without specifying a 
     * function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testTwoShortConst() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // default / default
        helpTestMod(args, "MOD(10, 6)"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Short} constants for both parameters returns MOD(x,y). 
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "MOD" but without a supported type list.
     *  
     * @throws Exception
     */
    public void testTwoShortConst2() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // mod / default 
        helpTestMod("MOD", null, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Short} constants for both parameters returns MOD(x,y). 
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "MOD" and a supported type list which contains {@link Short}. 
     * 
     * @throws Exception
     */
    public void testTwoShortConst3() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Short} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * does not contains {@link Short}. 
     * 
     * @throws Exception
     */
    public void testTwoShortConst4() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // mod / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Integer.class);
        helpTestMod("MOD", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Short} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testTwoShortConst5() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // % / default 
        helpTestMod("%", null, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Short} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and a supported type list which contains {@link Short}. 
     * 
     * @throws Exception
     */
    public void testTwoShortConst6() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Short} constants for both parameters returns 
     * (10 - (TRUNC((10 / 6), 0) * 6)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list does not 
     * contain {@link Short}. 
     * 
     * @throws Exception
     */
    public void testTwoShortConst7() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Short((short) 10), Short.class),
            LANG_FACTORY.createLiteral(new Short((short) 6), Short.class)           
        };
        // % / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Integer.class);
        helpTestMod("%", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Integer} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed without 
     * specifying a function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testTwoIntConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // default / default
        helpTestMod(args, "MOD(10, 6)"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Integer} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" but without a supported type list.
     *  
     * @throws Exception
     */
    public void testTwoIntConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // mod / default 
        helpTestMod("MOD", null, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Integer} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" and a supported type list which contains {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testTwoIntConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Integer.class);
        helpTestMod("MOD", typeList, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Integer} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * does not contain {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testTwoIntConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // mod / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Integer} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testTwoIntConst5() throws Exception {
        IExpression[] args = new IExpression[] {
        		LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // % / default 
        helpTestMod("%", null, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Integer} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and a supported type list which contains {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testTwoIntConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Integer.class);
        helpTestMod("%", typeList, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Integer} constants for both parameters returns 
     * (10 - (TRUNC((10 / 6), 0) * 6)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list that 
     * does not contain {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testTwoIntConst7() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral(new Integer(10), Integer.class),
            LANG_FACTORY.createLiteral(new Integer(6), Integer.class)           
        };
        // % / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Long} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed without 
     * specifying a function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testTwoLongConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // default / default
        helpTestMod(args, "MOD(10, 6)"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Long} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" but without a supported type list.
     *  
     * @throws Exception
     */
    public void testTwoLongConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // mod / default 
        helpTestMod("MOD", null, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Long} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" and a supported type list which contains {@link Long}. 
     * 
     * @throws Exception
     */
    public void testTwoLongConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // mod / Long
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Long.class);
        helpTestMod("MOD", typeList, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Long} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * does not contain {@link Long}. 
     * 
     * @throws Exception
     */
    public void testTwoLongConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Long} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testTwoLongConst5() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // % / default 
        helpTestMod("%", null, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Long} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and a supported type list which contains {@link Long}. 
     * 
     * @throws Exception
     */
    public void testTwoLongConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // % / Long
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Long.class);
        helpTestMod("%", typeList, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Long} constants for both parameters returns 
     * (10 - (TRUNC((10 / 6), 0) * 6)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list that 
     * does not contain {@link Long}. 
     * 
     * @throws Exception
     */
    public void testTwoLongConst7() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Long(10), Long.class),
                LANG_FACTORY.createLiteral(new Long(6), Long.class)           
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Float} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed without specifying a function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testTwoFloatConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // default / default
        helpTestMod(args, "(10.0 - (TRUNC((10.0 / 6.0), 0) * 6.0))"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Float} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" but without a supported type 
     * list.
     *  
     * @throws Exception
     */
    public void testTwoFloatConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // mod / default 
        helpTestMod("MOD", null, args, "(10.0 - (TRUNC((10.0 / 6.0), 0) * 6.0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Float} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" and a supported type list which contains {@link Float}. 
     * 
     * @throws Exception
     */
    public void testTwoFloatConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // mod / Float
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Float.class);
        helpTestMod("MOD", typeList, args, "MOD(10.0, 6.0)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link Float} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * does not contain {@link Float}. 
     * 
     * @throws Exception
     */
    public void testTwoFloatConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(10.0 - (TRUNC((10.0 / 6.0), 0) * 6.0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Float} constants for both parameters returns  
     * (x - (TRUNC((x / y), 0) * x)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testTwoFloatConst5() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // % / default 
        helpTestMod("%", null, args, "(10.0 - (TRUNC((10.0 / 6.0), 0) * 6.0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Float} constants for both parameters returns (x % y).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and a supported type list which contains {@link Float}. 
     * 
     * @throws Exception
     */
    public void testTwoFloatConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // % / Float
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Float.class);
        helpTestMod("%", typeList, args, "(10.0 % 6.0)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link Float} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * x)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list that 
     * does not contain {@link Float}. 
     * 
     * @throws Exception
     */
    public void testTwoFloatConst7() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(10), Float.class),
                LANG_FACTORY.createLiteral(new Float(6), Float.class)           
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(10.0 - (TRUNC((10.0 / 6.0), 0) * 6.0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigInteger} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed without specifying a function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testTwoBigIntConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // default / default
        helpTestMod(args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigInteger} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" but without a supported type 
     * list.
     *  
     * @throws Exception
     */
    public void testTwoBigIntConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // mod / default 
        helpTestMod("MOD", null, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigInteger} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" and a supported type list which contains {@link BigInteger}. 
     * 
     * @throws Exception
     */
    public void testTwoBigIntConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // mod / BigInteger
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(BigInteger.class);
        helpTestMod("MOD", typeList, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigInteger} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * does not contain {@link BigInteger}. 
     * 
     * @throws Exception
     */
    public void testTwoBigIntConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link BigInteger} constants for both parameters returns  
     * (x - (TRUNC((x / y), 0) * x)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testTwoBigIntConst5() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // % / default 
        helpTestMod("%", null, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link BigInteger} constants for both parameters returns 
     * (x % y).  {@link MODFunctionModifier} will be constructed with a function 
     * name of "%" and a supported type list which contains {@link BigInteger}. 
     * 
     * @throws Exception
     */
    public void testTwoBigIntConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // % / BigInteger
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(BigInteger.class);
        helpTestMod("%", typeList, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link BigInteger} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * x)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list that 
     * does not contain {@link BigInteger}. 
     * 
     * @throws Exception
     */
    public void testTwoBigIntConst7() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigInteger("10"), BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigInteger("6"), BigInteger.class) //$NON-NLS-1$
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigDecimal} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed without specifying a function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testTwoBigDecConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // default / default
        helpTestMod(args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigDecimal} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" but without a supported type 
     * list.
     *  
     * @throws Exception
     */
    public void testTwoBigDecConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // mod / default 
        helpTestMod("MOD", null, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigDecimal} constants for both parameters returns 
     * MOD(x,y).  {@link MODFunctionModifier} will be constructed with a 
     * function name of "MOD" and a supported type list which contains {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testTwoBigDecConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // mod / BigDecimal
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(BigDecimal.class);
        helpTestMod("MOD", typeList, args, "MOD(10, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,y) using {@link BigDecimal} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * y)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * does not contain {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testTwoBigDecConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link BigDecimal} constants for both parameters returns  
     * (x - (TRUNC((x / y), 0) * x)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testTwoBigDecConst5() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // % / default 
        helpTestMod("%", null, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link BigDecimal} constants for both parameters returns 
     * (x % y).  {@link MODFunctionModifier} will be constructed with a function 
     * name of "%" and a supported type list which contains {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testTwoBigDecConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // % / BigDecimal
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(BigDecimal.class);
        helpTestMod("%", typeList, args, "(10 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * x % y using {@link BigDecimal} constants for both parameters returns 
     * (x - (TRUNC((x / y), 0) * x)).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list that 
     * does not contain {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testTwoBigDecConst7() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new BigDecimal("10"), BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal("6"), BigDecimal.class) //$NON-NLS-1$
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(10 - (TRUNC((10 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link Integer} element and a {@link Integer} constant 
     * for parameters returns MOD(e1,y).  {@link MODFunctionModifier} will be 
     * constructed without specifying a function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testOneIntElemOneIntConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // default / default
        helpTestMod(args, "MOD(e1, 6)"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link Integer} element and a {@link Integer} constant 
     * for parameters returns MOD(e1,y).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" but without a supported type 
     * list.
     *  
     * @throws Exception
     */
    public void testOneIntElemOneIntConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // mod / default 
        helpTestMod("MOD", null, args, "MOD(e1, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link Integer} element and a {@link Integer} constant 
     * for parameters returns MOD(e1,y).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "MOD" and a supported type list which 
     * contains {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testOneIntElemOneIntConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // mod / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Integer.class);
        helpTestMod("MOD", typeList, args, "MOD(e1, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link Integer} element and a {@link Integer} constant 
     * for parameters returns (e1 - (TRUNC((e1 / y), 0) * y)).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "MOD" and a supported type list which does not contain {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testOneIntElemOneIntConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * e1 % y using a {@link Integer} element and a {@link Integer} constant for 
     * parameters returns (e1 % y).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and no supported type list. 
     * 
     * @throws Exception
     */
    public void testOneIntElemOneIntConst5() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // % / default 
        helpTestMod("%", null, args, "(e1 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * e1 % y using a {@link Integer} element and a {@link Integer} constant for 
     * parameters returns (e1 % y).  {@link MODFunctionModifier} will be 
     * constructed with a function name of "%" and a supported type list which 
     * contains {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testOneIntElemOneIntConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // % / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Integer.class);
        helpTestMod("%", typeList, args, "(e1 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * e1 % y using a {@link Integer} element and a {@link Integer} constant for 
     * parameters returns (e1 - (TRUNC((e1 / y), 0) * y)).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and a supported type list that does not contain {@link Integer}. 
     * 
     * @throws Exception
     */
    public void testOneIntElemOneIntConst7() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new Integer(6), Integer.class)
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns (e1 - (TRUNC((e1 / y), 0) * y)).  
     * {@link MODFunctionModifier} will be constructed without specifying a 
     * function name or a supported type list.
     * 
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // default / default
        helpTestMod(args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns (e1 - (TRUNC((e1 / y), 0) * y)).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "MOD" but without a supported type list.
     *  
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst2() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // mod / default 
        helpTestMod("MOD", null, args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns MOD(e1,y).  {@link MODFunctionModifier} 
     * will be constructed with a function name of "MOD" and a supported type 
     * list which contains {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst3() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // mod / Integer
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(BigDecimal.class);
        helpTestMod("MOD", typeList, args, "MOD(e1, 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(e1,y) using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns (e1 - (TRUNC((e1 / y), 0) * y)).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "MOD" and a supported type list which does not contain {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst4() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // mod / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("MOD", typeList, args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * e1 % y using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns (e1 % y).  {@link MODFunctionModifier} 
     * will be constructed with a function name of "%" and no supported type 
     * list. 
     * 
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst5() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // % / default 
        helpTestMod("%", null, args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * e1 % y using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns (e1 % y).  {@link MODFunctionModifier} 
     * will be constructed with a function name of "%" and a supported type list 
     * which contains {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst6() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // % / BigDecimal
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(BigDecimal.class);
        helpTestMod("%", typeList, args, "(e1 % 6)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * e1 % y using a {@link BigDecimal} element and a {@link BigDecimal} 
     * constant for parameters returns (e1 - (TRUNC((e1 / y), 0) * y)).  
     * {@link MODFunctionModifier} will be constructed with a function name of 
     * "%" and a supported type list that does not contain {@link BigDecimal}. 
     * 
     * @throws Exception
     */
    public void testOneBigDecElemOneBigDecConst7() throws Exception {
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createElement("e1", null, null, BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral(new BigDecimal(6), BigDecimal.class)
        };
        // % / Short
        List<Class<?>> typeList = new ArrayList<Class<?>>(1);
        typeList.add(Short.class);
        helpTestMod("%", typeList, args, "(e1 - (TRUNC((e1 / 6), 0) * 6))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test {@link MODFunctionModifier#modify(IFunction)} to validate a call to 
     * MOD(x,e1) using a {@link Float} literal and a {@link Float} element for 
     * parameters returns (x - (floor((x / e1)) * e1)).  {@link MODFunctionModifier} 
     * will be constructed with a function name of "MOD" and no supported type 
     * list.  The test explicitly overrides 
     * {@link MODFunctionModifier#getQuotientExpression(IExpression)} to produce 
     * output that uses the floor(z) function. 
     * 
     * @throws Exception
     */
    public void testOverrideGetQuotient() throws Exception {
    	final Class<?> dataType = Float.class;
    	final String modFunctionName = "MOD"; //$NON-NLS-1$
    	final String expectedStr = "(1000.23 - (floor((1000.23 / e1)) * e1))"; //$NON-NLS-1$
    	
        IExpression[] args = new IExpression[] {
                LANG_FACTORY.createLiteral(new Float(1000.23), dataType),
                LANG_FACTORY.createElement("e1", null, null, dataType), //$NON-NLS-1$
        };
    	IFunction func = LANG_FACTORY.createFunction(modFunctionName, args, dataType);

    	final Translator trans = new Translator() {
			@Override
			public void initialize(ConnectorEnvironment env)
					throws ConnectorException {
				super.initialize(env);
				registerFunctionModifier(SourceSystemFunctions.MOD, new MODFunctionModifier(getLanguageFactory(), modFunctionName, null) {
					@Override
					protected IExpression getQuotientExpression(
							IExpression division) {
						return getLanguageFactory().createFunction("floor", Arrays.asList(division), division.getType()); //$NON-NLS-1$
					}
					
				});
			}
    	};

        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));

        IExpression expr = trans.getFunctionModifiers().get(SourceSystemFunctions.MOD).modify(func);
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        assertEquals("Modified function does not match", expectedStr, sqlVisitor.toString()); //$NON-NLS-1$
    }
}
