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

package com.metamatrix.script.junit;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;

import junit.framework.TestSuite;
import bsh.BshMethod;
import bsh.Interpreter;

/**
 * TestSuite class to be used with the BeanShell unit tests. This class
 * given the script name, aggregates all the test methods in the script
 * and add them to the current suite as test cases. This class needs to 
 * be used in conjunction with the BshTestCase.
 *
 * <pre>
 * public MyTest extends TestCase{
 * 
 *     public static Test suite() {
 *		   BshTestSuite suite = new BshTestSuite();
 *		   suite.addTest("JUnitTest.bsh");
 *		   suite.addTest("Math.bsh");
 *		   return suite;
 *     }
 * }
 * </pre>
 * During debugging, when you want to execute a single test method, or group of
 * test methods, you can select a predefined tests by two ways.
 * <p>One: In the ".bsh" script add a variable called "runOnly=*" like, and supply a 
 * regular expression which matches to the test name, or test name itself.
 * <pre>
 *  runOnly = ".*One"; // Regular Expression
 *  testMethodOne(){
 *      //test case 1
 *  }
 *  testMethodTwo(){
 *      // test case 2
 *  }
 * </pre>
 * 
 * <p>Two: There is a overridden "addTest" method on the BshTestSuite, which will take
 * a regEx or method name.
 * <pre>
 * public MyTest extends TestCase{ 
 *     public static Test suite() {
 *         BshTestSuite suite = new BshTestSuite();
 *         suite.addTest("JUnitTest.bsh", ".*One");
 *         suite.addTest("Math.bsh");
 *         return suite;
 *     }
 * }
 * </pre> 
 * 
 * Currently the path to the script must be relative to the working directory
 * or fully qualified.
 *
 * @see BshTestCase
 */
public class BshTestSuite extends TestSuite {
	
    public BshTestSuite() {
        
    }
    
    public BshTestSuite(String name) {
        super(name);
    }
    
	/** 
     * Loads the specified script from the classpath.
     * @param bshScript Name of the test beanshell file in the classpath.
	 * @see junit.framework.TestSuite#addTest(junit.framework.Test)
	 */
	public void addTest(String bshScript) {
		        
		try {
            BshTestSuite thisScriptSuite = new BshTestSuite(bshScript);
			Interpreter interpreter = new Interpreter();
			interpreter.eval("importCommands(\"commands\")"); //$NON-NLS-1$
            evaluate(interpreter, bshScript);
            
            
            String runOnly = (String)interpreter.get("runOnly"); //$NON-NLS-1$
            BshMethod[] methods = interpreter.getNameSpace().getMethods();
            if (runOnly == null || runOnly.length() == 0) {
                methods = getMatchingTests("^test.*", methods); //$NON-NLS-1$
            }
            else {
                methods = getMatchingTests(runOnly, methods);
            }
            
            // Now add the tests.
            for (int i = 0; i < methods.length; i++){
                thisScriptSuite.addTest(new BshTestCase(bshScript, methods[i], interpreter));
            }                  
            
            // add the suite
            super.addTest(thisScriptSuite);
            
		} catch (Exception e) {
			throw new RuntimeException("failed to add tests from bean shell script "+bshScript, e); //$NON-NLS-1$
		} 		
	}

    /**
     * A way add a single test while debugging. 
     * @param bshScript
     * @param methodName
     * @since 4.3
     */
    public void addTest(String bshScript, String methodNamePattern) {
        
        try {
            Interpreter interpreter = new Interpreter();
            evaluate(interpreter, bshScript);
            
            BshMethod[] methods = interpreter.getNameSpace().getMethods();
            methods = getMatchingTests(methodNamePattern, methods);
            for (int i = 0; i < methods.length; i++){                
                super.addTest(new BshTestCase(bshScript, methods[i], interpreter));                
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to add tests from bean shell script "+bshScript, e); //$NON-NLS-1$
        }       
    }
    
    
    private void evaluate(Interpreter interpreter, String bshScript) throws Exception {
        //try to load file from the classpath
        ClassLoader loader = getClass().getClassLoader();
        URL url = loader.getResource(bshScript);
        
        if (url == null) {
            //load from "hardcoded" filename
            interpreter.source(bshScript);
        } else {
            //load from classpath
            
            String filename = url.getFile();
            
            InputStream is = loader.getResourceAsStream(bshScript);
            InputStreamReader isr = new InputStreamReader(is);
            
            interpreter.eval(isr, interpreter.getNameSpace(), filename);
            
            isr.close();
            is.close();
        }
    }
    
    
        
    /**
     * Filter out the matching methods to execute. 
     * @param pattern
     * @param methods
     * @return
     * @since 4.3
     */
    BshMethod[] getMatchingTests(String pattern, BshMethod[] methods ) {
        ArrayList matchedMethods = new ArrayList();
        for (int i = 0; i < methods.length; i++) {
            String[] parameters = methods[i].getParameterNames();
            if (methods[i].getName().matches(pattern) && parameters.length == 0) {
                matchedMethods.add(methods[i]);
            }
        }        
        return (BshMethod[])matchedMethods.toArray(new BshMethod[matchedMethods.size()]);
    }
    
}
