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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;

import junit.framework.Test;
import junit.framework.TestCase;
import bsh.BshMethod;
import bsh.Interpreter;
import bsh.TargetError;

/**
 * BshTestCase class along with BshTestSuite ties the JUnit and BeanShell 
 * toghether. Using the combination of these, user can write test scripts 
 * using the BeanShell scripting language, and integrate,run and report
 * results using JUnit framework.
 * <p>usage:
 * For example if we have a BeanShell script called "JUnitTest.bsh", that
 * can be executed by using as follows.
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
 * Currently the path to the script must be relative to the working directory
 * or fully qualified.
 */
public class BshTestCase extends TestCase {
	String script = null;
	BshMethod method = null;
	Interpreter interpreter = null;
    String scriptFileName = null;
	
	public BshTestCase(String scriptName, BshMethod method, Interpreter interpreter){
		super(method.getName());
        this.scriptFileName = scriptName;
		this.method = method;
		this.interpreter = interpreter;
	}		
			
	public static Test suite() {
		BshTestSuite suite = new BshTestSuite();
		//suite.addTest("JUnitTest.bsh");
		return suite;
	}
	

	/** 
	 * @see junit.framework.TestCase#runTest()
	 */
	protected void runTest() throws Throwable {
		// run setup if one available
		BshMethod setup = interpreter.getNameSpace().getMethod("setUp", new Class [] {}); //$NON-NLS-1$
		if (setup != null){
			setup.invoke(null, interpreter);
		}

		try{
            System.out.println("Running test="+method.getName()); //$NON-NLS-1$
			// run the actual method
			method.invoke(null, interpreter);		
		}catch(TargetError e) {            
            printStackTrace(e, System.err);            
            throw removeVeboseStackTrace(e.getTarget());
        } finally{
			// run teardown
			BshMethod teardown = interpreter.getNameSpace().getMethod("tearDown", new Class [] {}); //$NON-NLS-1$
			if (teardown != null){
				teardown.invoke(null, interpreter);
			}
		}		
	}
    
    void printStackTrace(TargetError te, PrintStream out) {
        Throwable t = te.getTarget();
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        
        String line = null;
        BufferedReader r = new BufferedReader(new StringReader(sw.toString()));
        try {
            while ((line=r.readLine()) != null) {
                if (    !line.startsWith("\tat bsh.")                   //$NON-NLS-1$
                     && !line.startsWith("\tat sun.reflect.")           //$NON-NLS-1$
                     && !line.startsWith("\tat junit.")                 //$NON-NLS-1$
                     && !line.startsWith("\tat org.eclipse.jdt")) {     //$NON-NLS-1$
                    out.println(line);
                }
            }
        } catch (IOException e) {
            // ignore
        }       
        out.println("\n\tbsh.callstack");//$NON-NLS-1$
        out.println("\t"+te.getErrorText()+" at Line: "+te.getErrorLineNumber()+" : in file: "+te.getErrorSourceFile()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        out.println(te.getScriptStackTrace());
        out.println("\twhile running \""+this.method.getName()+"()\" method in script file \""+scriptFileName+"\""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }    

    /**
     * Remove the verbose bsh stack trace and sun.refelect before returning. 
     * @param t
     * @param out
     * @return
     */
    Throwable removeVeboseStackTrace(Throwable t) {

        // First walk up to deepest level
        if (t.getCause() != null) {        	
            return new Exception(t.getMessage(), removeVeboseStackTrace(t.getCause()));
        }
        
        StackTraceElement[] traces = t.getStackTrace();
        ArrayList newtrace = new ArrayList();
        
        for (int i = 0; i < traces.length; i++) {
            StackTraceElement trace = traces[i];
            String clsName = trace.getClassName();
            if (!clsName.startsWith("bsh.")                   //$NON-NLS-1$
                && !clsName.startsWith("sun.reflect.")           //$NON-NLS-1$
                && !clsName.startsWith("junit.")                 //$NON-NLS-1$
                && !clsName.startsWith("org.eclipse.jdt")) {     //$NON-NLS-1$
                newtrace.add(trace);
            }
        }
        
        Exception e = new Exception(t.getMessage());
        e.setStackTrace((StackTraceElement[]) newtrace.toArray(new StackTraceElement[newtrace.size()]));
        return e;
    }
}
