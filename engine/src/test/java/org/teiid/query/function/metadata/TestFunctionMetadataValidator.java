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

package org.teiid.query.function.metadata;

import junit.framework.TestCase;

import org.teiid.api.exception.query.FunctionMetadataException;
import org.teiid.metadata.FunctionMethod.PushDown;


public class TestFunctionMetadataValidator extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestFunctionMetadataValidator(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################
	
	public void helpTestValidateName(String name) {
	    try {
	        FunctionMetadataValidator.validateName(name);
	    } catch(FunctionMetadataException e) {
	     	fail("Got exception but did not expect it: " + e.getMessage());    //$NON-NLS-1$
	    }	    
	}
	
	public void helpTestValidateNameFail(String name) { 
	    try {
	        FunctionMetadataValidator.validateName(name);
	     	fail("Expected exception but did not get one"); //$NON-NLS-1$
	    } catch(FunctionMetadataException e) {
	    }	    
	}	

	public void helpTestValidateFunction(String className, String methodName, PushDown pushdown) {
	    try {
	        FunctionMetadataValidator.validateInvocationMethod(className, methodName, pushdown);
	    } catch(FunctionMetadataException e) {
	     	fail("Got exception but did not expect it: " + e.getMessage());    //$NON-NLS-1$
	    }	    
	}
	
	public void helpTestValidateFunctionFail(String className, String methodName, PushDown pushdown) { 
	    try {
	        FunctionMetadataValidator.validateInvocationMethod(className, methodName, pushdown);
	     	fail("Expected exception but did not get one"); //$NON-NLS-1$
	    } catch(FunctionMetadataException e) {
	    }	    
	}	
	
	// ################################## ACTUAL TESTS ################################
	
	public void testValidateName1() {
		helpTestValidateName("abc");     //$NON-NLS-1$
	}

	public void testValidateName2() {
		helpTestValidateName("a13");     //$NON-NLS-1$
	}

	public void testValidateName3() {
		helpTestValidateName("a_c");     //$NON-NLS-1$
	}

	public void testValidateName4() {
		helpTestValidateName("a");     //$NON-NLS-1$
	}
	
	public void testValidateNameFail1() {
	    helpTestValidateNameFail(null);
	}
	
	public void testValidateNameFail3() {
	    helpTestValidateNameFail("a.b"); //$NON-NLS-1$
	}
	
	public void testValidateFunction1() {
	 	helpTestValidateFunction("a", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunction2() {
	 	helpTestValidateFunction("a.b", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunction3() {
	 	helpTestValidateFunction("a.b.c", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunction4() {
	 	helpTestValidateFunction("a$1", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    public void testValidateFunction5() {
        helpTestValidateFunction(null, null, PushDown.MUST_PUSHDOWN);    
    }

	public void testValidateFunctionFail1() {
	 	helpTestValidateFunctionFail(null, null, PushDown.CAN_PUSHDOWN);   
	}

	public void testValidateFunctionFail2() {
	 	helpTestValidateFunctionFail(null, "a", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$
	}

	public void testValidateFunctionFail3() {
	 	helpTestValidateFunctionFail("a", null, PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$
	}

	public void testValidateFunctionFail4() {
	 	helpTestValidateFunctionFail("1", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunctionFail5() {
	 	helpTestValidateFunctionFail("a", "2", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunctionFail6() {
	 	helpTestValidateFunctionFail("a@(", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunctionFail7() {
	 	helpTestValidateFunctionFail("a.b.", "b", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testValidateFunctionFail8() {
	 	helpTestValidateFunctionFail("a", "b.c", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    public void testValidateFunctionFail9() {
        helpTestValidateFunctionFail("a", "b@", PushDown.CAN_PUSHDOWN);    //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testValidateFunctionFail10() {
        helpTestValidateFunctionFail(null, null, PushDown.CAN_PUSHDOWN);    
        helpTestValidateFunctionFail(null, null, PushDown.CANNOT_PUSHDOWN);    
    }
}
