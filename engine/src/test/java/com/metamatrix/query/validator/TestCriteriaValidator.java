/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.validator;

import junit.framework.TestCase;

public class TestCriteriaValidator extends TestCase {

	public TestCriteriaValidator(String name) { 
		super(name);
	}	
    
    public void testClobEquals() {
        TestValidator.helpValidate("SELECT * FROM test.group where e4 = '1'", new String[] {"e4", "'1'"}, TestValidator.exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    /**
     *  Should not fail since the update changing set is not really criteria
     */
    public void testUpdateWithClob() {
        TestValidator.helpValidate("update test.group set e4 = ?", new String[] {}, TestValidator.exampleMetadata2()); //$NON-NLS-1$ 
    }

    public void testBlobLessThan() {
        TestValidator.helpValidate("SELECT * FROM test.group where e3 < ?", new String[] {"e3", "?"}, TestValidator.exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
}
