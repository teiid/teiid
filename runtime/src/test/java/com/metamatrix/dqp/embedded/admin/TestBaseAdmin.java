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

package com.metamatrix.dqp.embedded.admin;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestBaseAdmin extends TestCase {

    public void testRegexStuff() {
        assertTrue("RegEx Failed", "one".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one two".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one two three ".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one9_two_Three".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one9_two*Three".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "#one9_two Three".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue("RegEx Failed", "one".matches(BaseAdmin.SINGLE_WORD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one ".matches(BaseAdmin.SINGLE_WORD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one*".matches(BaseAdmin.SINGLE_WORD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one two".matches(BaseAdmin.SINGLE_WORD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one_TWO_three".matches(BaseAdmin.SINGLE_WORD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue("RegEx Failed", "one*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one two*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one two *".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "*one two*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "#two*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one.*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one.two".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one.two*".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        
        
        assertTrue("RegEx Failed", "*".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one*".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one_TWO_three*".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "*one".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "* one".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "*.*".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one.*".matches(BaseAdmin.SINGLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$

        
        assertTrue("RegEx Failed", "*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one.*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one.two".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "*.one".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one_two *".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "one.two*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$       
        assertTrue("RegEx Failed", "one.two*.three*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$        
        assertFalse("RegEx Failed", "one.two**".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$                       
        assertTrue("RegEx Failed", "one.two.*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$        
        assertTrue("RegEx Failed", "one*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "one.".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "0.10.*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "0.10..*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "0.10..".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", ".one*".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", ".one_two".matches(BaseAdmin.WORD_AND_DOT_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue("RegEx Failed", "One".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "One.1".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "*.1".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "One.One".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "One*".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "One*.101".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "*.*".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "100.*".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", ".1".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        //assertTrue("RegEx Failed", "V0.*".matches(BaseAdmin.VDB_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue("RegEx Failed", "XML-Relational File Connector".matches(BaseAdmin.MULTIPLE_WORDS_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("RegEx Failed", "XML Connector".matches(BaseAdmin.MULTIPLE_WORDS_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("RegEx Failed", "XML&Relational Connector".matches(BaseAdmin.MULTIPLE_WORDS_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
        //assertTrue("RegEx Failed", "".matches(BaseAdmin.MULTIPLE_WORD_WILDCARD_REGEX)); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
