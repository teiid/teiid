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

package com.metamatrix.toolbox.ui.widget.util;

import org.junit.Assert;
import org.junit.Test;

public class TestStringFilter {

    // unit test
    @Test public void testStringFilter() {

        StringFilter filter = new StringFilter("*"); //$NON-NLS-1$
        testFilter(true,filter,"A string"); //$NON-NLS-1$
        testFilter(true,filter,"*"); //$NON-NLS-1$
        testFilter(true,filter,""); //$NON-NLS-1$
        testFilter(true,filter,null);

        filter = new StringFilter("ABCD"); //$NON-NLS-1$
        testFilter(false,filter,"A string"); //$NON-NLS-1$
        testFilter(true,filter,"ABCD"); //$NON-NLS-1$
        testFilter(false,filter,"ABC"); //$NON-NLS-1$
        testFilter(false,filter,"ABCDE"); //$NON-NLS-1$
        testFilter(false,filter,"vABCD"); //$NON-NLS-1$
        testFilter(false,filter,"vABC"); //$NON-NLS-1$
        testFilter(false,filter,"vABCDE"); //$NON-NLS-1$
        testFilter(false,filter,"abcd"); //$NON-NLS-1$
        testFilter(false,filter,"abc"); //$NON-NLS-1$
        testFilter(false,filter,"abcde"); //$NON-NLS-1$
        testFilter(false,filter,"vabcd"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(false,filter,"vabcde"); //$NON-NLS-1$
        testFilter(false,filter,"aBcd"); //$NON-NLS-1$
        testFilter(false,filter,"aBc"); //$NON-NLS-1$
        testFilter(false,filter,"aBcde"); //$NON-NLS-1$

        filter = new StringFilter("abcd"); //$NON-NLS-1$
        testFilter(false,filter,"A string"); //$NON-NLS-1$
        testFilter(false,filter,"ABCD"); //$NON-NLS-1$
        testFilter(false,filter,"ABC"); //$NON-NLS-1$
        testFilter(false,filter,"ABCDE"); //$NON-NLS-1$
        testFilter(false,filter,"vABCD"); //$NON-NLS-1$
        testFilter(false,filter,"vABC"); //$NON-NLS-1$
        testFilter(false,filter,"vABCDE"); //$NON-NLS-1$
        testFilter(true,filter,"abcd"); //$NON-NLS-1$
        testFilter(false,filter,"abc"); //$NON-NLS-1$
        testFilter(false,filter,"abcde"); //$NON-NLS-1$
        testFilter(false,filter,"vabcd"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(false,filter,"vabcde"); //$NON-NLS-1$
        testFilter(false,filter,"aBcd"); //$NON-NLS-1$
        testFilter(false,filter,"aBc"); //$NON-NLS-1$
        testFilter(false,filter,"aBcde"); //$NON-NLS-1$

        filter = new StringFilter("abcd*"); //$NON-NLS-1$
        testFilter(false,filter,"A string"); //$NON-NLS-1$
        testFilter(false,filter,"ABCD"); //$NON-NLS-1$
        testFilter(false,filter,"ABC"); //$NON-NLS-1$
        testFilter(false,filter,"ABCDE"); //$NON-NLS-1$
        testFilter(false,filter,"vABCD"); //$NON-NLS-1$
        testFilter(false,filter,"vABC"); //$NON-NLS-1$
        testFilter(false,filter,"vABCDE"); //$NON-NLS-1$
        testFilter(true,filter,"abcd"); //$NON-NLS-1$
        testFilter(false,filter,"abc"); //$NON-NLS-1$
        testFilter(true,filter,"abcde"); //$NON-NLS-1$
        testFilter(false,filter,"vabcd"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(false,filter,"vabcde"); //$NON-NLS-1$
        testFilter(false,filter,"aBcd"); //$NON-NLS-1$
        testFilter(false,filter,"aBc"); //$NON-NLS-1$
        testFilter(false,filter,"aBcde"); //$NON-NLS-1$

        filter = new StringFilter("*abcd*"); //$NON-NLS-1$
        testFilter(false,filter,"A string"); //$NON-NLS-1$
        testFilter(false,filter,"ABCD"); //$NON-NLS-1$
        testFilter(false,filter,"ABC"); //$NON-NLS-1$
        testFilter(false,filter,"ABCDE"); //$NON-NLS-1$
        testFilter(false,filter,"vABCD"); //$NON-NLS-1$
        testFilter(false,filter,"vABC"); //$NON-NLS-1$
        testFilter(false,filter,"vABCDE"); //$NON-NLS-1$
        testFilter(true,filter,"abcd"); //$NON-NLS-1$
        testFilter(false,filter,"abc"); //$NON-NLS-1$
        testFilter(true,filter,"abcde"); //$NON-NLS-1$
        testFilter(true,filter,"vabcd"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(true,filter,"vabcde"); //$NON-NLS-1$
        testFilter(false,filter,"aBcd"); //$NON-NLS-1$
        testFilter(false,filter,"aBc"); //$NON-NLS-1$
        testFilter(false,filter,"aBcde"); //$NON-NLS-1$

        filter = new StringFilter("*abcd*efg*"); //$NON-NLS-1$
        testFilter(false,filter,"A string"); //$NON-NLS-1$
        testFilter(false,filter,"ABCDEFG"); //$NON-NLS-1$
        testFilter(false,filter,"ABC"); //$NON-NLS-1$
        testFilter(false,filter,"ABCDEFGH"); //$NON-NLS-1$
        testFilter(false,filter,"vABCDEFG"); //$NON-NLS-1$
        testFilter(false,filter,"vABC"); //$NON-NLS-1$
        testFilter(false,filter,"vABCDEFG"); //$NON-NLS-1$
        testFilter(true,filter,"abcdxefgh"); //$NON-NLS-1$
        testFilter(true,filter,"abcdefgh"); //$NON-NLS-1$
        testFilter(false,filter,"abc"); //$NON-NLS-1$
        testFilter(true,filter,"abcdefghx"); //$NON-NLS-1$
        testFilter(true,filter,"vabcdefgh"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(true,filter,"vabcdefghx"); //$NON-NLS-1$
        testFilter(true,filter,"vabcdeefghx"); //$NON-NLS-1$

        filter = new StringFilter("*abcd*defg*"); //$NON-NLS-1$
        testFilter(true,filter,"abcdxdefg"); //$NON-NLS-1$
        testFilter(true,filter,"abcddefg"); //$NON-NLS-1$
        testFilter(true,filter,"abcdxdefgx"); //$NON-NLS-1$
        testFilter(false,filter,"abcdefgh"); //$NON-NLS-1$
        testFilter(true,filter,"vabcddefgx"); //$NON-NLS-1$
        testFilter(true,filter,"vabcdxdefgx"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(false,filter,"aBcdxdefg"); //$NON-NLS-1$

        filter = new StringFilter("*abcd*defg*",false); //$NON-NLS-1$
        testFilter(true,filter,"abcdxdefg"); //$NON-NLS-1$
        testFilter(true,filter,"abcddefg"); //$NON-NLS-1$
        testFilter(true,filter,"abcdxdefgx"); //$NON-NLS-1$
        testFilter(false,filter,"abcdefgh"); //$NON-NLS-1$
        testFilter(true,filter,"vabcddefgx"); //$NON-NLS-1$
        testFilter(true,filter,"vabcdxdefgx"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(false,filter,"aBcdxdefg"); //$NON-NLS-1$


        filter = new StringFilter("*abcd*defg*",true); //$NON-NLS-1$
        testFilter(true,filter,"abcdxdefg"); //$NON-NLS-1$
        testFilter(true,filter,"abcddefg"); //$NON-NLS-1$
        testFilter(true,filter,"abcdxdefgx"); //$NON-NLS-1$
        testFilter(false,filter,"abcdefgh"); //$NON-NLS-1$
        testFilter(true,filter,"vabcddefgx"); //$NON-NLS-1$
        testFilter(true,filter,"vabcdxdefgx"); //$NON-NLS-1$
        testFilter(false,filter,"vabc"); //$NON-NLS-1$
        testFilter(true,filter,"aBcdxdefg"); //$NON-NLS-1$

    }

    private static void testFilter( boolean correctResult, StringFilter filter, String str ) {
        boolean result = filter.includes(str);
        Assert.assertEquals(correctResult, result);
    }
	
}
