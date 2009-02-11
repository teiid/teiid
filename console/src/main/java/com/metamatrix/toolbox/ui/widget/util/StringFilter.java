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

package com.metamatrix.toolbox.ui.widget.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class StringFilter {

    public static final String WILDCARD = "*";
    public static final boolean DEFAULT_IGNORE_CASE = false;
    private String filter;
    private List filterTokens;
    private boolean startsWithWildcard = false;
    private boolean endsWithWildcard = false;
    private boolean containsWildcard = true;
    private boolean ignoreCase = DEFAULT_IGNORE_CASE;

    public StringFilter(String filter) {
        this(filter,DEFAULT_IGNORE_CASE);
    }

    public StringFilter(String filter, boolean ignoreCase ) {
        this.filter = filter;
        if ( this.filter == null ) {
            this.filter = WILDCARD;
        }

        this.ignoreCase = ignoreCase;

        if ( this.filter.indexOf(WILDCARD) == -1 ) {
            this.containsWildcard = false;
        } else {
            // Tokenize the filter and save in the list ...
            StringTokenizer tokens = new StringTokenizer(filter,"*",false);
            this.filterTokens = new ArrayList(tokens.countTokens());
            while ( tokens.hasMoreTokens() ) {
                String token = tokens.nextToken();
                if ( ignoreCase ) {
                    token = token.toUpperCase();
                }
                this.filterTokens.add( token );
            }

            if ( filter.startsWith(WILDCARD) ) {
                this.startsWithWildcard = true;
            }
            if ( filter.endsWith(WILDCARD) ) {
                this.endsWithWildcard = true;
            }
        }
    }

    public String getFilter() {
        return this.filter;
    }

    public String toString() {
        return this.filter;
    }

    public boolean isWildcard() {
        return this.filter == WILDCARD;
    }

    public boolean ignoresCase() {
        return this.ignoreCase;
    }

    public boolean includes( String str ) {
        if ( this.isWildcard() ) {
            return true;
        }
        if ( str == null || str.length() == 0 ) {
            return false;
        }

        if ( this.ignoreCase ) {
            str = str.toUpperCase();
        }

        if ( this.containsWildcard ) {
            Iterator iter = this.filterTokens.iterator();
            String token = null;
            int index = 0;
            int counter = 0;
            while ( iter.hasNext() ) {
                token = iter.next().toString();
                index = str.indexOf(token,index);
                if ( index == -1 ) {
                    return false;
                }
                if ( counter == 0 && ! this.startsWithWildcard && index != 0 ) {
                    return false;
                }
                ++counter;
                index = index + token.length();
            }
            if ( ! this.endsWithWildcard && index < str.length() ) {
                return false;
            }
            return true;
        }
        return this.filter.equals(str);

    }

    // unit test
    public static void main(String[] args) {

        StringFilter filter = new StringFilter("*");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(true,filter,"A string");
        StringFilter.testFilter(true,filter,"*");
        StringFilter.testFilter(true,filter,"");
        StringFilter.testFilter(true,filter,null);

        System.out.print("\n");

        filter = new StringFilter("ABCD");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(false,filter,"A string");
        StringFilter.testFilter(true,filter,"ABCD");
        StringFilter.testFilter(false,filter,"ABC");
        StringFilter.testFilter(false,filter,"ABCDE");
        StringFilter.testFilter(false,filter,"vABCD");
        StringFilter.testFilter(false,filter,"vABC");
        StringFilter.testFilter(false,filter,"vABCDE");
        StringFilter.testFilter(false,filter,"abcd");
        StringFilter.testFilter(false,filter,"abc");
        StringFilter.testFilter(false,filter,"abcde");
        StringFilter.testFilter(false,filter,"vabcd");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(false,filter,"vabcde");
        StringFilter.testFilter(false,filter,"aBcd");
        StringFilter.testFilter(false,filter,"aBc");
        StringFilter.testFilter(false,filter,"aBcde");

        filter = new StringFilter("abcd");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(false,filter,"A string");
        StringFilter.testFilter(false,filter,"ABCD");
        StringFilter.testFilter(false,filter,"ABC");
        StringFilter.testFilter(false,filter,"ABCDE");
        StringFilter.testFilter(false,filter,"vABCD");
        StringFilter.testFilter(false,filter,"vABC");
        StringFilter.testFilter(false,filter,"vABCDE");
        StringFilter.testFilter(true,filter,"abcd");
        StringFilter.testFilter(false,filter,"abc");
        StringFilter.testFilter(false,filter,"abcde");
        StringFilter.testFilter(false,filter,"vabcd");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(false,filter,"vabcde");
        StringFilter.testFilter(false,filter,"aBcd");
        StringFilter.testFilter(false,filter,"aBc");
        StringFilter.testFilter(false,filter,"aBcde");

        filter = new StringFilter("abcd*");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(false,filter,"A string");
        StringFilter.testFilter(false,filter,"ABCD");
        StringFilter.testFilter(false,filter,"ABC");
        StringFilter.testFilter(false,filter,"ABCDE");
        StringFilter.testFilter(false,filter,"vABCD");
        StringFilter.testFilter(false,filter,"vABC");
        StringFilter.testFilter(false,filter,"vABCDE");
        StringFilter.testFilter(true,filter,"abcd");
        StringFilter.testFilter(false,filter,"abc");
        StringFilter.testFilter(true,filter,"abcde");
        StringFilter.testFilter(false,filter,"vabcd");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(false,filter,"vabcde");
        StringFilter.testFilter(false,filter,"aBcd");
        StringFilter.testFilter(false,filter,"aBc");
        StringFilter.testFilter(false,filter,"aBcde");

        filter = new StringFilter("*abcd*");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(false,filter,"A string");
        StringFilter.testFilter(false,filter,"ABCD");
        StringFilter.testFilter(false,filter,"ABC");
        StringFilter.testFilter(false,filter,"ABCDE");
        StringFilter.testFilter(false,filter,"vABCD");
        StringFilter.testFilter(false,filter,"vABC");
        StringFilter.testFilter(false,filter,"vABCDE");
        StringFilter.testFilter(true,filter,"abcd");
        StringFilter.testFilter(false,filter,"abc");
        StringFilter.testFilter(true,filter,"abcde");
        StringFilter.testFilter(true,filter,"vabcd");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(true,filter,"vabcde");
        StringFilter.testFilter(false,filter,"aBcd");
        StringFilter.testFilter(false,filter,"aBc");
        StringFilter.testFilter(false,filter,"aBcde");

        filter = new StringFilter("*abcd*efg*");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(false,filter,"A string");
        StringFilter.testFilter(false,filter,"ABCDEFG");
        StringFilter.testFilter(false,filter,"ABC");
        StringFilter.testFilter(false,filter,"ABCDEFGH");
        StringFilter.testFilter(false,filter,"vABCDEFG");
        StringFilter.testFilter(false,filter,"vABC");
        StringFilter.testFilter(false,filter,"vABCDEFG");
        StringFilter.testFilter(true,filter,"abcdxefgh");
        StringFilter.testFilter(true,filter,"abcdefgh");
        StringFilter.testFilter(false,filter,"abc");
        StringFilter.testFilter(true,filter,"abcdefghx");
        StringFilter.testFilter(true,filter,"vabcdefgh");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(true,filter,"vabcdefghx");
        StringFilter.testFilter(true,filter,"vabcdeefghx");

        filter = new StringFilter("*abcd*defg*");
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(true,filter,"abcdxdefg");
        StringFilter.testFilter(true,filter,"abcddefg");
        StringFilter.testFilter(true,filter,"abcdxdefgx");
        StringFilter.testFilter(false,filter,"abcdefgh");
        StringFilter.testFilter(true,filter,"vabcddefgx");
        StringFilter.testFilter(true,filter,"vabcdxdefgx");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(false,filter,"aBcdxdefg");

        filter = new StringFilter("*abcd*defg*",false);
        System.out.print("\nFilter = \"" + filter.toString() + "\"\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(true,filter,"abcdxdefg");
        StringFilter.testFilter(true,filter,"abcddefg");
        StringFilter.testFilter(true,filter,"abcdxdefgx");
        StringFilter.testFilter(false,filter,"abcdefgh");
        StringFilter.testFilter(true,filter,"vabcddefgx");
        StringFilter.testFilter(true,filter,"vabcdxdefgx");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(false,filter,"aBcdxdefg");


        filter = new StringFilter("*abcd*defg*",true);
        System.out.print("\nFilter = \"" + filter.toString() + "\" (ignores case)\n");
        System.out.print("Filter is wildcard = " + filter.isWildcard() + "\n" );
        StringFilter.testFilter(true,filter,"abcdxdefg");
        StringFilter.testFilter(true,filter,"abcddefg");
        StringFilter.testFilter(true,filter,"abcdxdefgx");
        StringFilter.testFilter(false,filter,"abcdefgh");
        StringFilter.testFilter(true,filter,"vabcddefgx");
        StringFilter.testFilter(true,filter,"vabcdxdefgx");
        StringFilter.testFilter(false,filter,"vabc");
        StringFilter.testFilter(true,filter,"aBcdxdefg");

    }

    private static void testFilter( boolean correctResult, StringFilter filter, String str ) {
        boolean result = filter.includes(str);
        if ( correctResult != result ) {
            System.out.print("Result for includes(\"" + str + "\") is INCORRECT (was " + result + ")\n");
        }
    }

}
