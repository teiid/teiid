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

package org.teiid.test.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;

import com.metamatrix.jdbc.util.ResultSetUtil;


/** 
 * TestResultSetUtil was built in order to override the {@link #printThrowable(Throwable, PrintStream)} method
 * in order to call  out.print  instead of out.println
 * This is because the println adds a line terminator, and when the result file is in turn used for 
 * comparison it fails because of the line terminator.
 * 
 * @since
 */
public class TestResultSetUtil {

    public static final int DEFAULT_MAX_COL_WIDTH = ResultSetUtil.DEFAULT_MAX_COL_WIDTH;
    
    public static List compareThrowable(Throwable t, File expectedResultsFile, boolean printToConsole) throws IOException, SQLException  {
        BufferedReader expectedResultsReader = null;
        if (expectedResultsFile != null && expectedResultsFile.exists() && expectedResultsFile.canRead()) {
            expectedResultsReader = new BufferedReader(new FileReader(expectedResultsFile));
        }
        
        PrintStream out =  ResultSetUtil.getPrintStream(null,expectedResultsReader, printToConsole ? System.out : null);
             
         printThrowable(t, out);
        return ResultSetUtil.getUnequalLines(out);
     }
    
    
    public static void printThrowable(Throwable t, PrintStream out) {
        
      out.print(t.getClass().getName() + " : " + t.getMessage()); //$NON-NLS-1$
        	
    }
    
}
