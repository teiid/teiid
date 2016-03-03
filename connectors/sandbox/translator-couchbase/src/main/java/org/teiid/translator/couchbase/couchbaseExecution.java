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

package org.teiid.translator.couchbase;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.teiid.language.ColumnReference;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


/**
 * Represents the execution of a command.
 */
public class couchbaseExecution implements ResultSetExecution {


    private Select command;
    
    // Execution state
    Iterator<List<?>> results;
    int[] neededColumns;
    private Select query;

    /**
     * 
     */
    public couchbaseExecution(Select query) {
        this.query = query;
    }
    
    @Override
    public void execute() throws TranslatorException {
        // Log our command
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, couchbasePlugin.UTIL.getString("execute_query", new Object[] { "couchbase", command })); //$NON-NLS-1$
    }    


    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (results.hasNext()) {
            return projectRow(results.next(), neededColumns);
        }
        return null;
    }

    /**
     * @param row
     * @param neededColumns
     */
    static List<Object> projectRow(List<?> row, int[] neededColumns) {
        List<Object> output = new ArrayList<Object>(neededColumns.length);
        
        for(int i=0; i<neededColumns.length; i++) {
            output.add(row.get(neededColumns[i]-1));
        }
        
        return output;    
    }

    @Override
    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, couchbasePlugin.UTIL.getString("close_query")); //$NON-NLS-1$

    
    }

    @Override
    public void cancel() throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, couchbasePlugin.UTIL.getString("cancel_query")); //$NON-NLS-1$
    }
}
