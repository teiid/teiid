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

package org.teiid.translator.salesforce34;

import org.teiid.translator.Translator;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@Translator(name="salesforce-34", description="A translator for Salesforce With API version 34")
public class SalesForce34ExecutionFactory extends SalesForceExecutionFactory {
    
    /*
     * Handles an api difference with {@link QueryResult}.setRecords
     */
    @Override
    public QueryResult buildQueryResult(SObject[] results) {
        QueryResult result = new QueryResult();
        result.setRecords(results);         
        result.setSize(results.length);
        result.setDone(true);
        return result;
    }
}
