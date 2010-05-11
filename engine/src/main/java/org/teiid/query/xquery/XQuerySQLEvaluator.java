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

package org.teiid.query.xquery;

import javax.xml.transform.Source;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;



/** 
 * @since 4.3
 */
public interface XQuerySQLEvaluator {

    /**
     * Execute a SQL string that returns an XML result
     * @param sql SQL string, typically an EXEC against an xml service or virtual document
     * @throws QueryParserException If sql parameter is not sql  
     * @throws TeiidProcessingException If execution of the sql fails due to a bad query
     * @throws TeiidComponentException If execution of the sql fails due to an internal failure
     */ 
    Source executeSQL(String sql) throws QueryParserException, TeiidProcessingException, TeiidComponentException;
    
    /**
     * Closes any resources opened during the evaluation 
     */
    void close() throws TeiidComponentException ;

	Object getParameterValue(String key) throws ExpressionEvaluationException, BlockedException, TeiidComponentException;
}
