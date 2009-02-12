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

package com.metamatrix.query.xquery.saxon;

import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.URIResolver;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.query.xquery.XQuerySQLEvaluator;


/** 
 * A URI resolver for XQuery 'doc' function. This resolver is knows
 * how to handle SQL calls from this URI, such that it can execute the 
 * SQL and return the results in the form of XML which 'doc' understands.
 */
public class DocFunctionURIResolver implements URIResolver {

    private Map virtualDocuments;
    private XQuerySQLEvaluator sqlEval;
    
    public DocFunctionURIResolver(Map virtualDocuments, XQuerySQLEvaluator sqlEval) {
        this.virtualDocuments = virtualDocuments;
        this.sqlEval = sqlEval;
    }

    public Source resolve(String href, String base) throws TransformerException {
        Source doc = null;
        
        if (this.virtualDocuments != null){
            doc = new StreamSource((String)this.virtualDocuments.get(href.toUpperCase()));
        }
        
        if(doc == null) {        
            // Attempt to parse dynamic sql string
            try {
                doc = sqlEval.executeSQL(href);
    
            } catch(QueryParserException e) {
                // ignore - fall through and try as URI
            } catch(Exception e) {
                throw new TransformerException(e.getMessage(), e);
            }
        }

        if(doc != null) {
            return doc;
        }

        //Will cause standard URI resolver to kick in
        return null;
    }

}
