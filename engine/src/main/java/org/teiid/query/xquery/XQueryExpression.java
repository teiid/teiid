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

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.XMLTranslator;

/**
 * An XQueryExpression - the object representation of
 * a compiled XQuery.  Must be thread-safe and immutable.
 */
public interface XQueryExpression {
    
    /**
     * Return the compiled XQueryExpression - the result of this
     * call should be used as a parameter to the 
     * {@link #getDocumentNames getDocumentNames} and
     * {@link #evaluateXQuery evaluateXQuery} methods.
     * A null return value is interpreted to mean that XQueries
     * are not supported by this engine at all.
     * @param xQueryString the original XQuery String
     * @throws TeiidProcessingException if xQueryString is
     * invalid and fails to compile
     */
    public void compileXQuery(String xQueryString)
    throws TeiidProcessingException;

    
    /**
     * Evaluate the XQuery and return results.  A null return
     * value is interpreted to mean that XQueries are not supported
     * by this engine.
     * @param compiledXQuery compiled XQueryExpression
     * @throws TeiidProcessingException if xQueryString is
     * invalid and fails to compile
     */
    public XMLTranslator evaluateXQuery(XQuerySQLEvaluator sqlEval)
    throws TeiidProcessingException, TeiidComponentException;
    
    
    /**
     * This method sets whether the documents should be returned in compact
     * format (no extraneous whitespace).  Non-compact format is more human-readable
     * (and bigger).  Additional formats may be possible in future.
     * @param xmlFormat A string giving the format in which xml results need to be returned
     */
    public void setXMLFormat(String xmlFormat);  
    
}
