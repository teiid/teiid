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

import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.trans.XPathException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.util.XMLFormatConstants;
import com.metamatrix.query.xquery.XQueryExpression;
import com.metamatrix.query.xquery.XQuerySQLEvaluator;

/**
 * Saxon implementation of MetaMatrix XQueryExpression
 */
public class SaxonXQueryExpression implements XQueryExpression {

    /**
     * Regex used to match the external parameter declarations and return parameter names and types (if defined).
     */
    private static final Pattern EXTERNAL_VARIABLE_PATTERN = Pattern.compile("declare\\s+variable\\s+\\$(\\S+)(?:\\s+as\\s+(\\S+))?\\s+external(?:\\s+)?;");   //$NON-NLS-1$    
    
    private String xQueryString;
    private net.sf.saxon.query.XQueryExpression xQuery;    
    private Map virtualDocuments;
    private String xmlFormat;
    private Map params; // map of param name to value
    private Map paramDeclarations;

    // Create a default error listener to use when compiling - this prevents 
    // errors from being printed to System.err.
    private static final ErrorListener ERROR_LISTENER = new ErrorListener() {
        public void warning(TransformerException arg0) throws TransformerException {
        }
        public void error(TransformerException arg0) throws TransformerException {
        }
        public void fatalError(TransformerException arg0) throws TransformerException {
        }       
    };
       
    /**
     * @see com.metamatrix.query.xquery.XQueryEngine#compileXQuery(java.lang.String)
     */
    public void compileXQuery(String xQueryString) 
    throws MetaMatrixProcessingException{
        
        this.xQueryString = xQueryString;
        
        Configuration config = new Configuration();
        config.setErrorListener(ERROR_LISTENER);
        
        StaticQueryContext context = new StaticQueryContext(config);

        try {
            this.xQuery = context.compileQuery(xQueryString);
        }catch(XPathException e) {
            throw new MetaMatrixProcessingException(e);
        }
        
        // Scrape parameter names from xquery prolog
        parseParameters();
    }
    
    /**
     *   declare variable $IN1 as xs:string external; 
     * 
     */
    private void parseParameters() {
        this.paramDeclarations = new HashMap();
        
        Matcher matcher = EXTERNAL_VARIABLE_PATTERN.matcher(xQueryString);
        while(matcher.find()) {
            String var = matcher.group(1);
            String type = matcher.group(2);
            paramDeclarations.put(var, type);
        }
    }
    
    /**
     * @see com.metamatrix.query.xquery.XQueryEngine#evaluateXQuery(com.metamatrix.query.xquery.XQueryExpression)
     */
    public SQLXML evaluateXQuery(XQuerySQLEvaluator sqlEval) 
    throws MetaMatrixProcessingException, MetaMatrixComponentException {

        Configuration config = new Configuration();
        DynamicQueryContext dynamicContext = new DynamicQueryContext(config);
        
        // Set URIResolver to handle virtual doc and sql in doc() function
        dynamicContext.setURIResolver(new DocFunctionURIResolver(this.virtualDocuments, sqlEval));
        
        // Set external parameter values (if used in a view with params)
        if(this.params != null && this.params.size() > 0) {
            Iterator paramIter = params.entrySet().iterator();
            while(paramIter.hasNext()) {
                Map.Entry entry = (Map.Entry) paramIter.next();
                String paramName = (String)entry.getKey();
                paramName = StringUtil.getLastToken(paramName, "."); //$NON-NLS-1$
                
                Expression expr = (Expression)entry.getValue();
                Object value = Evaluator.evaluate(expr);
                
                if(! paramDeclarations.containsKey(paramName)) {
                    // Look for a different case match
                    Iterator declIter = this.paramDeclarations.keySet().iterator();
                    while(declIter.hasNext()) {
                        String paramDecl = (String) declIter.next();
                        if(paramName.equalsIgnoreCase(paramDecl)) {
                            paramName = paramDecl;
                            break;
                        }
                    }
                }
                
                // Check for xml and serialize
                String type = (String) paramDeclarations.get(paramName);
                if(type != null && type.equals("node()") && value != null) { //$NON-NLS-1$                    
                    try {
                        value = ((SQLXML)value).getSource(null);
                    } catch (SQLException e) {
                        throw new MetaMatrixProcessingException(e);
                    }
                }
                
                dynamicContext.setParameter(paramName, value);                
            }
        }
        
        // Evaluate
        List rawResults = null;
        try {
            rawResults = this.xQuery.evaluate(dynamicContext);
        } catch (TransformerException e) {
            Throwable cause = e.getCause();
        	while (cause instanceof TransformerException) {
        		Throwable nestedCause = ((TransformerException)cause).getCause();
        		if (nestedCause == cause) {
        			break;
        		}
        		cause = nestedCause;
        	}
            if(cause instanceof MetaMatrixProcessingException) {
                throw (MetaMatrixProcessingException) cause;
            } else if(cause instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException) cause;
            }
            if (cause instanceof RuntimeException) {
            	throw new MetaMatrixComponentException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_xquery")); //$NON-NLS-1$
            }
        	throw new MetaMatrixProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_xquery")); //$NON-NLS-1$
        }       
        
        // Read results
        Iterator i = rawResults.iterator();
        while (i.hasNext()) {
            Object obj = i.next();
            
            // output
            if (obj instanceof NodeInfo){
                Properties props = new Properties();                
                if (XMLFormatConstants.XML_TREE_FORMAT.equals(this.xmlFormat)) {
                    props.setProperty("indent", "yes");//$NON-NLS-1$//$NON-NLS-2$
                }                
                return new SQLXMLImpl(new SaxonXMLTranslator((NodeInfo)obj, props));
            } 
        }        
        throw new MetaMatrixProcessingException(QueryPlugin.Util.getString("wrong_result_type")); //$NON-NLS-1$
    }

    /**
     * This method sets whether the documents should be returned in compact
     * format (no extraneous whitespace).  Non-compact format is more human-readable
     * (and bigger).  Additional formats may be possible in future.
     * @param xmlFormat A string giving the format in which xml results need to be returned
     */
    public void setXMLFormat(String xmlFormat) {
        this.xmlFormat = xmlFormat;
    }
    
    /** 
     * @see com.metamatrix.query.xquery.XQueryExpression#setParameters(java.util.Map)
     * @since 4.3
     */
    public void setParameters(Map params) {
        this.params = params;
    }        
}
