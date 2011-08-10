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

package org.teiid.query.xquery.saxon;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLXML;
import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;

import net.sf.saxon.AugmentedSource;
import net.sf.saxon.Configuration;
import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.trans.XPathException;
import nu.xom.Builder;
import nu.xom.DocType;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import nux.xom.xquery.StreamingPathFilter;
import nux.xom.xquery.StreamingTransform;

import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.util.CommandContext;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.RowProcessor;

/**
 * Used to isolate the xom/nux dependency and to better isolate the saxon processing logic.
 */
public class XQueryEvaluator {
	
	private static Nodes NONE = new Nodes(); 
	private static InputStream FAKE_IS = new InputStream() {

		@Override
		public int read() throws IOException {
			return 0;
		}
	};

	public static SaxonXQueryExpression.Result evaluateXQuery(final SaxonXQueryExpression xquery, Object context, Map<String, Object> parameterValues, final RowProcessor processor, CommandContext commandContext) throws TeiidProcessingException {
	    DynamicQueryContext dynamicContext = new DynamicQueryContext(xquery.config);
	
	    SaxonXQueryExpression.Result result = new SaxonXQueryExpression.Result();
	    try {
	        try {
		        for (Map.Entry<String, Object> entry : parameterValues.entrySet()) {
		            Object value = entry.getValue();
		            if(value instanceof SQLXML) {                    
		            	value = XMLSystemFunctions.convertToSource(value);
		            	result.sources.add((Source)value);
		            } else if (value instanceof java.util.Date) {
		            	value = XMLSystemFunctions.convertToAtomicValue(value);
		            }
		            dynamicContext.setParameter(entry.getKey(), value);                
		        }
	        } catch (TransformerException e) {
	        	throw new TeiidProcessingException(e);
	        }
	        if (context != null) {
	        	Source source = XMLSystemFunctions.convertToSource(context);
	        	result.sources.add(source);
	            if (xquery.contextRoot != null) {
	            	//create our own filter as this logic is not provided in the free saxon
	                ProxyReceiver filter = new PathMapFilter(xquery.contextRoot);
	                AugmentedSource sourceInput = AugmentedSource.makeAugmentedSource(source);
	                sourceInput.addFilter(filter);
	                source = sourceInput;
	
	            	//use streamable processing instead
	                if (xquery.streamingPath != null && processor != null) {
	                	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
	                		LogManager.logDetail(LogConstants.CTX_DQP, "Using stream processing for evaluation of", xquery.xQueryString); //$NON-NLS-1$
	                	}
	                	//set to non-blocking in case default expression evaluation blocks
	                	boolean isNonBlocking = commandContext.isNonBlocking();
						commandContext.setNonBlocking(true);
						
						final StreamingTransform myTransform = new StreamingTransform() {
							public Nodes transform(Element elem) {
								processor.processRow(XQueryEvaluator.wrap(elem, xquery.config));
								return NONE;
							}
						};
						
						Builder builder = new Builder(new SaxonReader(xquery.config, sourceInput), false, 
								new StreamingPathFilter(xquery.streamingPath, xquery.namespaceMap).createNodeFactory(null, myTransform));
						try {
							//the builder is hard wired to parse the source, but the api will throw an exception if the stream is null
							builder.build(FAKE_IS);
							return result;
						} catch (ParsingException e) {
							throw new TeiidProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_context")); //$NON-NLS-1$
						} catch (IOException e) {
							throw new TeiidProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_context")); //$NON-NLS-1$
						} finally {
							if (!isNonBlocking) {
								commandContext.setNonBlocking(false);
							}
						}
	                }
	            }
	            DocumentInfo doc;
				try {
					doc = xquery.config.buildDocument(source);
				} catch (XPathException e) {
					throw new TeiidProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_context")); //$NON-NLS-1$
				}
		        dynamicContext.setContextItem(doc);
	        }
	        try {
	        	result.iter = xquery.xQuery.iterator(dynamicContext);
	        	return result;
	        } catch (TransformerException e) {
	        	throw new TeiidProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_xquery")); //$NON-NLS-1$
	        }       
	    } finally {
	    	if (result.iter == null) {
	    		result.close();
	    	}
	    }
	}

	/**
	 * Converts a xom node into something readable by Saxon
	 * @param node
	 * @param config
	 * @return
	 */
	static NodeInfo wrap(Node node, Configuration config) {
		if (node == null) 
			throw new IllegalArgumentException("node must not be null"); //$NON-NLS-1$
		if (node instanceof DocType)
			throw new IllegalArgumentException("DocType can't be queried by XQuery/XPath"); //$NON-NLS-1$
		
		Node root = node;
		while (root.getParent() != null) {
			root = root.getParent();
		}
	
		DocumentWrapper docWrapper = new DocumentWrapper(root, root.getBaseURI(), config);
		
		return docWrapper.wrap(node);
	}

}
