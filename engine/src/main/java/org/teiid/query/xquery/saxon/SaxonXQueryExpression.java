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

import java.io.StringReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.AugmentedSource;
import net.sf.saxon.Configuration;
import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.expr.AxisExpression;
import net.sf.saxon.expr.Expression;
import net.sf.saxon.expr.PathMap;
import net.sf.saxon.expr.PathMap.PathMapArc;
import net.sf.saxon.expr.PathMap.PathMapNode;
import net.sf.saxon.expr.PathMap.PathMapNodeSet;
import net.sf.saxon.expr.PathMap.PathMapRoot;
import net.sf.saxon.om.Axis;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.pattern.AnyNodeTest;
import net.sf.saxon.pattern.NodeKindTest;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.QueryResult;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trace.ExpressionPresenter;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.ItemType;
import net.sf.saxon.type.TypeHierarchy;
import net.sf.saxon.value.SequenceType;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;

public class SaxonXQueryExpression {
	
	private static final Expression DUMMY_EXPRESSION = new Expression() {
		@Override
		public ItemType getItemType(TypeHierarchy th) {
			return null;
		}

		@Override
		public void explain(ExpressionPresenter out) {
		}

		@Override
		public Expression copy() {
			return null;
		}

		@Override
		protected int computeCardinality() {
			return 0;
		}

		@Override
		public PathMapNodeSet addToPathMap(PathMap pathMap,
				PathMapNodeSet pathMapNodeSet) {
			return pathMapNodeSet;
		}
	};

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

	private net.sf.saxon.query.XQueryExpression xQuery;    
	private PathMap.PathMapRoot contextRoot;
	private Configuration config = new Configuration();
	
    public SaxonXQueryExpression(String xQueryString, XMLNamespaces namespaces, List<DerivedColumn> passing, List<XMLTable.XMLColumn> columns) 
    throws TeiidProcessingException {
        config.setErrorListener(ERROR_LISTENER);
        StaticQueryContext context = new StaticQueryContext(config);
        IndependentContext ic = new IndependentContext(config);
        
        if (namespaces != null) {
        	for (NamespaceItem item : namespaces.getNamespaceItems()) {
        		if (item.getPrefix() == null) {
        			if (item.getUri() == null) {
        				context.setDefaultElementNamespace(""); //$NON-NLS-1$
        				ic.setDefaultElementNamespace(""); //$NON-NLS-1$
        			} else {
        				context.setDefaultElementNamespace(item.getUri());
        				ic.setDefaultElementNamespace(item.getUri());
        			}
        		} else {
    				context.declareNamespace(item.getPrefix(), item.getUri());
    				ic.declareNamespace(item.getPrefix(), item.getUri());
        		}
			}
        }
    	boolean hasContext = false;
        for (DerivedColumn derivedColumn : passing) {
        	if (derivedColumn.getAlias() == null) {
        		hasContext = true; //skip the context item
        		continue;
        	}
        	try {
				context.declareGlobalVariable(StructuredQName.fromClarkName(derivedColumn.getAlias()), SequenceType.ANY_SEQUENCE, null, true);
			} catch (XPathException e) {
				throw new TeiidRuntimeException(e, "Could not define global variable"); //$NON-NLS-1$ 
			}
        	ic.declareVariable("", derivedColumn.getAlias()); //$NON-NLS-1$
		}
        
        XPathEvaluator eval = new XPathEvaluator(config);
    	eval.setStaticContext(ic);
    	
    	processColumns(columns, eval);	    	
    
        try {
			this.xQuery = context.compileQuery(xQueryString);
		} catch (XPathException e) {
			throw new QueryResolverException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.compile_failed")); //$NON-NLS-1$
		}
        if (hasContext) {
        	useDocumentProjection(columns);
        } else {
	    	LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, "Document projection will not be used, since no context item exists or there are unknown dependencies"); //$NON-NLS-1$
        }
    }

	private void useDocumentProjection(List<XMLTable.XMLColumn> columns) {
		XQueryExpression toAnalyze = this.xQuery;
		
		PathMap map = new PathMap(DUMMY_EXPRESSION);
		PathMapNodeSet set = toAnalyze.getExpression().addToPathMap(map, null);
		
		boolean complexEndState = false;
		if (set != null) {  
			if (columns != null && !columns.isEmpty()) {
				if (set.size() != 1) {
					complexEndState = true;
				} else {
					for (XMLColumn xmlColumn : columns) {
						if (xmlColumn.isOrdinal()) {
							continue;
						}
				    	Expression internalExpression = xmlColumn.getPathExpression().getInternalExpression();
				    	PathMap subMap = new PathMap(internalExpression);
				    	PathMapRoot root = subMap.getContextRoot();
				    	if (root == null) {
				    		continue;
				    	}
				    	PathMapNodeSet finalNodes = internalExpression.addToPathMap(map, set);
				    	if (finalNodes != null) {
				    		for (Iterator iter = finalNodes.iterator(); iter.hasNext(); ) {
				                PathMapNode subNode = (PathMapNode)iter.next();
						    	if (xmlColumn.getSymbol().getType() == DataTypeManager.DefaultDataClasses.XML) {
						    		subNode.createArc(new AxisExpression(Axis.DESCENDANT_OR_SELF, AnyNodeTest.getInstance()));
						    		subNode.setReturnable(true);
						    	} else {
						    		//this may not always be needed, but it doesn't harm anything
						    		subNode.createArc(new AxisExpression(Axis.CHILD, NodeKindTest.TEXT));
						    		subNode.setAtomized();
						    	}
				            }
				    	}
					}
				}
			} else {
				for (Iterator iter = set.iterator(); iter.hasNext(); ) {
	                PathMapNode subNode = (PathMapNode)iter.next();
	                subNode.createArc(new AxisExpression(Axis.DESCENDANT_OR_SELF, AnyNodeTest.getInstance()));
	                subNode.setReturnable(true);
	            }
			}
		} 
		//PathMap map = toAnalyze.getPathMap();
		contextRoot = map.getContextRoot();
		if (contextRoot == null || complexEndState || contextRoot.hasUnknownDependencies()) {
	    	LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, "Document projection will not be used, since no context item exists or there are unknown dependencies"); //$NON-NLS-1$
			contextRoot = null;
		} else if (LogManager.isMessageToBeRecorded(LogConstants.CTX_QUERY_PLANNER, MessageLevel.DETAIL)) {
	    	StringBuilder sb = new StringBuilder();
	    	showArcs(sb, contextRoot, 0);
	    	LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, "Using path filtering for XQuery context item: \n" + sb.toString()); //$NON-NLS-1$
		}
/*		StringBuilder sb = new StringBuilder();
    	showArcs(sb, contextRoot, 0);
    	System.out.println(sb);
*/	}

	private void processColumns(List<XMLTable.XMLColumn> columns, XPathEvaluator eval)
			throws TeiidProcessingException {
		if (columns == null) {
			return;
		}
		for (XMLColumn xmlColumn : columns) {
        	if (xmlColumn.isOrdinal()) {
        		continue;
        	}
        	String path = xmlColumn.getPath();
        	if (path == null) {
        		path = xmlColumn.getName();
        	}
        	path = path.trim();
        	if (path.startsWith("/") && !path.startsWith("//")) {
        		path = path.substring(1);
        	}
	    	XPathExpression exp;
			try {
				exp = eval.createExpression(path);
			} catch (XPathException e) {
				throw new TeiidProcessingException(e, "Invalid path expression");
			}	
	    	xmlColumn.setPathExpression(exp);
		}
	}
    
    public static Source convertToSource(Object value) throws TeiidProcessingException {
    	if (value == null) {
    		return null;
    	}
    	try {
	    	if (value instanceof XMLType) {
				return ((SQLXML)value).getSource(null);
	    	}
	    	if (value instanceof String) {
	    		return new StreamSource(new StringReader((String)value));
	    	}
	    	if (value instanceof ClobType) {
	    		return new StreamSource(((Clob)value).getCharacterStream());
	    	}
    	} catch (SQLException e) {
			throw new TeiidProcessingException(e);
		}
    	throw new AssertionError("Unknown type"); //$NON-NLS-1$
    }
    
    public SequenceIterator evaluateXQuery(Object context, Map<String, Object> parameterValues) throws TeiidProcessingException {
        DynamicQueryContext dynamicContext = new DynamicQueryContext(config);
        
        for (Map.Entry<String, Object> entry : parameterValues.entrySet()) {
            Object value = entry.getValue();
            if(value instanceof SQLXML) {                    
                try {
                    value = ((SQLXML)value).getSource(null);
                } catch (SQLException e) {
                    throw new TeiidProcessingException(e);
                }
            }
            dynamicContext.setParameter(entry.getKey(), value);                
		}
        
        if (context != null) {
        	Source source = convertToSource(context);
            if (contextRoot != null) {
            	//create our own filter as this logic is not provided in the free saxon
                ProxyReceiver filter = new PathMapFilter(contextRoot);
                AugmentedSource sourceInput = AugmentedSource.makeAugmentedSource(source);
                sourceInput.addFilter(filter);
                source = sourceInput;
            }
            DocumentInfo doc;
			try {
				doc = config.buildDocument(source);
			} catch (XPathException e) {
				throw new TeiidProcessingException(e);
			}
	        dynamicContext.setContextItem(doc);
        }
        try {
            //return this.xQuery.iterator(dynamicContext);
        	QueryResult.serializeSequence(xQuery.iterator(dynamicContext), config, System.out, new Properties());
        	return null;
        } catch (TransformerException e) {
        	throw new TeiidProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_xquery")); //$NON-NLS-1$
        }       
    }
    
    public Configuration getConfig() {
		return config;
	}

	public static void showArcs(StringBuilder sb, PathMapNode node, int level) {
		for (PathMapArc pathMapArc : node.getArcs()) {
			char[] pad = new char[level*2];
			Arrays.fill(pad, ' ');
			sb.append(new String(pad));
			sb.append(pathMapArc.getStep());
			sb.append('\n');
			node = pathMapArc.getTarget();
			showArcs(sb, node, level + 1);
		}
	}

}
