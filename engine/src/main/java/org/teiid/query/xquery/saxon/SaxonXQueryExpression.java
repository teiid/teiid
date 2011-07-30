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
import java.io.Writer;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;

import net.sf.saxon.AugmentedSource;
import net.sf.saxon.Configuration;
import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.expr.AxisExpression;
import net.sf.saxon.expr.ContextItemExpression;
import net.sf.saxon.expr.Expression;
import net.sf.saxon.expr.PathMap;
import net.sf.saxon.expr.RootExpression;
import net.sf.saxon.expr.PathMap.PathMapArc;
import net.sf.saxon.expr.PathMap.PathMapNode;
import net.sf.saxon.expr.PathMap.PathMapNodeSet;
import net.sf.saxon.expr.PathMap.PathMapRoot;
import net.sf.saxon.om.Axis;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
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
import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import nux.xom.xquery.StreamingPathFilter;
import nux.xom.xquery.StreamingTransform;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.WSConnection.Util;

@SuppressWarnings("serial")
public class SaxonXQueryExpression {
	
	public static final Properties DEFAULT_OUTPUT_PROPERTIES = new Properties();
	{
		DEFAULT_OUTPUT_PROPERTIES.setProperty(OutputKeys.METHOD, "xml"); //$NON-NLS-1$
	    //props.setProperty(OutputKeys.INDENT, "yes"); //$NON-NLS-1$
		DEFAULT_OUTPUT_PROPERTIES.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
	}
	
	private static Nodes NONE = new Nodes(); 
	private static InputStream FAKE_IS = new InputStream() {

		@Override
		public int read() throws IOException {
			return 0;
		}
	};
	
	public interface RowProcessor {
		
		void processRow(NodeInfo row);

	}
	
	public static class Result {
		public SequenceIterator iter;
		public List<Source> sources = new LinkedList<Source>();
		
		public void close() {
			for (Source source : sources) {
				Util.closeSource(source);
			}
			if (iter != null) {
				iter.close();
			}
			sources.clear();
			iter = null;
		}
	}
	
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

	private XQueryExpression xQuery;
	private String xQueryString;
	private Map<String, String> namespaceMap = new HashMap<String, String>();
	private Configuration config = new Configuration();
	private PathMapRoot contextRoot;
	private StreamingPathFilter streamingPathFilter;

    public SaxonXQueryExpression(String xQueryString, XMLNamespaces namespaces, List<DerivedColumn> passing, List<XMLTable.XMLColumn> columns) 
    throws QueryResolverException {
        config.setErrorListener(ERROR_LISTENER);
        this.xQueryString = xQueryString;
        StaticQueryContext context = new StaticQueryContext(config);
        IndependentContext ic = new IndependentContext(config);
        namespaceMap.put("", ""); //$NON-NLS-1$ //$NON-NLS-2$
        if (namespaces != null) {
        	for (NamespaceItem item : namespaces.getNamespaceItems()) {
        		if (item.getPrefix() == null) {
        			if (item.getUri() == null) {
        				context.setDefaultElementNamespace(""); //$NON-NLS-1$
        				ic.setDefaultElementNamespace(""); //$NON-NLS-1$
        			} else {
        				context.setDefaultElementNamespace(item.getUri());
        				ic.setDefaultElementNamespace(item.getUri());
        				namespaceMap.put("", item.getUri()); //$NON-NLS-1$
        			}
        		} else {
    				context.declareNamespace(item.getPrefix(), item.getUri());
    				ic.declareNamespace(item.getPrefix(), item.getUri());
    				namespaceMap.put(item.getPrefix(), item.getUri());
        		}
			}
        }
        for (DerivedColumn derivedColumn : passing) {
        	if (derivedColumn.getAlias() == null) {
        		continue;
        	}
        	try {
				context.declareGlobalVariable(StructuredQName.fromClarkName(derivedColumn.getAlias()), SequenceType.ANY_SEQUENCE, null, true);
			} catch (XPathException e) {
				//this is always expected to work
				throw new TeiidRuntimeException(e, "Could not define global variable"); //$NON-NLS-1$ 
			}
		}
        
    	processColumns(columns, ic);	    	
    
        try {
			this.xQuery = context.compileQuery(xQueryString);
		} catch (XPathException e) {
			throw new QueryResolverException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.compile_failed")); //$NON-NLS-1$
		}
    }
    
    private SaxonXQueryExpression() {
    	
    }
    
    public SaxonXQueryExpression clone() {
    	SaxonXQueryExpression clone = new SaxonXQueryExpression();
    	clone.xQuery = xQuery;
    	clone.xQueryString = xQueryString;
    	clone.config = config;
    	clone.contextRoot = contextRoot;
    	clone.namespaceMap = namespaceMap;
    	clone.streamingPathFilter = streamingPathFilter;
    	return clone;
    }
    
    public boolean usesContextItem() {
    	return this.xQuery.usesContextItem();
    }
    
	public void useDocumentProjection(List<XMLTable.XMLColumn> columns, AnalysisRecord record) {
		try {
			streamingPathFilter = StreamingUtils.getStreamingPathFilter(xQueryString, namespaceMap);
		} catch (IllegalArgumentException e) {
			if (record.recordDebug()) {
				record.println("Document streaming will not be used: " + e.getMessage()); //$NON-NLS-1$
			}
		}
		this.contextRoot = null;
		PathMap map = this.xQuery.getPathMap();
		PathMapRoot parentRoot;
		try {
			parentRoot = map.getContextRoot();
		} catch (IllegalStateException e) {
			if (record.recordDebug()) {
				record.println("Document projection will not be used, since multiple context item exist."); //$NON-NLS-1$
			}
			return;
		}
		if (parentRoot == null) {
			//TODO: this seems like we could omit the context item altogether
			//this.xQuery.usesContextItem() should also be false
			if (record.recordDebug()) {
				record.println("Document projection will not be used, since no context item reference was found in the XQuery"); //$NON-NLS-1$
			}
			return;			
		}
		HashSet<PathMapNode> finalNodes = new HashSet<PathMapNode>();
		getReturnableNodes(parentRoot, finalNodes);
				
		if (!finalNodes.isEmpty()) {  
			if (columns != null && !columns.isEmpty()) {
				if (finalNodes.size() != 1) {
					if (record.recordDebug()) {
						record.println("Document projection will not be used, since multiple return items exist"); //$NON-NLS-1$
					}
					return;	
				} 
				parentRoot = projectColumns(parentRoot, columns, finalNodes.iterator().next(), record);
				if (parentRoot == null) {
					return;
				}
			} else {
				for (Iterator<PathMapNode> iter = finalNodes.iterator(); iter.hasNext(); ) {
	                PathMapNode subNode = iter.next();
	                subNode.createArc(new AxisExpression(Axis.DESCENDANT_OR_SELF, AnyNodeTest.getInstance()));
	            }
			}
		} 
		if (parentRoot.hasUnknownDependencies()) {
			if (record.recordDebug()) {
				record.println("Document projection will not be used since there are unknown dependencies (most likely a user defined function)."); //$NON-NLS-1$
			}
	    	return;
		}
		if (record.recordDebug()) {
			StringBuilder sb = new StringBuilder();
	    	showArcs(sb, parentRoot, 0);
	    	record.println("Using path filtering for XQuery context item: \n" + sb.toString()); //$NON-NLS-1$
		}
		this.contextRoot = parentRoot;
	}
	
    public static final boolean[] isValidAncestorAxis =
    {
        false,          // ANCESTOR
        false,          // ANCESTOR_OR_SELF;
        true,           // ATTRIBUTE;
        false,           // CHILD;
        false,           // DESCENDANT;
        false,           // DESCENDANT_OR_SELF;
        false,          // FOLLOWING;
        false,          // FOLLOWING_SIBLING;
        true,           // NAMESPACE;
        true,          // PARENT;
        false,          // PRECEDING;
        false,          // PRECEDING_SIBLING;
        true,           // SELF;
        false,          // PRECEDING_OR_ANCESTOR;
    };

	private PathMapRoot projectColumns(PathMapRoot parentRoot, List<XMLTable.XMLColumn> columns, PathMapNode finalNode, AnalysisRecord record) {
		for (XMLColumn xmlColumn : columns) {
			if (xmlColumn.isOrdinal()) {
				continue;
			}
	    	Expression internalExpression = xmlColumn.getPathExpression().getInternalExpression();
	    	PathMap subMap = new PathMap(internalExpression);
	    	PathMapRoot subContextRoot = null;
	    	for (PathMapRoot root : subMap.getPathMapRoots()) {
				if (root.getRootExpression() instanceof ContextItemExpression || root.getRootExpression() instanceof RootExpression) {
					if (subContextRoot != null) {
						if (record.recordDebug()) {
							record.println("Document projection will not be used, since multiple context items exist in column path " + xmlColumn.getPath()); //$NON-NLS-1$
						}
						return null;
					}
					subContextRoot = root;
				}
			}
	    	if (subContextRoot == null) {
	    		//special case for handling '.', which the pathmap logic doesn't consider as a root
	    		if (internalExpression instanceof ContextItemExpression) {
	    			addReturnedArcs(xmlColumn, finalNode);
	    		}
	    		continue;
	    	}
	    	for (PathMapArc arc : subContextRoot.getArcs()) {
	    		if (streamingPathFilter != null && !validateColumnForStreaming(record, xmlColumn, arc)) {
	    			streamingPathFilter = null;
	    		}
				finalNode.createArc(arc.getStep(), arc.getTarget());
			}
	    	HashSet<PathMapNode> subFinalNodes = new HashSet<PathMapNode>();
			getReturnableNodes(subContextRoot, subFinalNodes);
	    	for (PathMapNode subNode : subFinalNodes) {
		    	addReturnedArcs(xmlColumn, subNode);
	        }
		}
		//Workaround to rerun the reduction algorithm - by making a copy of the old version
		PathMap newMap = new PathMap(DUMMY_EXPRESSION);
		PathMapRoot newRoot = newMap.makeNewRoot(parentRoot.getRootExpression());
		if (parentRoot.isAtomized()) {
			newRoot.setAtomized();
		}
		if (parentRoot.isReturnable()) {
			newRoot.setReturnable(true);
		}
		if (parentRoot.hasUnknownDependencies()) {
			newRoot.setHasUnknownDependencies();
		}
		for (PathMapArc arc : parentRoot.getArcs()) {
			newRoot.createArc(arc.getStep(), arc.getTarget());
		}
		return newMap.reduceToDownwardsAxes(newRoot);
	}

	private boolean validateColumnForStreaming(AnalysisRecord record,
			XMLColumn xmlColumn, PathMapArc arc) {
		boolean ancestor = false;
		LinkedList<PathMapArc> arcStack = new LinkedList<PathMapArc>();
		arcStack.add(arc);
		while (!arcStack.isEmpty()) {
			PathMapArc current = arcStack.removeFirst();
			byte axis = current.getStep().getAxis();
			if (ancestor) {
				if (current.getTarget().isReturnable()) {
					if (axis != Axis.NAMESPACE && axis != Axis.ATTRIBUTE) {
						if (record.recordDebug()) {
							record.println("Document streaming will not be used, since the column path contains an invalid reverse axis " + xmlColumn.getPath()); //$NON-NLS-1$
						}
						return false;
					}
				}
				if (!isValidAncestorAxis[axis]) {
					if (record.recordDebug()) {
						record.println("Document streaming will not be used, since the column path contains an invalid reverse axis " + xmlColumn.getPath()); //$NON-NLS-1$
					}
					return false;
				}
			} else if (!Axis.isSubtreeAxis[axis]) {
				if (axis == Axis.PARENT 
						|| axis == Axis.ANCESTOR
						|| axis == Axis.ANCESTOR_OR_SELF) {
					if (current.getTarget().isReturnable()) {
						if (record.recordDebug()) {
							record.println("Document streaming will not be used, since the column path contains an invalid reverse axis " + xmlColumn.getPath()); //$NON-NLS-1$
						}
						return false;
					}
					ancestor = true; 
				} else {
					if (record.recordDebug()) {
						record.println("Document streaming will not be used, since the column path may not reference an ancestor or subtree " + xmlColumn.getPath()); //$NON-NLS-1$
					}
					return false;
				}
			}
	    	for (PathMapArc pathMapArc : current.getTarget().getArcs()) {
	    		arcStack.add(pathMapArc);
			}
		}
		return true;
	}

	private void addReturnedArcs(XMLColumn xmlColumn, PathMapNode subNode) {
		if (xmlColumn.getSymbol().getType() == DataTypeManager.DefaultDataClasses.XML) {
			subNode.createArc(new AxisExpression(Axis.DESCENDANT_OR_SELF, AnyNodeTest.getInstance()));
		} else {
			//this may not always be needed, but it doesn't harm anything
			subNode.createArc(new AxisExpression(Axis.CHILD, NodeKindTest.TEXT));
			subNode.setAtomized();
		}
	}

	private void getReturnableNodes(PathMapNode node, HashSet<PathMapNode> finalNodes) {
		if (node.isReturnable()) {
			finalNodes.add(node);
		}
		for (PathMapArc arc : node.getArcs()) {
			getReturnableNodes(arc.getTarget(), finalNodes);
		}
	}

	private void processColumns(List<XMLTable.XMLColumn> columns, IndependentContext ic)
			throws QueryResolverException {
		if (columns == null) {
			return;
		}
        XPathEvaluator eval = new XPathEvaluator(config);
    	eval.setStaticContext(ic);
		for (XMLColumn xmlColumn : columns) {
        	if (xmlColumn.isOrdinal()) {
        		continue;
        	}
        	String path = xmlColumn.getPath();
        	if (path == null) {
        		path = xmlColumn.getName();
        	}
        	path = path.trim();
        	if (path.startsWith("/")) { //$NON-NLS-1$ 
        		if (path.startsWith("//")) { //$NON-NLS-1$
        			path = '.' + path;
        		} else {
        			path = path.substring(1);
        		}
        	}
	    	XPathExpression exp;
			try {
				exp = eval.createExpression(path);
			} catch (XPathException e) {
				throw new QueryResolverException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.invalid_path", xmlColumn.getName(), xmlColumn.getPath())); //$NON-NLS-1$
			}	
	    	xmlColumn.setPathExpression(exp);
		}
	}
	
    public Result evaluateXQuery(Object context, Map<String, Object> parameterValues, final RowProcessor processor, CommandContext commandContext) throws TeiidProcessingException {
        DynamicQueryContext dynamicContext = new DynamicQueryContext(config);

        Result result = new Result();
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
	            if (contextRoot != null) {
	            	//create our own filter as this logic is not provided in the free saxon
	                ProxyReceiver filter = new PathMapFilter(contextRoot);
	                AugmentedSource sourceInput = AugmentedSource.makeAugmentedSource(source);
	                sourceInput.addFilter(filter);
	                source = sourceInput;

                	//use streamable processing instead
	                if (streamingPathFilter != null && processor != null) {
	                	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
	                		LogManager.logDetail(LogConstants.CTX_DQP, "Using stream processing for evaluation of", this.xQueryString); //$NON-NLS-1$
	                	}
	                	//set to non-blocking in case default expression evaluation blocks
	                	boolean isNonBlocking = commandContext.isNonBlocking();
    					commandContext.setNonBlocking(true);
    					
						final StreamingTransform myTransform = new StreamingTransform() {
							public Nodes transform(Element elem) {
								processor.processRow(StreamingUtils.wrap(elem, config));
								return NONE;
							}
						};
						
						Builder builder = new Builder(new SaxonReader(config, sourceInput), false, 
								streamingPathFilter.createNodeFactory(null, myTransform));
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
					doc = config.buildDocument(source);
				} catch (XPathException e) {
					throw new TeiidProcessingException(e, QueryPlugin.Util.getString("SaxonXQueryExpression.bad_context")); //$NON-NLS-1$
				}
		        dynamicContext.setContextItem(doc);
	        }
	        try {
	        	result.iter = xQuery.iterator(dynamicContext);
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
    
	public XMLType createXMLType(final SequenceIterator iter, BufferManager bufferManager, boolean emptyOnEmpty) throws XPathException, TeiidComponentException, TeiidProcessingException {
		Item item = iter.next();
		if (item == null && !emptyOnEmpty) {
			return null;
		}
		XMLType.Type type = Type.CONTENT;
		if (item instanceof NodeInfo) {
			NodeInfo info = (NodeInfo)item;
			type = getType(info);
		}
		Item next = iter.next();
		if (next != null) {
			type = Type.CONTENT;
		}
		SQLXMLImpl xml = XMLSystemFunctions.saveToBufferManager(bufferManager, new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
			    QueryResult.serializeSequence(iter.getAnother(), config, writer, DEFAULT_OUTPUT_PROPERTIES);
			}
		});
		XMLType value = new XMLType(xml);
		value.setType(type);
		return value;
	}

	public static XMLType.Type getType(NodeInfo info) {
		switch (info.getNodeKind()) {
			case net.sf.saxon.type.Type.DOCUMENT:
				return Type.DOCUMENT;
			case net.sf.saxon.type.Type.ELEMENT:
				return Type.ELEMENT;
			case net.sf.saxon.type.Type.TEXT:
				return Type.TEXT;
			case net.sf.saxon.type.Type.COMMENT:
				return Type.COMMENT;
			case net.sf.saxon.type.Type.PROCESSING_INSTRUCTION:
				return Type.PI;
		}
		return Type.CONTENT;
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
	
	public boolean isStreaming() {
		return streamingPathFilter != null;
	}

}
