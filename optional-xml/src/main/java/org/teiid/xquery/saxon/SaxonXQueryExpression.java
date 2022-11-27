/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.xquery.saxon;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stax.StAXSource;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;
import org.teiid.query.util.CommandContext;
import org.teiid.util.WSUtil;

import net.sf.saxon.Configuration;
import net.sf.saxon.expr.ContextItemExpression;
import net.sf.saxon.expr.Expression;
import net.sf.saxon.expr.Operand;
import net.sf.saxon.expr.RootExpression;
import net.sf.saxon.expr.SystemFunctionCall;
import net.sf.saxon.expr.parser.PathMap;
import net.sf.saxon.expr.parser.PathMap.PathMapArc;
import net.sf.saxon.expr.parser.PathMap.PathMapNode;
import net.sf.saxon.expr.parser.PathMap.PathMapNodeSet;
import net.sf.saxon.expr.parser.PathMap.PathMapRoot;
import net.sf.saxon.expr.parser.RebindingMap;
import net.sf.saxon.om.AxisInfo;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.pattern.AnyNodeTest;
import net.sf.saxon.query.QueryResult;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trace.ExpressionPresenter;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.ItemType;
import net.sf.saxon.value.EmptySequence;
import net.sf.saxon.value.SequenceType;

public class SaxonXQueryExpression implements org.teiid.query.xquery.XQueryExpression {

    private static final String XQUERY_PLANNING = "XQuery Planning"; //$NON-NLS-1$
    private static final String EMPTY_STRING = ""; //$NON-NLS-1$

    public static final Properties DEFAULT_OUTPUT_PROPERTIES = new Properties();
    {
        DEFAULT_OUTPUT_PROPERTIES.setProperty(OutputKeys.METHOD, "xml"); //$NON-NLS-1$
        //props.setProperty(OutputKeys.INDENT, "yes"); //$NON-NLS-1$
        DEFAULT_OUTPUT_PROPERTIES.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
    }

    public interface RowProcessor {

        void processRow(NodeInfo row);

    }

    public static class Result {
        public SequenceIterator iter;
        public List<Source> sources = new LinkedList<Source>();

        public void close() {
            for (Source source : sources) {
                WSUtil.closeSource(source);
                if (source instanceof StAXSource) {
                    StAXSource ss = (StAXSource)source;
                    if (ss.getXMLEventReader() != null) {
                        try {
                            ss.getXMLEventReader().close();
                        } catch (XMLStreamException e) {
                        }
                    } else {
                        try {
                            ss.getXMLStreamReader().close();
                        } catch (XMLStreamException e) {
                        }
                    }
                }
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
        protected int computeCardinality() {
            return 0;
        }

        public PathMapNodeSet addToPathMap(PathMap arg0, PathMapNodeSet arg1) {
            return arg1;
        }

        @Override
        public int getImplementationMethod() {
            return 0;
        }

        @Override
        public ItemType getItemType() {
            return null;
        }

        @Override
        public void export(ExpressionPresenter out) throws XPathException {

        }

        @Override
        public Expression copy(RebindingMap rebindings) {
            return null;
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

    XQueryExpression xQuery;
    String xQueryString;
    Map<String, String> namespaceMap = new HashMap<String, String>();
    Configuration config = new Configuration();
    PathMapRoot contextRoot;
    String streamingPath;
    Map<String, XPathExpression> columnMap;

    boolean relativePaths = true;

    public static SaxonXQueryExpression compile(String xQueryString, XMLNamespaces namespaces, List<DerivedColumn> passing, List<XMLTable.XMLColumn> columns)
    throws QueryResolverException {
        SaxonXQueryExpression saxonXQueryExpression = new SaxonXQueryExpression();
        CommandContext cc = CommandContext.getThreadLocalContext();
        if (cc != null) {
            saxonXQueryExpression.relativePaths = cc.getOptions().isRelativeXPath();
        }
        saxonXQueryExpression.config.setErrorListener(ERROR_LISTENER);
        saxonXQueryExpression.xQueryString = xQueryString;
        StaticQueryContext context = saxonXQueryExpression.config.newStaticQueryContext();
        IndependentContext ic = new IndependentContext(saxonXQueryExpression.config);
        saxonXQueryExpression.namespaceMap.put(EMPTY_STRING, EMPTY_STRING);
        if (namespaces != null) {
            for (NamespaceItem item : namespaces.getNamespaceItems()) {
                if (item.getPrefix() == null) {
                    if (item.getUri() == null) {
                        context.setDefaultElementNamespace(EMPTY_STRING);
                        ic.setDefaultElementNamespace(EMPTY_STRING);
                    } else {
                        context.setDefaultElementNamespace(item.getUri());
                        ic.setDefaultElementNamespace(item.getUri());
                        saxonXQueryExpression.namespaceMap.put(EMPTY_STRING, item.getUri());
                    }
                } else {
                    context.declareNamespace(item.getPrefix(), item.getUri());
                    ic.declareNamespace(item.getPrefix(), item.getUri());
                    saxonXQueryExpression.namespaceMap.put(item.getPrefix(), item.getUri());
                }
            }
        }
        for (DerivedColumn derivedColumn : passing) {
            if (derivedColumn.getAlias() == null) {
                continue;
            }
            try {
                context.declareGlobalVariable(StructuredQName.fromClarkName(derivedColumn.getAlias()), SequenceType.ANY_SEQUENCE, EmptySequence.getInstance(), true);
            } catch (XPathException e) {
                //this is always expected to work
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30153, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30153));
            }
        }

        saxonXQueryExpression.processColumns(columns, ic);

        try {
            saxonXQueryExpression.xQuery = context.compileQuery(xQueryString);
        } catch (XPathException e) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30154, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30154, xQueryString));
        }

        return saxonXQueryExpression;
    }

    private SaxonXQueryExpression() {

    }

    @Override
    public SaxonXQueryExpression clone() {
        try {
            return (SaxonXQueryExpression) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    public boolean usesContextItem() {
        return this.xQuery.usesContextItem();
    }

    public void useDocumentProjection(List<XMLTable.XMLColumn> columns, AnalysisRecord record) {
        try {
            streamingPath = StreamingUtils.getStreamingPath(xQueryString, namespaceMap);
        } catch (IllegalArgumentException e) {
            if (record.recordAnnotations()) {
                record.addAnnotation(XQUERY_PLANNING, "Invalid streaming path " + xQueryString + " "+ e.getMessage(), "Document streaming will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }
        this.contextRoot = null;
        PathMap map = new PathMap(this.xQuery.getExpression());
        PathMapRoot parentRoot;
        try {
            parentRoot = map.getContextDocumentRoot();
        } catch (IllegalStateException e) {
            if (record.recordAnnotations()) {
                record.addAnnotation(XQUERY_PLANNING, "Multiple context items exist " + xQueryString, "Document projection will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return;
        }
        if (parentRoot == null) {
            //TODO: this seems like we could omit the context item altogether
            //this.xQuery.usesContextItem() should also be false
            if (record.recordAnnotations()) {
                record.addAnnotation(XQUERY_PLANNING, "No context item reference was found in the XQuery " + xQueryString, "Document projection will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return;
        }
        HashSet<PathMapNode> finalNodes = new HashSet<PathMapNode>();
        getReturnableNodes(parentRoot, finalNodes);

        if (!finalNodes.isEmpty()) {
            if (columns != null && !columns.isEmpty()) {
                if (finalNodes.size() != 1) {
                    if (record.recordAnnotations()) {
                        record.addAnnotation(XQUERY_PLANNING, "multiple return items exist " + xQueryString, "Document projection will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
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
                    subNode.createArc(AxisInfo.DESCENDANT_OR_SELF, AnyNodeTest.getInstance());
                }
            }
        }
        if (parentRoot.hasUnknownDependencies()) {
            if (record.recordAnnotations()) {
                record.addAnnotation(XQUERY_PLANNING, "There are unknown dependencies (most likely a user defined function) in " + xQueryString, "Document projection will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return;
        }
        if (record.recordAnnotations()) {
            StringBuilder sb = null;
            if (record.recordDebug()) {
                sb = new StringBuilder();
                showArcs(sb, parentRoot, 0);
            }
            record.addAnnotation(XQUERY_PLANNING, "Projection conditions met for " + xQueryString, "Document projection will be used" + (sb != null ? "\n" +sb.toString():""), Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
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
            Expression internalExpression = getXPathExpression(xmlColumn.getName()).getInternalExpression();
            if (containsRootFunction(internalExpression)) {
                if (record.recordAnnotations()) {
                    record.addAnnotation(XQUERY_PLANNING, "Root function used in column path " + xmlColumn.getPath(), "Document projection will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
                }
                return null;
            }
            PathMap subMap = new PathMap(internalExpression);
            PathMapRoot subContextRoot = null;
            for (PathMapRoot root : subMap.getPathMapRoots()) {
                if (root.getRootExpression() instanceof ContextItemExpression || root.getRootExpression() instanceof RootExpression) {
                    if (subContextRoot != null) {
                        if (record.recordAnnotations()) {
                            record.addAnnotation(XQUERY_PLANNING, "Multiple context items exist in column path " + xmlColumn.getPath(), "Document projection will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
                        }
                        return null;
                    }
                    subContextRoot = root;
                }
            }
            //special case for handling '.', which the pathmap logic doesn't consider as a root
            if (internalExpression instanceof ContextItemExpression) {
                addReturnedArcs(finalNode);
            }
            if (subContextRoot == null) {
                continue;
            }
            for (PathMapArc arc : subContextRoot.getArcs()) {
                if (streamingPath != null && !validateColumnForStreaming(record, xmlColumn, arc)) {
                    streamingPath = null;
                }
                finalNode.createArc(arc.getAxis(), arc.getNodeTest(), arc.getTarget());
            }
            HashSet<PathMapNode> subFinalNodes = new HashSet<PathMapNode>();
            getReturnableNodes(subContextRoot, subFinalNodes);
            for (PathMapNode subNode : subFinalNodes) {
                addReturnedArcs(subNode);
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
            newRoot.createArc(arc.getAxis(), arc.getNodeTest(), arc.getTarget());
        }
        return newMap.reduceToDownwardsAxes(newRoot);
    }

    private boolean containsRootFunction(Expression internalExpression) {
        if (internalExpression instanceof SystemFunctionCall) {
            SystemFunctionCall sfc = (SystemFunctionCall)internalExpression;
            if (sfc.getDisplayName().equals("fn:root")) { //$NON-NLS-1$
                return true;
            }
        }
        for (Operand ex : internalExpression.operands()) {
            if (containsRootFunction(ex.getChildExpression())) {
                return true;
            }
        }
        return false;
    }

    private boolean validateColumnForStreaming(AnalysisRecord record,
            XMLColumn xmlColumn, PathMapArc arc) {
        boolean ancestor = false;
        LinkedList<PathMapArc> arcStack = new LinkedList<PathMapArc>();
        arcStack.add(arc);
        while (!arcStack.isEmpty()) {
            PathMapArc current = arcStack.removeFirst();
            byte axis = current.getAxis();
            if (ancestor) {
                if (current.getTarget().isReturnable()) {
                    if (axis != AxisInfo.NAMESPACE && axis != AxisInfo.ATTRIBUTE) {
                        if (record.recordAnnotations()) {
                            record.addAnnotation(XQUERY_PLANNING, "The column path contains an invalid reverse axis " + xmlColumn.getPath(), "Document streaming will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
                        }
                        return false;
                    }
                }
                if (!isValidAncestorAxis[axis]) {
                    if (record.recordAnnotations()) {
                        record.addAnnotation(XQUERY_PLANNING, "The column path contains an invalid reverse axis " + xmlColumn.getPath(), "Document streaming will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    return false;
                }
            } else if (!AxisInfo.isSubtreeAxis[axis]) {
                if (axis == AxisInfo.PARENT
                        || axis == AxisInfo.ANCESTOR
                        || axis == AxisInfo.ANCESTOR_OR_SELF) {
                    if (current.getTarget().isReturnable()) {
                        if (record.recordAnnotations()) {
                            record.addAnnotation(XQUERY_PLANNING, "The column path contains an invalid reverse axis " + xmlColumn.getPath(), "Document streaming will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
                        }
                        return false;
                    }
                    ancestor = true;
                } else {
                    if (record.recordAnnotations()) {
                        record.addAnnotation(XQUERY_PLANNING, "The column path may not reference an ancestor or subtree " + xmlColumn.getPath(), "Document streaming will not be used", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$
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

    private void addReturnedArcs(PathMapNode subNode) {
        subNode.createArc(AxisInfo.DESCENDANT_OR_SELF, AnyNodeTest.getInstance());
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
        this.columnMap = new HashMap<>();
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
            if (relativePaths && path.startsWith("/")) { //$NON-NLS-1$
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
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30155, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30155, xmlColumn.getName(), xmlColumn.getPath()));
            }
            this.columnMap.put(xmlColumn.getName(), exp);
        }
    }

    public XMLType createXMLType(final SequenceIterator iter, BufferManager bufferManager, boolean emptyOnEmpty, CommandContext context) throws XPathException, TeiidComponentException, TeiidProcessingException {
        final Item item = iter.next();
        if (item == null && !emptyOnEmpty) {
            return null;
        }
        XMLType.Type type = Type.CONTENT;
        if (item instanceof NodeInfo) {
            NodeInfo info = (NodeInfo)item;
            type = getType(info);
        }
        final Item next = iter.next();
        if (next != null) {
            type = Type.CONTENT;
        }
        SQLXMLImpl xml = XMLSystemFunctions.saveToBufferManager(bufferManager, new XMLTranslator() {

            @Override
            public void translate(Writer writer) throws TransformerException,
                    IOException {
                QueryResult.serializeSequence(new PushBackSequenceIterator(iter, item, next), config, writer, DEFAULT_OUTPUT_PROPERTIES);
            }
        }, context);
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
            sb.append(AxisInfo.axisName[pathMapArc.getAxis()]);
            sb.append(' ');
            sb.append(pathMapArc.getNodeTest());
            sb.append('\n');
            node = pathMapArc.getTarget();
            showArcs(sb, node, level + 1);
        }
    }

    /**
     * Streaming eligible if using document projection and
     * the context path is streamable.
     */
    public boolean isStreaming() {
        return streamingPath != null && contextRoot != null;
    }

    public XPathExpression getXPathExpression(String name) {
        if (columnMap == null) {
            return null;
        }
        return columnMap.get(name);
    }

}
