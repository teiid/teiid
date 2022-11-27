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
import java.io.InputStream;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.function.source.XMLSystemFunctions.XmlConcat;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.util.CommandContext;
import org.teiid.xquery.saxon.SaxonXQueryExpression.Result;
import org.teiid.xquery.saxon.SaxonXQueryExpression.RowProcessor;

import net.sf.saxon.Configuration;
import net.sf.saxon.event.FilterFactory;
import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.expr.JPConverter;
import net.sf.saxon.lib.AugmentedSource;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.om.TreeInfo;
import net.sf.saxon.option.xom.XOMDocumentWrapper;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.QueryResult;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.ValidationException;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.DateTimeValue;
import net.sf.saxon.value.DateValue;
import net.sf.saxon.value.DayTimeDurationValue;
import net.sf.saxon.value.HexBinaryValue;
import net.sf.saxon.value.StringValue;
import net.sf.saxon.value.TimeValue;
import nu.xom.Builder;
import nu.xom.DocType;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import nux.xom.xquery.StreamingPathFilter;
import nux.xom.xquery.StreamingTransform;

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

    public static SaxonXQueryExpression.Result evaluateXQuery(final SaxonXQueryExpression xquery, Object context, Map<String, Object> parameterValues, final RowProcessor processor, CommandContext commandContext) throws TeiidProcessingException, TeiidComponentException {
        DynamicQueryContext dynamicContext = new DynamicQueryContext(xquery.config);

        SaxonXQueryExpression.Result result = new SaxonXQueryExpression.Result();
        try {
            for (Map.Entry<String, Object> entry : parameterValues.entrySet()) {
                try {
                    Object value = entry.getValue();
                    Sequence s = null;
                    if(value instanceof SQLXML) {
                        value = XMLSystemFunctions.convertToSource(value);
                        result.sources.add((Source)value);
                        Source source = wrapStax((Source)value);
                        s = xquery.config.buildDocumentTree(source).getRootNode();
                    } else if (value instanceof java.util.Date) {
                        s = XQueryEvaluator.convertToAtomicValue(value);
                    } else if (value instanceof BinaryType) {
                        s = new HexBinaryValue(((BinaryType)value).getBytesDirect());
                    }
                    dynamicContext.setParameter(StructuredQName.fromClarkName(entry.getKey()), s);
                } catch (TransformerException e) {
                    throw new TeiidProcessingException(QueryPlugin.Event.TEIID30148, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30148, entry.getKey()));
                }
            }
            if (context != null) {
                Source source = XMLSystemFunctions.convertToSource(context);
                result.sources.add(source);
                source = wrapStax(source);
                if (xquery.contextRoot != null) {
                    //create our own filter as this logic is not provided in the free saxon
                    AugmentedSource sourceInput = AugmentedSource.makeAugmentedSource(source);
                    sourceInput.addFilter(new FilterFactory() {

                        @Override
                        public ProxyReceiver makeFilter(Receiver arg0) {
                            return new PathMapFilter(xquery.contextRoot, arg0);
                        }
                    });
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
                             if (e.getCause() instanceof TeiidRuntimeException) {
                                 RelationalNode.unwrapException((TeiidRuntimeException)e.getCause());
                             }
                             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30151, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30151));
                        } catch (IOException e) {
                             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30151, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30151));
                        } finally {
                            if (!isNonBlocking) {
                                commandContext.setNonBlocking(false);
                            }
                        }
                    }
                }
                TreeInfo doc;
                try {
                    doc = xquery.config.buildDocumentTree(source);
                } catch (XPathException e) {
                     throw new TeiidProcessingException(QueryPlugin.Event.TEIID30151, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30151));
                }
                dynamicContext.setContextItem(doc.getRootNode());
            }
            try {
                result.iter = xquery.xQuery.iterator(dynamicContext);
                return result;
            } catch (TransformerException e) {
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30152, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30152));
            }
        } finally {
            if (result.iter == null) {
                result.close();
            }
        }
    }

    private static Source wrapStax(Source value) throws TeiidProcessingException {
        if (value instanceof StAXSource) {
            StAXSource staxSource = (StAXSource)value;
            if (staxSource.getXMLStreamReader() != null) {
                return staxSource;
            }
            try {
                return new StAXSource(new XMLEventStreamReader(staxSource.getXMLEventReader()));
            } catch (XMLStreamException e) {
                //should not happen as the StAXSource already peeked
                throw new TeiidProcessingException(e);
            }
        }
        return value;
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

        XOMDocumentWrapper docWrapper = new XOMDocumentWrapper(root, config);

        return docWrapper.wrap(node);
    }

    static final class XMLQueryRowProcessor implements RowProcessor {
        XmlConcat concat; //just used to get a writer
        Type type;
        private javax.xml.transform.Result result;
        boolean hasItem;

        XMLQueryRowProcessor(boolean exists, CommandContext context) throws TeiidProcessingException {
            if (!exists) {
                concat = new XmlConcat(context.getBufferManager());
                result = new StreamResult(concat.getWriter());
            }
        }

        @Override
        public void processRow(NodeInfo row) {
            if (concat == null) {
                hasItem = true;
                return;
            }
            if (type == null) {
                type = SaxonXQueryExpression.getType(row);
            } else {
                type = Type.CONTENT;
            }
            try {
                QueryResult.serialize(row, result, SaxonXQueryExpression.DEFAULT_OUTPUT_PROPERTIES);
            } catch (XPathException e) {
                 throw new TeiidRuntimeException(e);
            }
        }
     }

    /**
     *
     * @param tuple
     * @param xmlQuery
     * @param exists - check only for the existence of a non-empty result
     * @return Boolean if exists is true, otherwise an XMLType value
     * @throws BlockedException
     * @throws TeiidComponentException
     * @throws FunctionExecutionException
     */
    public static Object evaluateXMLQuery(List<?> tuple, XMLQuery xmlQuery, boolean exists, Map<String, Object> parameters, CommandContext context)
            throws BlockedException, TeiidComponentException,
            FunctionExecutionException {
        boolean emptyOnEmpty = xmlQuery.getEmptyOnEmpty() == null || xmlQuery.getEmptyOnEmpty();
        Result result = null;
        try {
            XMLQueryRowProcessor rp = null;
            SaxonXQueryExpression sxqe = (SaxonXQueryExpression)xmlQuery.getXQueryExpression();
            if (sxqe.isStreaming()) {
                rp = new XMLQueryRowProcessor(exists, context);
            }
            try {
                Object contextItem = null;
                if (parameters.containsKey(null)) {
                    contextItem = parameters.remove(null);
                    if (contextItem == null) {
                        return null;
                    }
                }
                result = evaluateXQuery(sxqe, contextItem, parameters, rp, context);
                if (result == null) {
                    return null;
                }
                if (exists) {
                    if (result.iter.next() == null) {
                        return false;
                    }
                    return true;
                }
            } catch (TeiidRuntimeException e) {
                if (e.getCause() instanceof XPathException) {
                    throw (XPathException)e.getCause();
                }
                throw e;
            }
            if (rp != null) {
                if (exists) {
                    return rp.hasItem;
                }
                XMLType.Type type = rp.type;
                if (type == null) {
                    if (!emptyOnEmpty) {
                        return null;
                    }
                    type = Type.CONTENT;
                }
                XMLType val = rp.concat.close(context);
                val.setType(rp.type);
                return val;
            }
            return sxqe.createXMLType(result.iter, context.getBufferManager(), emptyOnEmpty, context);
        } catch (TeiidProcessingException e) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30333, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30333, e.getMessage()));
        } catch (XPathException e) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30333, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30333, e.getMessage()));
        } finally {
            if (result != null) {
                result.close();
            }
        }
    }

    public static Object evaluate(XMLType value, XMLCast expression, CommandContext context) throws ExpressionEvaluationException {
        Configuration config = new Configuration();
        Type t = value.getType();
        try {
            Item i = null;
            switch (t) {
                case CONTENT:
                    //content could map to an array value, but we aren't handling that case here yet - only in xmltable
                case COMMENT:
                case PI:
                    throw new FunctionExecutionException();
                case TEXT:
                    i = new StringValue(value.getString());
                    break;
                case UNKNOWN:
                case DOCUMENT:
                case ELEMENT:
                    StreamSource ss = value.getSource(StreamSource.class);
                    try {
                        i = config.buildDocument(ss);
                    } finally {
                        if (ss.getInputStream() != null) {
                            ss.getInputStream().close();
                        }
                        if (ss.getReader() != null) {
                            ss.getReader().close();
                        }
                    }
                    break;
                default:
                    throw new AssertionError("Unknown xml value type " + t); //$NON-NLS-1$
            }
            return SaxonXMLTableNode.getValue(expression.getType(), i, config, context);
        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        } catch (ValidationException e) {
            throw new FunctionExecutionException(e);
        } catch (TransformationException e) {
            throw new FunctionExecutionException(e);
        } catch (XPathException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        }
    }

    public static AtomicValue convertToAtomicValue(Object value) throws TransformerException {
        if (value instanceof java.util.Date) { //special handling for time types
            java.util.Date d = (java.util.Date)value;
            DateTimeValue tdv = DateTimeValue.fromJavaDate(d);
            if (value instanceof Date) {
                value = new DateValue(tdv.getYear(), tdv.getMonth(), tdv.getDay(), tdv.getTimezoneInMinutes(), true);
            } else if (value instanceof Time) {
                value = new TimeValue(tdv.getHour(), tdv.getMinute(), tdv.getSecond(), tdv.getMicrosecond(), tdv.getTimezoneInMinutes());
            } else if (value instanceof Timestamp) {
                Timestamp ts = (Timestamp)value;
                value = tdv.add(DayTimeDurationValue.fromMicroseconds(ts.getNanos() / 1000));
            }
            return (AtomicValue)value;
        }
        JPConverter converter = JPConverter.allocate(value.getClass(), null, null);
        return (AtomicValue)converter.convert(value, null);
    }

}
