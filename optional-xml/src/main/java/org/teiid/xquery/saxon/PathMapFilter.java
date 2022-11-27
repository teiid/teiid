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

import java.util.LinkedList;
import java.util.List;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;

import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.expr.parser.Location;
import net.sf.saxon.expr.parser.PathMap.PathMapArc;
import net.sf.saxon.expr.parser.PathMap.PathMapNode;
import net.sf.saxon.expr.parser.PathMap.PathMapRoot;
import net.sf.saxon.om.AxisInfo;
import net.sf.saxon.om.NamespaceBindingSet;
import net.sf.saxon.om.NodeName;
import net.sf.saxon.pattern.NodeTest;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.SchemaType;
import net.sf.saxon.type.SimpleType;
import net.sf.saxon.type.Type;

/**
 * A filter that uses the PathMap to determine what should be included in the document
 *
 * TODO: optimize filtering by not reconstructing the matchcontexts
 * TODO: we may still need to do xom/nux style handling of large results, but
 *       that requires more analysis to determine subtree independence
 */
class PathMapFilter extends ProxyReceiver {

    static class MatchContext {
        List<PathMapArc> elementArcs;
        List<PathMapArc> attributeArcs;
        boolean matchedElement;
        boolean matchesText;
        boolean matchesComment;

        void bulidContext(PathMapNode node) {
            for (PathMapArc arc : node.getArcs()) {
                processArc(arc);
            }
        }

        void processArc(PathMapArc arc) {
            NodeTest test = arc.getNodeTest();
            if (test == null) {
                addAnyNodeArc(arc);
            } else {
                switch (test.getPrimitiveType()) {
                case Type.TEXT:
                    matchesText = true;
                case Type.NODE:
                    addAnyNodeArc(arc);
                    break;
                case Type.COMMENT:
                    matchesComment = true;
                    break;
                case Type.ELEMENT:
                    addElementArc(arc);
                    break;
                case Type.ATTRIBUTE:
                    addAttributeArc(arc);
                    break;
                }
            }
        }

        private void addAnyNodeArc(PathMapArc arc) {
            if (arc.getAxis() == AxisInfo.ATTRIBUTE) {
                addAttributeArc(arc);
                return;
            }
            addElementArc(arc);
            addAttributeArc(arc);
            matchesText = true;
            matchesComment = true;
        }

        private void addAttributeArc(PathMapArc arc) {
            if (attributeArcs == null) {
                attributeArcs = new LinkedList<PathMapArc>();
            }
            attributeArcs.add(arc);
        }

        private void addElementArc(PathMapArc arc) {
            if (elementArcs == null) {
                elementArcs = new LinkedList<PathMapArc>();
            }
            elementArcs.add(arc);
        }
    }

    private boolean closed;
    private LinkedList<MatchContext> matchContext = new LinkedList<MatchContext>();
    private boolean logTrace = LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.TRACE);

    public PathMapFilter(PathMapRoot root, Receiver receiver) {
        super(receiver);
        MatchContext mc = new MatchContext();
        mc.bulidContext(root);
        matchContext.add(mc);
    }

    @Override
    public void startElement(NodeName elemName, SchemaType typeCode,
            Location locationId, int properties) throws XPathException {
        MatchContext mc = matchContext.getLast();
        MatchContext newContext = new MatchContext();
        if (mc.elementArcs != null) {
            for (PathMapArc arc : mc.elementArcs) {
                NodeTest test = arc.getNodeTest();
                if (test == null || test.matches(Type.ELEMENT, elemName, typeCode)) {
                    newContext.bulidContext(arc.getTarget());
                    newContext.matchedElement = true;
                    if (arc.getTarget().isAtomized() && arc.getTarget().getArcs().length == 0) {
                        //we must expect the text as there are no further arcs
                        newContext.matchesText = true;
                    }
                }
                if (arc.getAxis() == AxisInfo.DESCENDANT || arc.getAxis() == AxisInfo.DESCENDANT_OR_SELF) {
                    newContext.processArc(arc);
                }
            }
        }
        matchContext.add(newContext);
        if (newContext.matchedElement) {
            super.startElement(elemName, typeCode, locationId, properties);
        } else if (logTrace) {
            LogManager.logTrace(LogConstants.CTX_RUNTIME, "Document projection did not match element", elemName.getURI(), ':', elemName.getLocalPart()); //$NON-NLS-1$
        }
    }

    @Override
    public void attribute(NodeName nameCode, SimpleType typeCode,
            CharSequence value, Location locationId, int properties)
            throws XPathException {
        MatchContext mc = matchContext.getLast();
        if (!mc.matchedElement) {
            return;
        }
        if (nameCode.hasURI(javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI)) {
            super.attribute(nameCode, typeCode, value, locationId, properties);
            return;
        }
        if (mc.attributeArcs != null) {
            for (PathMapArc arc : mc.attributeArcs) {
                NodeTest test = arc.getNodeTest();
                if (test == null || test.matches(Type.ATTRIBUTE, nameCode, typeCode)) {
                    super.attribute(nameCode, typeCode, value, locationId, properties);
                    return;
                }
            }
        }
    }

    @Override
    public void characters(CharSequence chars, Location locationId,
            int properties) throws XPathException {
        MatchContext context = matchContext.getLast();
        if (context.matchedElement && context.matchesText) {
            super.characters(chars, locationId, properties);
        }
    }

    @Override
    public void comment(CharSequence chars, Location locationId,
            int properties) throws XPathException {
        MatchContext context = matchContext.getLast();
        if (context.matchedElement && context.matchesComment) {
            super.comment(chars, locationId, properties);
        }
    }

    @Override
    public void processingInstruction(String target,
            CharSequence data, Location locationId, int properties)
            throws XPathException {
        MatchContext context = matchContext.getLast();
        if (context.matchedElement) {
            super.processingInstruction(target, data, locationId, properties);
        }
    }

    @Override
    public void namespace(NamespaceBindingSet namespaceBindings, int properties)
            throws XPathException {
        MatchContext context = matchContext.getLast();
        if (context.matchedElement) {
            super.namespace(namespaceBindings, properties);
        }
    }

    @Override
    public void endElement() throws XPathException {
        MatchContext context = matchContext.removeLast();
        if (context.matchedElement) {
            super.endElement();
        }
    }

    @Override
    public void startContent() throws XPathException {
        MatchContext context = matchContext.getLast();
        if (context.matchedElement) {
            super.startContent();
        }
    }

    @Override
    public void close() throws XPathException {
        if (!closed) {
            super.close();
            closed = true;
        }
    }

}