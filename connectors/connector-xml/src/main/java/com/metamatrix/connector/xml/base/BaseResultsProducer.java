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

package com.metamatrix.connector.xml.base;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.jaxen.JaxenException;
import org.jaxen.JaxenHandler;
import org.jaxen.XPath;
import org.jaxen.expr.XPathExpr;
import org.jaxen.jdom.JDOMXPath;
import org.jaxen.saxpath.Axis;
import org.jaxen.saxpath.SAXPathException;
import org.jaxen.saxpath.XPathReader;
import org.jaxen.saxpath.helpers.XPathReaderFactory;
import org.jdom.Attribute;
import org.jdom.CDATA;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.Text;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.http.Messages;
/**
 * Converts a Response into an List containing results based upon the metadata
 * from the ExecutionInfo. Elements of the List are List that each contain all
 * of the results for a single column.
 * 
 * @author Jdoyle
 * 
 */
public class BaseResultsProducer {

    ExecutionInfo info;

    private IDocumentCache statementCache;

    private ConnectorLogger logger;

    public BaseResultsProducer(IDocumentCache statementCache,
            ConnectorLogger logger) {
        this.statementCache = statementCache;
        this.logger = logger;
    }

    public List getResult(ExecutionInfo info, Response response)
            throws ConnectorException {
        this.info = info;
        if (info.getTableXPath() == null
                || info.getTableXPath().trim().length() == 0) {
            throw new ConnectorException(Messages
                    .getString("Executor.name.in.source.required")); //$NON-NLS-1$
        }

        XMLDocument[] docs = response.getDocuments();

        if (docs == null || docs.length == 0) {
            throw new ConnectorException(Messages
                    .getString("Executor.xml.docs.not.found")); //$NON-NLS-1$
        }

        // initialize the result set
        ArrayList resultData = new ArrayList(info.getRequestedColumns().size());
        for (int i = 0; i < info.getRequestedColumns().size(); i++) {
            ArrayList columnData = new ArrayList();
            resultData.add(columnData);
        }

        XMLDocument xmlDocument = null;

        // result processing loop
        for (int docCtr = 0; docCtr < docs.length; docCtr++) {

            xmlDocument = docs[docCtr];
            List groupList = generateGroupList(xmlDocument);

            // first iterate to find each record
            processRecords(groupList, resultData, xmlDocument, response);
        }

        return resultData;
    }

    private List generateGroupList(XMLDocument xmlDocument)
            throws ConnectorException {
        XPath groupExpression = null;
        try {
            String groupString = info.getTableXPath();
            groupString = relativizeAbsoluteXpath(groupString);
            groupExpression = createXPath(groupString);

        } catch (SAXPathException sex) {
            throw new ConnectorException(sex, Messages
                    .getString("Executor.saxpath.error.on.group")); //$NON-NLS-1$
        }

        List groupList = null;
        try {
            // http://xml.sys-con.com/read/40294.htm
            // claims that the xpath objects are completely thread safe.
            // If it turns out they are not, we'll have to synchronize
            // here. See also getColumnData.
            Object contextRoot = xmlDocument.getContextRoot();
            groupList = groupExpression.selectNodes(contextRoot);
        } catch (JaxenException jex) {
            throw new ConnectorException(jex, Messages
                    .getString("Executor.jaxen.error.on.selectnodes")); //$NON-NLS-1$
        }
        return groupList;
    }

    /**
     * @param xmlDocument
     */
    private void processRecords(List groupList, ArrayList resultData,
            XMLDocument xmlDocument, Response response)
            throws ConnectorException {
        Iterator groupNodeIterator = groupList.iterator();
        List requestedColumns = info.getRequestedColumns();

        // Create a list of xpaths so we don't have to keep calculating them
        // (or looking them up)
        ArrayList xpaths = generateXPaths(requestedColumns);

        while (groupNodeIterator.hasNext()) {
            Object groupNode = groupNodeIterator.next();

            Iterator requestedXPathIterator = requestedColumns.iterator();
            Iterator actualXpathIterator = xpaths.iterator();
            int colNum = 0;
            // then iterate to find each column value for that record

            while (requestedXPathIterator.hasNext()) {
                ArrayList columnData = (ArrayList) resultData.get(colNum);

                OutputXPathDesc columnDesc = (OutputXPathDesc) requestedXPathIterator
                        .next();
                XPath xpath = (XPath) actualXpathIterator.next();
                LargeOrSmallString data;
                if (columnDesc.isResponseId()) {
                    String r = response.getResponseId();
                    data = LargeOrSmallString.createSmallString(r);
                } else {
                    data = getColumnData(xmlDocument, columnDesc, groupNode,
                            xpath);
                }
                columnData.add(data);
                colNum++;
            }
        }
    }

    private String relativizeAbsoluteXpath(String xpath)
            throws ConnectorException {
        String retval;
        if (xpath.indexOf('|') != -1 && xpath.indexOf('(') == -1) {
            // We are forcing compound XPaths to have parents, first reason is
            // that we
            // should never produce them in the importer, second reson is that
            // it makes
            // this function easier to fix under our current time constraints.
            throw new ConnectorException(Messages
                    .getString("Executor.unsupported.compound.xpath"));//$NON-NLS-1$ 
        } else if (xpath.equals("/")) {//$NON-NLS-1$ 
            retval = ".";//$NON-NLS-1$ 
        } else if (xpath.startsWith("/") && !(xpath.startsWith("//"))) {//$NON-NLS-1$ //$NON-NLS-2$ 
            retval = xpath.substring(1);
        } else if (xpath.startsWith("(")) {//$NON-NLS-1$ 
            xpath = xpath.replaceAll("\\(/", "("); // change (/ to ( //$NON-NLS-1$ //$NON-NLS-2$  
            xpath = xpath.replaceAll("\\|/", "|"); // change |/ to | //$NON-NLS-1$ //$NON-NLS-2$
            retval = xpath;
        } else {
            retval = xpath;
        }
        return retval;
    }

    private ArrayList generateXPaths(List requestedColumns)
            throws ConnectorException {
        ArrayList xpaths = new ArrayList();
        OutputXPathDesc xPathDesc = null;
        try {
            for (Iterator iter = requestedColumns.iterator(); iter.hasNext();) {
                xPathDesc = (OutputXPathDesc) iter.next();
                XPath xpath = null;
                if (!xPathDesc.isResponseId()) {
                    String xpathString = xPathDesc.getXPath();
                    if (xpathString != null) {
                        xpathString = relativizeAbsoluteXpath(xpathString);
                        xpath = createXPath(xpathString);
                    }
                }
                xpaths.add(xpath);
            }
        } catch (SAXPathException sex) {
            String msgRaw = Messages
                    .getString("Executor.saxpath.error.on.column"); //$NON-NLS-1$
            String msg = MessageFormat.format(msgRaw, new Object[] { xPathDesc
                    .getColumnName() });
            throw new ConnectorException(sex, msg); //$NON-NLS-1$
        }
        return xpaths;
    }

    private String getNamespaces(Map namespacePairs) throws ConnectorException {

        String namespacePrefixes = getNamespacePrefixes();
        return getNamespaces(namespacePairs, namespacePrefixes);
    }

    private String getNamespacePrefixes() {
        String namespacePrefixes = info.getOtherProperties().getProperty(
                RequestResponseDocumentProducer.NAMESPACE_PREFIX_PROPERTY_NAME);
        return namespacePrefixes;
    }

    private String getNamespaces(Map namespacePairs, String namespacePrefixes)
            throws ConnectorException {
        if (namespacePrefixes == null || namespacePrefixes.trim().length() == 0) {
            return null;
        }
        String prefix = null;
        String uri = null;
        try {
            // Perhaps this will be a performance hog? In which case we should
            // do
            // the parsing here instead of using a full XML parse
            String xml = "<e " + namespacePrefixes + "/>"; //$NON-NLS-1$ //$NON-NLS-2$
            Reader reader = new StringReader(xml);
            SAXBuilder builder = new SAXBuilder();
            Document domDoc = builder.build(reader);
            Element elem = domDoc.getRootElement();
            List namespaces = elem.getAdditionalNamespaces();
            for (Iterator iter = namespaces.iterator(); iter.hasNext();) {
                Object o = iter.next();
                Namespace namespace = (Namespace) o;
                prefix = namespace.getPrefix();
                uri = namespace.getURI();
                namespacePairs.put(prefix, uri);
            }
            Namespace defaultNamespace = elem.getNamespace();
            String defaultUri = defaultNamespace.getURI();
            if (defaultUri != null && defaultUri.equals("")) { //$NON-NLS-1$
                defaultUri = null;
            }
            return defaultUri;
        } catch (JDOMException e) {
            String rawMsg = Messages
                    .getString("Executor.jaxen.error.on.namespace.pairs"); //$NON-NLS-1$
            Object[] objs = new Object[2];
            objs[0] = prefix;
            objs[1] = uri;
            String msg = MessageFormat.format(rawMsg, objs);
            throw new ConnectorException(e, msg);

        } catch (IOException e) {
            throw new ConnectorException(e, e.getMessage());
        }
    }

    private XPath createXPath(String xpathString) throws SAXPathException,
            ConnectorException {
        String namespacePrefixes = getNamespacePrefixes();
        String lookup = namespacePrefixes == null ? xpathString : xpathString
                + namespacePrefixes;
        Object o = statementCache.fetchObject(lookup, null);
        XPath expression;
        if (o instanceof XPath) {
            expression = (XPath) o;
        } else {
            String defaultNamespace;
            Map otherNamespaces = new HashMap();
            defaultNamespace = getNamespaces(otherNamespaces);

            if (defaultNamespace == null) {
                expression = new JDOMXPath(xpathString);
            } else {
                XPathReader reader = XPathReaderFactory.createReader();
                DefaultNamespaceWorkaroundHander handler = new DefaultNamespaceWorkaroundHander(
                        otherNamespaces);
                reader.setXPathHandler(handler);
                reader.parse(xpathString);
                XPathExpr xpath = handler.getXPathExpr();
                String xpathWithPrefixes = xpath.getText();
                logger.logInfo("Rewriting XPath expression due to use of default namespace.");
                logger.logInfo("Was: \"" + xpathString + "\". ");
                logger.logInfo("Now: \"" + xpathWithPrefixes + "\". " + "");
                logger.logInfo("and adding namespace declaration xmlns:"
                        + handler.getNonceNamespacePrefix() + "="
                        + defaultNamespace);
                expression = new JDOMXPath(xpathWithPrefixes);
                otherNamespaces.put(handler.getNonceNamespacePrefix(),
                        defaultNamespace);
            }
            addNamespacePairs(expression, otherNamespaces);
            statementCache.addToCache(lookup, expression, lookup.length(), null);
        }
        return expression;
    }

    private LargeOrSmallString getColumnData(XMLDocument xmlDocument,
            OutputXPathDesc xPath, Object groupNode, XPath colExpression)
            throws ConnectorException {

        // For no good reason, xPath.getCurrentValue returns an Object. It is
        // known that
        // these objects must be String's
        // (in fact before the large string support was added, they got
        // explicitly cast to String's later, in XMLExecutionImpl.nextBatch).
        // However, for several steps down, the data is typed as Object

        // I will cast to String here. At some point later strong typing can be
        // added to the methods I mentioned and their dependents.
        String data = null;
        if (xPath.getXPath() == null) {
            // project the parameter value
            data = (String) xPath.getCurrentValue();
        } else {
            Object node = null;
            try {
                // http://xml.sys-con.com/read/40294.htm
                // claims that the xpath objects are completely thread safe.
                // If it turns out they are not, we'll have to synchronize
                // here. See also generateGroupList.
                node = colExpression.selectSingleNode(groupNode);
            } catch (JaxenException jex) {
                String msgRaw = Messages
                        .getString("Executor.jaxen.error.on.selectsinglenode"); //$NON-NLS-1$
                String msg = MessageFormat.format(msgRaw, new Object[] { xPath
                        .getColumnName() });
                throw new ConnectorException(jex, msg); //$NON-NLS-1$
            }
            data = getNodeValue(node);
        }

        LargeOrSmallString los = LargeTextExtractingXmlFilter
                .stringOrValueReference(data, xmlDocument);
        return los;
    }

    private static String getDistinctPrefixes(Map otherNamespaces) {
        String distinctPrefix = null;
        // create a simple string that is not any of the keys in the map
        for (int i = 0; distinctPrefix == null; ++i) {
            // represent the number as a 'base 26' string with a = 0 and z = 25
            int j = i;
            String s = ""; //$NON-NLS-1$
            do {
                char digit = (char) (j % 26);
                char c = (char) ('a' + digit);
                s = s + c;
                j = j / 26;
            } while (j > 0);
            Object found = otherNamespaces.get(s);
            if (found == null) {
                distinctPrefix = s;
            }
        }
        return distinctPrefix;
    }

    private static class DefaultNamespaceWorkaroundHander extends JaxenHandler {
        private String nonceNamespacePrefix;

        private DefaultNamespaceWorkaroundHander(Map otherNamespaces) {
            super();
            nonceNamespacePrefix = getDistinctPrefixes(otherNamespaces);
        }

        public void startNameStep(int axis, String prefix, String localName)
                throws JaxenException {
            String prefixToUse;
            if (prefix == null || prefix.equals("")) { //$NON-NLS-1$
                switch (axis) {
                // Elements
                case Axis.ANCESTOR:
                case Axis.ANCESTOR_OR_SELF:
                case Axis.CHILD:
                case Axis.DESCENDANT:
                case Axis.DESCENDANT_OR_SELF:
                case Axis.FOLLOWING:
                case Axis.FOLLOWING_SIBLING:
                case Axis.PARENT:
                case Axis.PRECEDING:
                case Axis.PRECEDING_SIBLING:
                case Axis.SELF:
                    prefixToUse = nonceNamespacePrefix;
                    break;
                // Things other than elements
                case Axis.ATTRIBUTE:
                case Axis.INVALID_AXIS:
                case Axis.NAMESPACE:
                default:
                    prefixToUse = prefix;
                    break;
                }
            } else {
                prefixToUse = prefix;
            }
            super.startNameStep(axis, prefixToUse, localName);
        }

        public String getNonceNamespacePrefix() {
            return nonceNamespacePrefix;
        }
    }

    private String getNodeValue(Object node) {
        String value = null;
        if (node != null) {
            if (node instanceof Attribute) {
                value = ((Attribute) node).getValue();
            } else if (node instanceof Text) {
                value = ((Text) node).getValue();
            } else if (node instanceof CDATA) {
                value = ((CDATA) node).getText();
            } else if (node instanceof Element) {
                Element elem = (Element) node;
                XMLOutputter out = new XMLOutputter();
                value = out.outputString(elem);
            } else {
                value = node.toString();
            }
        }
        return value;
    }

    private void addNamespacePairs(XPath expression, Map namespacePairs)
            throws ConnectorException {
        try {
            for (Iterator iter = namespacePairs.keySet().iterator(); iter
                    .hasNext();) {
                String prefix = (String) iter.next();
                String uri = (String) namespacePairs.get(prefix);
                expression.addNamespace(prefix, uri);
            }
        } catch (JaxenException e) {
            throw new ConnectorException(e, Messages
                    .getString("Executor.jaxen.error.on.namespace.pairs")); //$NON-NLS-1$
        }
    }

    public static List combineResults(List responseResultList,
            List allResultsList) {
        if (allResultsList.size() == 0) {
            allResultsList = responseResultList;
        } else {
            for (int x = 0; x < allResultsList.size(); x++) {
                Object oResultList;
                oResultList = allResultsList.get(x);
                List allResultsField = (List) oResultList;
                oResultList = responseResultList.get(x);
                List loopResultsField = (List) oResultList;
                allResultsField.addAll(loopResultsField);
            }
        }
        return allResultsList;
    }

}
