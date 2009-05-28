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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.IllegalNameException;
import org.jdom.Namespace;
import org.jdom.output.XMLOutputter;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.soap.SOAPDocBuilder;

public class DocumentBuilder {

    public static final String PARM_INPUT_XPATH_TABLE_PROPERTY_NAME = "XPathRootForInput"; //$NON-NLS-1$

    private Map m_namespaceMap;

    private boolean m_useTypeAttributes;

    public DocumentBuilder() {
        m_namespaceMap = new HashMap();
        m_useTypeAttributes = false;
    }

    private Map getNamespaces() {
        return m_namespaceMap;
    }

    public String buildDocumentString(List contents, String topLevelXPath,
            String nsDecl) throws ConnectorException {
        Document doc = buildDocument(contents, topLevelXPath, nsDecl);
        return outputDocToString(doc);
    }

    public Document buildDocument(List contents, String topLevelXPath,
            String nsDecl) throws ConnectorException {
        Document doc = null;
        setNamespaces(nsDecl);
        // assemble table-level elements
        org.jdom.Element curElement = null;
        try {
            curElement = makeElement(curElement, topLevelXPath, false);
        } catch(IllegalNameException ex) {
            ConnectorException ce = new ConnectorException(topLevelXPath + " is not a valid XPath for the root request node." +
                    "Change the Request table " + PARM_INPUT_XPATH_TABLE_PROPERTY_NAME);
            throw ce;
        }
        if (curElement == null) {
            throw new ConnectorException(Messages
                    .getString("HTTPExecutor.root.element.required")); //$NON-NLS-1$
        }
        doc = makeTableLevelElements(curElement);

        // loop through each parameter
        Iterator iter = contents.iterator();
        processParameters(iter, curElement);

        // add all of the namespaces
        Element nsHolder = doc.getRootElement();
        addAllNamespaces(nsHolder);

        return doc;
    }

    private void addAllNamespaces(Element nsHolder) {
        Map namespaceMap = getNamespaces();
        Iterator nsIter = namespaceMap.values().iterator();
        while (nsIter.hasNext()) {
            nsHolder.addNamespaceDeclaration((Namespace) nsIter.next());
        }
    }

    private Document makeTableLevelElements(Element curElement)
            throws ConnectorException {

        // this walks up from the current node to the root, so the root can be
        // assigned to a document
        Element walkupElement = curElement;
        Element parentElement;
        // why is this failing?
        // could be due to incompatibilities with core jdom version
        // and the one the connector uses
        while ((parentElement = (Element) walkupElement.getParent()) != null) {
            walkupElement = parentElement;
        }
        Document document = new Document(walkupElement);
        return document;
    }

    private void setNamespaces(String nsDecl) throws ConnectorException {
        if (nsDecl != null && nsDecl.trim().length() > 0) {
            String[] nsPairs = nsDecl.trim().replace('\"', '\0').replace('\'',
                    '\0').split("xmlns"); //$NON-NLS-1$
            // the first entry will be blank since the string starts with the
            // delimiter
            for (int i = 1; i < nsPairs.length; i++) {
                // remove the ':'
                if (nsPairs[i].startsWith(":"))
                    nsPairs[i] = nsPairs[i].substring(1);
                String[] nsSplit = nsPairs[i].split("="); //$NON-NLS-1$
                if (nsSplit.length != 2) {
                    throw new ConnectorException(
                            Messages
                                    .getString("DocumentBuilder.could.not.parse.namespaces")); //$NON-NLS-1$
                }
                Namespace ns = Namespace.getNamespace(nsSplit[0].trim(),
                        nsSplit[1].trim());
                m_namespaceMap.put(nsSplit[0], ns);
            }
        }
    }

    private Element makeElement(Element colElement, String inputXPath,
            boolean dupLastElement) throws ConnectorException {
        if (inputXPath == null || inputXPath.trim().length() == 0) {
            return colElement;
        }
        Element tempElement = colElement;
        String tempXPath = inputXPath.trim();
        tempElement = visitXPath(colElement, tempXPath, dupLastElement);
        return tempElement;
    }

    private Element visitXPath(Element elem, String tempXPath,
            boolean dupLastElement) throws ConnectorException {
        // loop through, searching for path seperators
        int startParmIndx = 0;
        int endParmIndx = tempXPath.indexOf("/", startParmIndx); //$NON-NLS-1$
        int endOfTempXPath = tempXPath.length() - 1;
        while (endParmIndx >= 0 && endParmIndx < endOfTempXPath) {
            elem = setElement(elem, tempXPath, startParmIndx, endParmIndx);
            startParmIndx = endParmIndx + 1;
            endParmIndx = checkForIndexEnd(tempXPath, startParmIndx);
        }
        String finalXpath = getFinalXPath(tempXPath, startParmIndx, endParmIndx);
        if (finalXpath != null) {
            elem = addOneElement(elem, finalXpath, dupLastElement);
        }
        return elem;

    }

    private int checkForIndexEnd(String tempXPath, int startParmIndx) {
        int endParmIndx;
        if (startParmIndx < tempXPath.length() - 1) {
            endParmIndx = tempXPath.indexOf("/", startParmIndx); //$NON-NLS-1$
        } else {
            endParmIndx = -1;
        }
        return endParmIndx;
    }

    private String getFinalXPath(String tempXPath, int startParmIndx,
            int endParmIndx) {
        String finalXpath = null;
        if (endParmIndx > 0) {
            finalXpath = tempXPath.substring(startParmIndx, endParmIndx);
        } else {
            if (startParmIndx <= tempXPath.length() - 1) {
                finalXpath = tempXPath.substring(startParmIndx);
            }
        }
        return finalXpath;
    }

    private Element setElement(Element element, String tempXPath,
            int startParmIndx, int endParmIndx) throws ConnectorException {
        Element tempElement = element;
        if (endParmIndx > 0) {
            String tempXP = tempXPath.substring(startParmIndx, endParmIndx);
            if (tempXP.indexOf("..") >= 0) { //$NON-NLS-1$
                throw new ConnectorException(Messages
                        .getString("HTTPExecutor.dot.notation.not.allowed")); //$NON-NLS-1$
            }

            if (tempXP != null && tempXP.trim().length() > 0) {
                tempElement = addOneElement(tempElement, tempXP, false);
            }
        }
        return tempElement;
    }

    private void processParameters(Iterator iter, Element curElement)
            throws ConnectorException {

        CriteriaDesc parmCriteria = null;
        while (iter.hasNext()) {
            try {
                parmCriteria = (CriteriaDesc) iter.next();
                boolean isAutoIncrement = parmCriteria.isAutoIncrement();
                createParameterXML(curElement, parmCriteria, isAutoIncrement);
            } catch (Exception ex) {
                throw new ConnectorException(Messages
                        .getString("HTTPExecutor.error.building.column") //$NON-NLS-1$
                        + parmCriteria.getColumnName() + ": " + ex.toString()); //$NON-NLS-1$
            }
        }

    }

    private void createParameterXML(Element curElement,
            CriteriaDesc parmCriteria, boolean isAutoIncrement)
            throws ConnectorException {
        if (parmCriteria.isParentAttribute()) {
            // add parameter as attribute of connector
            curElement.setAttribute(parmCriteria.getInputXpath(), parmCriteria
                    .getCurrentIndexValue());
        } else if (parmCriteria.isUnlimited()
                && !parmCriteria.isEnumeratedAttribute()) {
            // add parameter as mutliple elements
            createMultipleElementXML(curElement, parmCriteria, isAutoIncrement);
        } else {
            createSingleElementXML(curElement, parmCriteria, isAutoIncrement);
        }

    }

    private void createSingleElementXML(Element curElement,
            CriteriaDesc parmCriteria, boolean isAutoIncrement)
            throws ConnectorException {
        org.jdom.Element colElement = null;
        if (parmCriteria.isDataInAttribute()) {
            // add parameter as an element and an attribute in the
            // element
            colElement = makeElement(curElement, parmCriteria.getInputXpath(),
                    false);
            String attName = parmCriteria.getDataAttributeName();
            String namePart = getNamePart(attName);
            String nsPart = getNamespacePart(attName);
            Namespace attNS = Namespace.NO_NAMESPACE;
            if (nsPart != null) {
                attNS = (Namespace) m_namespaceMap.get(nsPart);
                colElement.setAttribute(namePart, parmCriteria
                        .getCurrentIndexValue(), attNS);
            } else {
                colElement.setAttribute(attName, parmCriteria
                        .getCurrentIndexValue());
            }
        } else {
            // add parameter as a new element
            colElement = makeElement(curElement, parmCriteria.getInputXpath(),
                    false);
            colElement.addContent(parmCriteria.getCurrentIndexValue());
        }
        if (isAutoIncrement) {
            String subscrValue = "[0]"; //$NON-NLS-1$
            colElement.setAttribute("SUBSCR", subscrValue); //$NON-NLS-1$
        }

        if (useTypeAttributes()) {
            // if there is a native type in the model use it, otherwise
            // if 5.0 or later, derive from the data type
            String xsdType = parmCriteria.getElement().getNativeType();
            if (xsdType == null) {
                // attempt use the data type
                // this method call does not exist before 5.0 so we have to
                // check for it before we call it.
                try {
                    Method method = parmCriteria.getElement().getClass()
                            .getMethod("getModeledType", new Class[] {});
                    String type = parmCriteria.getElement().getModeledType();
                    String nsPart = type.substring(0, type.indexOf('#'));
                    String namePart = type.substring(type.indexOf('#') + 1);
                    Iterator nsIter = getNamespaces().values().iterator();
                    String prefix = null;
                    while (nsIter.hasNext()) {
                        Namespace ns = (Namespace) nsIter.next();
                        if (ns.getURI().equals(nsPart))
                            prefix = ns.getPrefix();
                    }
                    if (prefix == null) {
                        int prefixInt = 0;
                        while (getNamespaces().get("ns" + prefixInt) != null) {
                            ++prefixInt;
                        }
                        prefix = "ns" + prefixInt;
                        Namespace newNS = Namespace
                                .getNamespace(prefix, nsPart);
                        getNamespaces().put(prefix, newNS);
                    }
                    xsdType = prefix + ":" + namePart;
                } catch (NoSuchMethodException nme) {
                    throw new ConnectorException(
                            "column "
                                    + colElement.getName()
                                    + Messages
                                            .getString("DocumentBuilder.encoding.type.required"));
                }
            }
            final String xsiType = "type"; //$NON-NLS-1$
            final String xsiNS = "http://www.w3.org/1999/XMLSchema-instance"; //$NON-NLS-1$
            final String xsiPrefix = "xsi"; //$NON-NLS-1$
            Attribute type = new Attribute(xsiType, xsdType, Namespace
                    .getNamespace(xsiPrefix, xsiNS));
            colElement.setAttribute(type);
        }
    }

    /**
     * 
     */
    private void createMultipleElementXML(Element curElement,
            CriteriaDesc parmCriteria, boolean isAutoIncrement)
            throws ConnectorException {
        if(parmCriteria.isSOAPArrayElement()) {
            createSOAPArrayElement(curElement, parmCriteria);
        } else {
            parmCriteria.resetIndex();
            boolean moreParms = true;
            boolean isDataInAttribute = parmCriteria.isDataInAttribute();
            String inputXpath = parmCriteria.getInputXpath();
            for (int x = 0; moreParms; x++) {
                org.jdom.Element colElement = makeElement(curElement, inputXpath,
                        true);
                if (isAutoIncrement) {
                    String subscrValue = "[" + Integer.toString(x) //$NON-NLS-1$
                            + "]"; //$NON-NLS-1$
                    colElement.setAttribute("SUBSCR", subscrValue); //$NON-NLS-1$
                }
                if (isDataInAttribute) {
                    colElement.setAttribute(parmCriteria.getDataAttributeName(),
                            parmCriteria.getCurrentIndexValue());
                } else {
                    colElement.setText(parmCriteria.getCurrentIndexValue());
                }
                moreParms = parmCriteria.incrementIndex();
            }
        }
    }

    private void createSOAPArrayElement(Element curElement, CriteriaDesc parmCriteria) throws ConnectorException {
        
        /*
         * loop over the values in the criteriaDesc and create <item> nodes containing them
         * 
         * get the imformation for the soapArray element out of the criteriaDesc's
         * nativeType property and create the attributes including the number of items.
         * 
         * 
         */
        String inputXpath = parmCriteria.getInputXpath();
        curElement = visitXPath(curElement, inputXpath, false);
        
        parmCriteria.resetIndex();
        boolean moreParms = true;
        
        String nativeTypeInfo = parmCriteria.getNativeType();
        StringTokenizer tokenizer = new StringTokenizer(nativeTypeInfo, ";");
        String xsdTypeString = tokenizer.nextToken();
        
        int start,end;
        start = xsdTypeString.indexOf("\"");
        end = xsdTypeString.indexOf("\"", start +1);
        String xsdTypeValue = xsdTypeString.substring(start +1, end);
        
        String arrayType = tokenizer.nextToken();
        start = arrayType.indexOf("\"");
        end = arrayType.indexOf("\"", start +1);
        String arrayTypeValue = arrayType.substring(start +1, end);

        end = arrayType.indexOf(":");
        String soapEncodingPrefix = arrayType.substring(0, end);
        
        String arrayNamespace = tokenizer.nextToken();
        start = arrayNamespace.indexOf(":");
        end = arrayNamespace.indexOf("=", start +1);
        start = arrayNamespace.indexOf("\"");
        arrayTypeValue = arrayTypeValue + "[" + parmCriteria.getNumberOfValues() + "]";
        
        
        if(!parmCriteria.isSimpleSoapElement()) {
            Element arrayElement = curElement.getParentElement();
            //See if we have already added the attributes to the array from
            //an earlier array element.
            if(null == arrayElement.getAttribute("type", Namespace.XML_NAMESPACE)) {
                arrayElement.setAttribute("type", xsdTypeValue, Namespace.XML_NAMESPACE);
                Namespace soapEncodingNamespace = Namespace.getNamespace(soapEncodingPrefix, SOAPDocBuilder.encodingStyleUrl);
                arrayElement.setAttribute("arrayType", arrayTypeValue, soapEncodingNamespace);
            }
            curElement.detach();
            org.jdom.Element itemElement = arrayElement.getChild("item");
            if(null == itemElement) {
                itemElement = makeElement(arrayElement, "item", true);
            }
            itemElement.addContent(curElement);
            curElement.setText(parmCriteria.getCurrentIndexValue());
            
        } else {
            //See if we have already added the attributes to the array from
            //an earlier array element.
            if(null == curElement.getAttribute("type", Namespace.XML_NAMESPACE)) {
                curElement.setAttribute("type", xsdTypeValue, Namespace.XML_NAMESPACE);
                Namespace soapEncodingNamespace = Namespace.getNamespace(soapEncodingPrefix, SOAPDocBuilder.encodingStyleUrl);
                curElement.setAttribute("arrayType", arrayTypeValue, soapEncodingNamespace);
            }
            
            for (int x = 0; moreParms; x++) {
                org.jdom.Element itemElement = makeElement(curElement, "item", true);
                itemElement.setText(parmCriteria.getCurrentIndexValue());
                moreParms = parmCriteria.incrementIndex();
            }
        }
    }

    private Element addOneElement(org.jdom.Element colElement,
            String inputXPath, boolean allowDup) throws ConnectorException {

        // Create element namespace if needed and use in the get Child and
        // Element ctor
        String tempXPath = inputXPath.trim();
        String elementName = getElementName(tempXPath);
        String nsPart = getNamespacePart(elementName);
        String namePart = getNamePart(elementName);
        Namespace elemNS = Namespace.NO_NAMESPACE;
        if (nsPart != null) {
            elemNS = (Namespace) getNamespaces().get(nsPart);
        }

        org.jdom.Element childElement = null;
        if (colElement != null && !allowDup) {
            childElement = colElement.getChild(namePart, elemNS);
        }

        // element does not already exist, create it
        if (childElement == null) {
            childElement = new Element(namePart, elemNS);
            // add new element to connector
            if (colElement != null) {
                colElement.addContent(childElement);
            }
        }

        addAttributes(childElement, tempXPath);
        return childElement;
    }

    private String getNamespacePart(String elemName) {
        int colonIndex = elemName.indexOf(':');
        if (colonIndex < 0) {
            return null;
        }
        return elemName.substring(0, colonIndex);
    }

    private String getNamePart(String elemName) {
        int colonIndex = elemName.indexOf(':');
        if (colonIndex < 0) {
            return elemName;
        }
        return elemName.substring(colonIndex + 1);
    }

    private void addAttributes(Element childElement, String tempXPath)
            throws ConnectorException {
        // add attribute definitions
        int startAttIndx = tempXPath.indexOf("["); //$NON-NLS-1$
        while (startAttIndx > 0) {
            int equalIndx = tempXPath.indexOf("=", startAttIndx); //$NON-NLS-1$
            int endAttIndx = tempXPath.indexOf("]", startAttIndx); //$NON-NLS-1$
            if (equalIndx > 0 && equalIndx < endAttIndx) {
                String attName = tempXPath.substring(startAttIndx + 1,
                        equalIndx);
                String namePart = getNamePart(attName);
                String nsPart = getNamespacePart(attName);
                Namespace attNS = Namespace.NO_NAMESPACE;
                if (nsPart != null) {
                    attNS = (Namespace) m_namespaceMap.get(nsPart);
                }
                String attValue = tempXPath
                        .substring(equalIndx + 1, endAttIndx);
                childElement.setAttribute(namePart, attValue, attNS);
            } else {
                throw new ConnectorException(Messages
                        .getString("HTTPExecutor.bad.attribute.def")); //$NON-NLS-1$
            }
            startAttIndx = tempXPath.indexOf("[", endAttIndx); //$NON-NLS-1$
        }
    }

    private String getElementName(String tempXPath) {
        String elementName;
        int startAttIndx = tempXPath.indexOf("["); //$NON-NLS-1$
        if (startAttIndx > 0) {
            elementName = tempXPath.substring(0, startAttIndx).trim();
        } else {
            elementName = tempXPath;
        }
        return elementName;
    }

    public static String outputDocToString(Document doc) {
        XMLOutputter out = new XMLOutputter();
        return out.outputString(doc).trim();
    }

    public void setUseTypeAttributes(boolean m_useTypeAttributes) {
        this.m_useTypeAttributes = m_useTypeAttributes;
    }

    private boolean useTypeAttributes() {
        return m_useTypeAttributes;
    }
}
