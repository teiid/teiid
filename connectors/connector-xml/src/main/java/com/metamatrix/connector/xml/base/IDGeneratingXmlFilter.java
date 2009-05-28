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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.ConnectorLogger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;


public class IDGeneratingXmlFilter extends XMLFilterImpl
{
    protected static class PathElement
    {
        protected PathElement(String name, int index)
        {
            this.name = name;
            this.index = index;
            predecessorCounts = new HashMap();
        }
    	protected String name;
        protected int index;
        protected Map predecessorCounts; // keys: predecessor sibling names, values: count of times it has appeared
    }
    
    public IDGeneratingXmlFilter(String documentId, ConnectorLogger logger)
    {
        this.documentId = documentId;
        this.logger = logger;
    }

    public static final String MM_ID_ATTR_NAME_BY_PATH = "com.metamatrix.xml.xpathpart";
    public static final String MM_ID_ATTR_NAME_BY_INDEX = "com.metamatrix.xml.xpathpart.byindex";
    public static final String MM_ID_ATTR_NAME = MM_ID_ATTR_NAME_BY_PATH;
    public static final String MM_ID_ATTR_VALUE_PREFIX = "";

    ConnectorLogger logger;
    String documentId;

    // This way of doing things seems like cheating, but it will produce the desired results.
    // Only problem: the IDs are completely opaque and meaningless.
    int index = 0;
    
    // This way is nicer
    List path; // a list of PathElement objects

    @Override
	public void startDocument() throws SAXException
    {
        path = new ArrayList();
        PathElement newPathElement = new PathElement(documentId, -1);
        path.add(newPathElement);

        super.startDocument();
    }

    private String getIdFromIndex()
    {
        String retval = MM_ID_ATTR_VALUE_PREFIX + documentId + "/" + index;
        ++index;
        return retval;
    }

    private String getIdFromPath(String qName)
    {
        StringBuffer retval = new StringBuffer();

        Object oParentPath = path.get(path.size() - 1);
        PathElement parentPath = (PathElement)oParentPath;
        Map predecessorCounts = parentPath.predecessorCounts;
        Object oCount = predecessorCounts.get(qName);
        Integer count = (Integer)oCount;
        int index;
        if (count == null) {
            index = 0;
        }
        else {
            index = count.intValue() + 1;
        }
        predecessorCounts.put(qName, Integer.valueOf(index));
        
        PathElement newPathElement = new PathElement(qName, index);
        path.add(newPathElement);

        boolean first = true;
        for (Iterator iter = path.iterator(); iter.hasNext() ; ) {
        	Object o = iter.next();
            PathElement pathElement = (PathElement)o;
            if (first) {
            	first = false;
            }
            else {
            	retval.append('/');
            }
            retval.append(pathElement.name);
            if (pathElement.index >= 0) {
            	retval.append('[');
            	retval.append(pathElement.index);
            	retval.append(']');
            }
        }
        
        return retval.toString();
    }
    
    @Override
	public void startElement(String namespaceURI, String localName,
            String qName, Attributes atts) throws SAXException
    {
        String indexValue = getIdFromIndex();
        String pathValue = getIdFromPath(qName);
        Attributes newAtts = addAttributes(atts, indexValue, pathValue);
        super.startElement(namespaceURI, localName, qName, newAtts);
    }

	protected Attributes addAttributes(Attributes atts, String indexValue, String pathValue)
    {
		AttributesImpl newAtts = new AttributesImpl(atts);
        newAtts.addAttribute("", MM_ID_ATTR_NAME, MM_ID_ATTR_NAME, "CDATA", pathValue);
        newAtts.addAttribute("", MM_ID_ATTR_NAME_BY_INDEX, MM_ID_ATTR_NAME_BY_INDEX, "CDATA", indexValue);
		return newAtts;
	}

	@Override
	public void endElement(String namespaceURI, String localName, String qName)
            throws SAXException {
        path.remove(path.size() - 1);
        super.endElement(namespaceURI, localName, qName);
    }
}