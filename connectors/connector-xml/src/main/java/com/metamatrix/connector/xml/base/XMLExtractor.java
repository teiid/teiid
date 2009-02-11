/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.SAXFilterProvider;

/**
 * created by JChoate on Jul 12, 2005
 *
 */
public class XMLExtractor {

    /**
     * 
     */
    public XMLExtractor(int largeTextThreshold,
                        boolean generateIds,
                        boolean logDocument,
                        File cacheFolder,
                        ConnectorLogger connectorLogger)
    {
        super();
        this.largeTextThreshold = largeTextThreshold;
        this.generateIds = generateIds;
        this.connectorLogger = connectorLogger;
        m_cacheFolder = cacheFolder;
        m_logDocument = logDocument;
    }

    private int largeTextThreshold;
    private boolean generateIds;
    private ConnectorLogger connectorLogger;
    private File m_cacheFolder;
    private boolean m_logDocument;

    private static final int AVERAGE_ELEMENT_FILE_SIZE = 20;
    private static final int AVERAGE_ELEMENT_FILE_MEMORY_RATIO = 5;

    public DocumentInfo createDocumentFromStream(InputStream responseBody, String documentDistinguishingId, SAXFilterProvider provider) throws ConnectorException
    {
    	
        CountingInputStream stream = new CountingInputStream(responseBody);
        SAXBuilder builder;
        
        //TODO: call the factory to get the filters
        XMLFilterImpl [] extendedFilters = provider.getExtendedFilters(connectorLogger);

        LargeTextExtractingXmlFilter largeTextFilter = null;
        
        List filters = new ArrayList();

        //add extended filters
        for(int i = 0; i < extendedFilters.length; i++) {
        	filters.add(extendedFilters[i]);
        }
        
        if (largeTextThreshold > 0) {
            largeTextFilter = new LargeTextExtractingXmlFilter(largeTextThreshold, m_cacheFolder, connectorLogger);
            filters.add(largeTextFilter);
        }
        if (generateIds) {
            IDGeneratingXmlFilter idGeneratingFilter = new IDGeneratingXmlFilter(documentDistinguishingId, connectorLogger);
            filters.add(idGeneratingFilter);
        }            


        if (filters.size() > 0)
        {
            // See SAXBuilderFix for why we need it instead of plain SAXBuilder
            builder = new SAXBuilderFix();
        }
        else {
            builder = new SAXBuilder();
        }

        for (Iterator iter = filters.iterator(); iter.hasNext(); ) {
        	Object o = iter.next();
            XMLFilter filter = (XMLFilter)o;
            builder.setXMLFilter(filter);
        }
        
        Document domDoc = null;
        try {
            domDoc = builder.build(stream);
        } catch (Exception de) {
            throw new ConnectorException(de);
        }

        FileLifeManager[] externalFiles;
        if (largeTextFilter != null) {
            externalFiles = largeTextFilter.getFiles();
        }
        else {
            externalFiles = new FileLifeManager[0];
        }
        long fileCacheSize = stream.getSize();
        return createDocument(domDoc, fileCacheSize, externalFiles);
    }

    public DocumentInfo createDocumentFromJDOM(Document domDoc, String documentDistinguishingId) throws ConnectorException
    {
        // We are going to use the IDGeneratingXmlFilter to add the unique ID attribute to
        // every element. Note that this is not processing the elements as they are parsed by
        // SAX; we have to iterate through the JDOM objects.
        // The power of interfaces -- kewl.

        SelfAddingIDGeneratingXmlFilter idGeneratingXmlFilter = new SelfAddingIDGeneratingXmlFilter(documentDistinguishingId, connectorLogger);
        
        try {
        	idGeneratingXmlFilter.startDocument();
            List children = domDoc.getContent();
            traverse(children, idGeneratingXmlFilter);
            idGeneratingXmlFilter.endDocument();
        }
        catch (SAXException e) {
        	throw new ConnectorException(e);
        }
        int fileCacheSize = idGeneratingXmlFilter.elemCount * AVERAGE_ELEMENT_FILE_SIZE;
        
        return createDocument(domDoc, fileCacheSize, new FileLifeManager[0]);
    }

    private void traverse(List content, IDGeneratingXmlFilter idGeneratingXmlFilter) throws SAXException
    {
        for (Iterator iter = content.iterator(); iter.hasNext(); ) {
        	Object o = iter.next();
            if (!(o instanceof Element)) {
            	continue;
            }
            Element elem = (Element)o;
            String namespace = elem.getNamespaceURI();
            String name = elem.getName();
            String qname = elem.getQualifiedName();
            org.xml.sax.Attributes attributes = new SAXAttributesWrapper(elem);
            idGeneratingXmlFilter.startElement(namespace, name, qname, attributes);
            List children = elem.getContent();
            traverse(children, idGeneratingXmlFilter);
            idGeneratingXmlFilter.endElement(namespace, name, qname);
        }
	}

    private class SAXAttributesWrapper extends AttributesImpl
    {
        Element elem;
        SAXAttributesWrapper(Element elem)
        {
            super();
            this.elem = elem;
            // We don't care what the existing attributes are; we just want to add the new ones
        }
    }
    
    private class SelfAddingIDGeneratingXmlFilter extends IDGeneratingXmlFilter
    {
        int elemCount = 0;
        SelfAddingIDGeneratingXmlFilter(String documentDistinguishingId, ConnectorLogger connectorLogger)
        {
        	super(documentDistinguishingId, connectorLogger);
        }

        public void startElement(String namespaceURI, String localName,
                String qName, Attributes atts) throws SAXException
        {
            super.startElement(namespaceURI, localName, qName, atts);
        	elemCount++;
        }
        
        protected Attributes addAttributes(Attributes atts, String indexValue, String pathValue)
        {
            SAXAttributesWrapper wrappedAttributes = (SAXAttributesWrapper)atts;
            wrappedAttributes.elem.setAttribute(MM_ID_ATTR_NAME, pathValue);
            wrappedAttributes.elem.setAttribute(MM_ID_ATTR_NAME_BY_INDEX, indexValue);
            return atts;
        }
    }
    
	private DocumentInfo createDocument(Document domDoc, long fileCacheSize, FileLifeManager[] externalFiles) throws ConnectorException
    {
        long memoryCacheSize = fileCacheSize;
        try {
            for (int iFile = 0; iFile < externalFiles.length; ++iFile) {
                memoryCacheSize -= externalFiles[iFile].getLength();
            }
        } catch (IOException e) {
            throw new ConnectorException(e);
        }
        if(memoryCacheSize < 100) {
        	memoryCacheSize = 100;
        }
        //its really hard to esitmate the actual size of the XMLDocument object
        //so we'll arbitrarily multiply its size by AVERAGE_ELEMENT_FILE_MEMORY_RATIO
        memoryCacheSize *= AVERAGE_ELEMENT_FILE_MEMORY_RATIO;

        return new DocumentInfo(domDoc, externalFiles, (int)memoryCacheSize, fileCacheSize);
    }

	// This class is a bug fix override for SAXBuilder. SAXBuilder
    // fails to properly insert the filter in the chain
    private final class SAXBuilderFix extends SAXBuilder
    {
        private final List filters;
        private SAXBuilderFix() {
            super();
            filters = new ArrayList();
        }

        protected XMLReader createParser() throws JDOMException {
            // SAXBuilder fails to properly insert the filters into the
            // chain. We know exactly what the filters are (the filters list),
            // so we can insert them ourselves directly.

            XMLReader reader = super.createParser();
            for (Iterator iter = filters.iterator(); iter.hasNext(); ) {
                Object o = iter.next();
                XMLFilter filter = (XMLFilter)o;
                filter.setParent(reader);
                reader = filter;
            }

            return reader;
        }
        
        // Since SAXBuilder fails to properly insert the filter into the
        // chain, we need to swallow this method. createParser knows about
        // the filter through other means.
        public void setXMLFilter(XMLFilter xmlFilter)
        {
            filters.add(xmlFilter);
        }

    }
}
