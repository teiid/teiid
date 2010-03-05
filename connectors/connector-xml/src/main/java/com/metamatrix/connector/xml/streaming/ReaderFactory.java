package com.metamatrix.connector.xml.streaming;

import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;
import org.xml.sax.helpers.XMLReaderFactory;

import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.base.IDGeneratingXmlFilter;


public class ReaderFactory {
	
	static public XMLReader getXMLReader(SAXFilterProvider filterProvider) throws SAXException {
		XMLReader reader = XMLReaderFactory.createXMLReader();
		
		if (filterProvider != null) {
			XMLFilterImpl[] filters = filterProvider.getExtendedFilters();
			for(int i = 0; i < filters.length; i++) {
				XMLFilter filter = filters[i];
				filter.setParent(reader);
				reader = filter;
			}
		}
		
		IDGeneratingXmlFilter filter = new IDGeneratingXmlFilter("");
		filter.setParent(reader);
		return filter;
	}

}
