package org.teiid.translator.xml.streaming;

import org.teiid.translator.xml.IDGeneratingXmlFilter;
import org.teiid.translator.xml.SAXFilterProvider;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;
import org.xml.sax.helpers.XMLReaderFactory;



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
