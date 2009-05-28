package com.metamatrix.connector.xml.streaming;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;
import org.xml.sax.helpers.XMLReaderFactory;

import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.base.IDGeneratingXmlFilter;


public class ReaderFactory {
	
	static public XMLReader getXMLReader(XMLConnectorState state) throws ParserConfigurationException, SAXException {
		XMLReader reader = XMLReaderFactory.createXMLReader();
		SAXFilterProvider filterProvider = state.getSAXFilterProvider();
		XMLFilterImpl[] filters = filterProvider.getExtendedFilters(state.getLogger());
		for(int i = 0; i < filters.length; i++) {
			XMLFilter filter = filters[i];
			filter.setParent(reader);
			reader = filter;
		}
		IDGeneratingXmlFilter filter = new IDGeneratingXmlFilter("", state.getLogger());
		filter.setParent(reader);
		return filter;
	}

}
