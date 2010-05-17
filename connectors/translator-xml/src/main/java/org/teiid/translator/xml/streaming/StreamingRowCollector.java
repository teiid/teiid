package org.teiid.translator.xml.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.Nodes;
import nux.xom.xquery.StreamingTransform;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.Document;
import org.xml.sax.XMLReader;



/*
 * StreamingRowCollector builds result rows from a single XML file for List of XPath 
 * like paths.
 */
public class StreamingRowCollector {

	private Map<String, String> namespaces;
	private ArrayList<Object[]> result;
	private XMLReader reader;
	private ElementProcessor elemProcessor;

	public StreamingRowCollector(Map<String, String> namespaces, XMLReader reader, ElementProcessor elemProcessor) {
		this.namespaces = namespaces;
		this.reader = reader;
		this.elemProcessor = elemProcessor;
		this.result = new ArrayList<Object[]>();
	}

	/**
	 * Builds a list of rows from an InputStream
	 * @param xml
	 * @param xPaths
	 * @return
	 * @throws TranslatorException
	 * @throws InvalidPathException
	 */
	public List<Object[]> getElements(Document xml, List<String> xPaths)
			throws TranslatorException, InvalidPathException {
		result.clear();
		StreamingTransform myTransform = new StreamingTransform() {
			public Nodes transform(Element item) {
				if (item != null) {
					parseRow(item);
				}
				return new Nodes();
				// mark current element as subject to garbage collection
			}
		};

		Builder builder = new Builder(reader, false, new StreamingMultiPathFilter(xPaths, namespaces).createNodeFactory(null, myTransform));
		try {
			builder.build(xml.getStream());
		} catch (Exception e) {
			throw new TranslatorException(e);
		}
		elemProcessor.insertResponseId(xml, result);
		return result;
	}

	/**
	 * Create a result row from the element.
	 * @param item
	 */
	private void parseRow(Element item) {
		Object[] row = elemProcessor.process(item);
		if(null != row && !Arrays.asList(row).isEmpty()) {
			this.result.add(row);
		}
	}	
}
