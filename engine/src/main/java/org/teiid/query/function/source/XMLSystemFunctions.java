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

package org.teiid.query.function.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.CharBuffer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.EventFilter;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.processor.xml.XMLUtil;
import org.teiid.query.util.CommandContext;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


/** 
 * This class contains scalar system functions supporting for XML manipulation.
 * 
 * @since 4.2
 */
public class XMLSystemFunctions {
	
	public static class NameValuePair<T> {
		String name;
		T value;
		
		public NameValuePair(String name, T value) {
			this.name = name;
			this.value = value;
		}
	}
	
    //YEAR 0 in the server timezone. used to determine negative years
    public static long YEAR_ZERO;
    static String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"; //$NON-NLS-1$
    
    static String TIMESTAMP_MICROZEROS = "000000000"; //$NON-NLS-1$
    
    static {
        Calendar cal = Calendar.getInstance();
    
        for (int i = 0; i <= Calendar.MILLISECOND; i++) {
            cal.set(i, 0);
        }
        YEAR_ZERO = cal.getTimeInMillis();
    }
    
	public static ClobType xslTransform(CommandContext context, String xmlResults, String styleSheet) throws Exception {
		return xslTransform(context, DataTypeManager.transformValue(xmlResults, DataTypeManager.DefaultDataClasses.XML), DataTypeManager.transformValue(styleSheet, DataTypeManager.DefaultDataClasses.XML));
	}

	public static ClobType xslTransform(CommandContext context, String xmlResults, XMLType styleSheet) throws Exception {
		return xslTransform(context, DataTypeManager.transformValue(xmlResults, DataTypeManager.DefaultDataClasses.XML), styleSheet);
	}

	public static ClobType xslTransform(CommandContext context, XMLType xmlResults, String styleSheet) throws Exception {
		return xslTransform(context, xmlResults, DataTypeManager.transformValue(styleSheet, DataTypeManager.DefaultDataClasses.XML));
	}

	public static ClobType xslTransform(CommandContext context, XMLType xmlResults, XMLType styleSheet) throws Exception {
    	Reader styleSheetReader = styleSheet.getCharacterStream();
    	final Source styleSource = new StreamSource(styleSheetReader);
		Reader reader = xmlResults.getCharacterStream();
		final Source xmlSource = new StreamSource(reader);
		try {
			//this creates a non-validated sqlxml - it may not be valid xml/root-less xml
			SQLXML result = XMLUtil.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
				
				@Override
				public void translate(Writer writer) throws TransformerException {
	                TransformerFactory factory = TransformerFactory.newInstance();
	                Transformer transformer = factory.newTransformer(styleSource);
	                //transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
	                // Feed the resultant I/O stream into the XSLT processor
					transformer.transform(xmlSource, new StreamResult(writer));
				}
			}, Streamable.STREAMING_BATCH_SIZE_IN_BYTES);
			return DataTypeManager.transformValue(new XMLType(result), DataTypeManager.DefaultDataClasses.CLOB);
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
			}
			try {
				styleSheetReader.close();
			} catch (IOException e) {
			}
		}
    }
		
	public static XMLType xmlForest(final CommandContext context, final NameValuePair[] namespaces, final NameValuePair[] values) throws TeiidComponentException, TeiidProcessingException {
		boolean valueExists = false;
		for (NameValuePair nameValuePair : values) {
			if (nameValuePair.value != null) {
				valueExists = true;
				break;
			}
		}
		if (!valueExists) {
			return null;
		}

		XMLType result = new XMLType(XMLUtil.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
				try {
					XMLOutputFactory factory = XMLOutputFactory.newInstance();
					XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
					XMLEventFactory eventFactory = XMLEventFactory.newInstance();
					for (NameValuePair nameValuePair : values) {
						if (nameValuePair.value == null) {
							continue;
						}
						addElement(nameValuePair.name, writer, eventWriter, eventFactory, namespaces, null, Collections.singletonList(nameValuePair.value));
					}
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} 
			}
		}, context.getStreamingBatchSize()));
		result.setType(Type.SIBLINGS);
		return result;
	}
	
	/**
	 * Basic support for xmlelement.  namespaces are not yet supported.
	 * @param context
	 * @param name
	 * @param contents
	 * @return
	 * @throws TeiidComponentException
	 * @throws TeiidProcessingException 
	 */
	public static XMLType xmlElement(CommandContext context, final String name, 
			final NameValuePair<String>[] namespaces, final NameValuePair<?>[] attributes, final List<?> contents) throws TeiidComponentException, TeiidProcessingException {
		XMLType result = new XMLType(XMLUtil.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
				try {
					XMLOutputFactory factory = XMLOutputFactory.newInstance();
					XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
					XMLEventFactory eventFactory = XMLEventFactory.newInstance();
					addElement(name, writer, eventWriter, eventFactory, namespaces, attributes, contents);
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} 
			}

		}, context.getStreamingBatchSize()));
		result.setType(Type.FRAGMENT);
		return result;
	}
	
	private static void addElement(final String name, Writer writer, XMLEventWriter eventWriter, XMLEventFactory eventFactory,
			NameValuePair<String> namespaces[], NameValuePair<?> attributes[], List<?> contents) throws XMLStreamException, IOException, TransformerException {
		eventWriter.add(eventFactory.createStartElement("", null, name)); //$NON-NLS-1$
		if (namespaces != null) {
			for (NameValuePair<String> nameValuePair : namespaces) {
				if (nameValuePair.name == null) {
					if (nameValuePair.value == null) {
						eventWriter.add(eventFactory.createNamespace(XMLConstants.NULL_NS_URI));
					} else {
						eventWriter.add(eventFactory.createNamespace(nameValuePair.value));
					} 
				} else {
					eventWriter.add(eventFactory.createNamespace(nameValuePair.name, nameValuePair.value));
				}
			}
		}
		if (attributes != null) {
			for (NameValuePair<?> nameValuePair : attributes) {
				if (nameValuePair.value != null) {
					eventWriter.add(eventFactory.createAttribute(new QName(nameValuePair.name), getStringValue(nameValuePair.value)));
				}
			}
		}
		//add empty chars to close the start tag
		eventWriter.add(eventFactory.createCharacters("")); //$NON-NLS-1$ 
		for (Object object : contents) {
			convertValue(writer, eventWriter, eventFactory, object);
		}
		eventWriter.add(eventFactory.createEndElement("", null, name)); //$NON-NLS-1$
	}
	
	public static XMLType xmlConcat(CommandContext context, final XMLType xml, final Object... other) throws TeiidComponentException, TeiidProcessingException {
		XMLType result = new XMLType(XMLUtil.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
				try {
					XMLOutputFactory factory = XMLOutputFactory.newInstance();
					XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
					XMLEventFactory eventFactory = XMLEventFactory.newInstance();
					convertValue(writer, eventWriter, eventFactory, xml);
					for (Object object : other) {
						convertValue(writer, eventWriter, eventFactory, object);
					}
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} 
			}
		}, context.getStreamingBatchSize()));
		result.setType(Type.SIBLINGS);
		return result;
	}
	
	public static XMLType xmlPi(String name) {
		return xmlPi(name, ""); //$NON-NLS-1$
	}
	
	public static XMLType xmlPi(String name, String content) {
		int start = 0;
		char[] chars = content.toCharArray();
		while (start < chars.length && chars[start] == ' ') {
			start++;
		}
		XMLType result = new XMLType(new SQLXMLImpl("<?" + name + " " + content.substring(start) + "?>")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		result.setType(Type.PI);
		return result;
	}
	
	static String getStringValue(Object object) throws TransformerException {
		if (object instanceof Timestamp) {
			try {
				return timestampToDateTime((Timestamp)object);
			} catch (FunctionExecutionException e) {
				throw new TransformerException(e);
			}
		}
		try {
			return DataTypeManager.transformValue(object, DataTypeManager.DefaultDataClasses.STRING);
		} catch (TransformationException e) {
			throw new TransformerException(e);
		}
	}
	
	static void convertValue(Writer writer, XMLEventWriter eventWriter, XMLEventFactory eventFactory, Object object) throws IOException,
			FactoryConfigurationError, XMLStreamException,
			TransformerException {
		if (object == null) {
			return;
		}
		Reader r = null;
		try {
			if (object instanceof XMLType) {
				XMLType xml = (XMLType)object;
				r = xml.getCharacterStream();
				Type type = xml.getType();
				convertReader(writer, eventWriter, r, type);
			} else if (object instanceof Clob) {
				Clob clob = (Clob)object;
				r = clob.getCharacterStream();
				convertReader(writer, eventWriter, r, Type.TEXT);
			} else {
				String val = getStringValue(object);
				eventWriter.add(eventFactory.createCharacters(val));
			}
		} catch (SQLException e) {
			throw new IOException(e);
		} finally {
			if (r != null) {
				r.close();
			}
		}
		//TODO: blob - with base64 encoding
	}

	private static void convertReader(Writer writer,
			XMLEventWriter eventWriter, Reader r, Type type)
			throws XMLStreamException, IOException, FactoryConfigurationError {
		if (!(r instanceof BufferedReader)) {
			r = new BufferedReader(r);
		}
		switch(type) {
		case FRAGMENT:
		case SIBLINGS: 
		case PI:
		case COMMENT: //write the value directly to the writer
			eventWriter.flush();
			int chr = -1;
			while ((chr = r.read()) != -1) {
				writer.write(chr);
			}
			break;
		case UNKNOWN:  //assume a document
		case DOCUMENT: //filter the doc declaration
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			XMLEventReader eventReader = inputFactory.createXMLEventReader(r);
			eventReader = inputFactory.createFilteredReader(eventReader, new EventFilter() {
				@Override
				public boolean accept(XMLEvent event) {
					return !event.isStartDocument() && !event.isEndDocument();
				}
			});
			eventWriter.add(eventReader);
			break;
		case TEXT:
			CharBuffer buffer = CharBuffer.allocate(1 << 13);
			XMLEventFactory eventFactory = XMLEventFactory.newInstance();
			while (r.read(buffer) != -1) {
				eventWriter.add(eventFactory.createCharacters(new String(buffer.array(), 0, buffer.position())));
				buffer.reset();
			}
			break;
		}
	}
	
	public static XMLType xmlComment(String comment) {
		return new XMLType(new SQLXMLImpl("<!--" + comment + "-->")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/**
	 * Formats a timestamp to an xs:dateTime.  This uses DATETIME_FORMAT
	 * with a trailing string for nanoseconds (without right zeros). 
	 */
	public static String timestampToDateTime(Timestamp time) throws FunctionExecutionException {
	    String result = FunctionMethods.format(time, DATETIME_FORMAT);
	    int nanos = time.getNanos();
	    if (nanos == 0) {
	        return result;
	    }
	    
	    StringBuffer resultBuffer = new StringBuffer();
	    boolean first = true;
	    int i = 0;
	    for (; i < 9 && nanos > 0; i++) {
	        int digit = nanos%10;
	        if (first) {
	            if (digit > 0) {
	                resultBuffer.insert(0, digit);
	                first = false;
	            }
	        } else {
	            resultBuffer.insert(0, digit);
	        }
	        nanos /= 10;
	    }
	    if (i < 9) {
	        resultBuffer.insert(0, TIMESTAMP_MICROZEROS.substring(i));
	    }
	    resultBuffer.insert(0, "."); //$NON-NLS-1$
	    resultBuffer.insert(0, result);
	    if (time.getTime() < YEAR_ZERO) {
	        resultBuffer.insert(0, "-"); //$NON-NLS-1$
	    }
	    return resultBuffer.toString();
	    
	}

    public static String xpathValue(XMLType document, String xpathStr, String namespaces) throws IOException, XPathExpressionException, SQLException, FunctionExecutionException {
    	Reader stream = document.getCharacterStream();
        return xpathValue(stream, xpathStr, namespaces);
    }
	
    public static String xpathValue(String document, String xpathStr, String namespaces) throws IOException, XPathExpressionException, FunctionExecutionException {
    	return xpathValue(new StringReader(document), xpathStr, namespaces);	
    }
    
    public static String xpathValue(String document, String xpathStr) throws IOException, XPathExpressionException, FunctionExecutionException {
    	return xpathValue(document, xpathStr, null);
    }
    
    public static String xpathValue(XMLType document, String xpathStr) throws IOException, SQLException, XPathExpressionException, FunctionExecutionException {
    	return xpathValue(document, xpathStr, null);
    }

    public static XMLType xpathQuery(CommandContext context, String document, String xpathStr) throws IOException, TeiidComponentException, TeiidProcessingException, XPathExpressionException {
    	return xpathQuery(context, document, xpathStr, null);
    }
    
    public static XMLType xpathQuery(CommandContext context, XMLType document, String xpathStr) throws IOException, SQLException, XPathExpressionException, TeiidComponentException, TeiidProcessingException {
    	return xpathQuery(context, document, xpathStr, null);
    }
    
    public static XMLType xpathQuery(CommandContext context, String document, String xpathStr, String namespaces) throws IOException, TeiidComponentException, TeiidProcessingException, XPathExpressionException {
    	Reader stream = new StringReader(document);
    	return xpathQuery(context, xpathStr, stream, namespaces);
    }
    
    public static XMLType xpathQuery(CommandContext context, XMLType document, String xpathStr, String namespaces) throws IOException, SQLException, XPathExpressionException, TeiidComponentException, TeiidProcessingException {
    	Reader stream = ((SQLXML)document).getCharacterStream();
    	return xpathQuery(context, xpathStr, stream, namespaces);
    }

	private static XMLType xpathQuery(CommandContext context, String xpathStr,
			Reader stream, String namespaces) throws XPathExpressionException,
			TeiidComponentException, TeiidProcessingException,
			IOException {
		try {
            XPathFactory xpathFactory = XPathFactory.newInstance();
        	XPath xp = xpathFactory.newXPath();
        	NamespaceContext nc = getNamespaces(namespaces);
        	if (nc != null) {
        		xp.setNamespaceContext(nc);
        	}
        	final NodeList nodes = (NodeList)xp.evaluate(xpathStr, new InputSource(stream), XPathConstants.NODESET);
        	if (nodes.getLength() == 0) {
        		return null;
        	}
        	Type type = nodes.getLength() > 1 ? Type.SIBLINGS : Type.FRAGMENT;
        	for (int i = 0; i < nodes.getLength(); i++) {
        		Node node = nodes.item(i);
        		if (node.getNodeType() == Node.TEXT_NODE) {
        			type = Type.TEXT;
        		}
        	}
        	SQLXML sqlXml = XMLUtil.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
				
				@Override
				public void translate(Writer writer) throws TransformerException {
	                TransformerFactory factory = TransformerFactory.newInstance();
	                Transformer transformer = factory.newTransformer();
	                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
	                for (int i = 0; i < nodes.getLength(); i++) {
	            		Node node = nodes.item(i);
						transformer.transform(new DOMSource(node), new StreamResult(writer));
	            	}
				}
			}, Streamable.STREAMING_BATCH_SIZE_IN_BYTES);
        	XMLType result = new XMLType(sqlXml);
        	result.setType(type);
        	return result;
        } finally {
    		stream.close();
        }
	}  
	
    public static String xpathValue(Reader documentReader, String xpath, String namespaces) throws IOException, XPathExpressionException, FunctionExecutionException {        
        try {
            XPathFactory xpathFactory = XPathFactory.newInstance();
        	XPath xp = xpathFactory.newXPath();
        	NamespaceContext nc = getNamespaces(namespaces);
        	if (nc != null) {
        		xp.setNamespaceContext(nc);
        	}
        	Node node = (Node)xp.evaluate(xpath, new InputSource(documentReader), XPathConstants.NODE);
        	if (node == null) {
        		return null;
        	}
        	return node.getTextContent();
        } finally {
            // Always close the reader
            documentReader.close();
        }
    }
    
    public static NamespaceContext getNamespaces(String namespaces) throws FunctionExecutionException {
    	if (namespaces == null) {
    		return null;
    	}
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		try {
			XMLStreamReader eventReader = inputFactory.createXMLStreamReader(new StringReader("<x " + namespaces + " />")); //$NON-NLS-1$ //$NON-NLS-2$
			eventReader.next();
        	return eventReader.getNamespaceContext();
		} catch (XMLStreamException e) {
			throw new FunctionExecutionException(e, QueryPlugin.Util.getString("XMLSystemFunctions.invalid_namespaces", namespaces)); //$NON-NLS-1$
		}
    }

    /**
     * Validate whether the XPath is a valid XPath.  If not valid, an XPathExpressionException will be thrown.
     * @param xpath An xpath expression, for example: a/b/c/getText()
     * @throws XPathExpressionException 
     */
    public static void validateXpath(String xpath) throws XPathExpressionException {
        if(xpath == null) { 
            return;
        }
        
        XPathFactory factory = XPathFactory.newInstance();
        XPath xp = factory.newXPath();
        xp.compile(xpath);
    }
	
}
