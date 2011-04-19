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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.EventFilter;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPathExpressionException;

import net.sf.saxon.expr.JPConverter;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.Name11Checker;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.DateTimeValue;
import net.sf.saxon.value.DateValue;
import net.sf.saxon.value.DayTimeDurationValue;
import net.sf.saxon.value.TimeValue;

import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.CharsetUtils;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.WSConnection.Util;


/** 
 * This class contains scalar system functions supporting for XML manipulation.
 * 
 * @since 4.2
 */
public class XMLSystemFunctions {
	
	private static final Charset UTF_32BE = Charset.forName("UTF-32BE"); //$NON-NLS-1$
	private static final Charset UTF_16BE = Charset.forName("UTF-16BE"); //$NON-NLS-1$
	private static final Charset UTF_32LE = Charset.forName("UTF-32LE"); //$NON-NLS-1$
	private static final Charset UTF_16LE = Charset.forName("UTF-16LE"); //$NON-NLS-1$
	private static final Charset UTF_8 = Charset.forName("UTF-8"); //$NON-NLS-1$

	//TODO: this could be done fully streaming without holding the intermediate xml output
	private static final class JsonToXmlContentHandler implements
			ContentHandler {
		private final XMLStreamWriter streamWriter;
		private String currentName;
		private LinkedList<Boolean> inArray = new LinkedList<Boolean>();

		private JsonToXmlContentHandler(String rootName,
				XMLStreamWriter streamWriter) {
			this.streamWriter = streamWriter;
			this.currentName = rootName;
		}

		@Override
		public boolean startObjectEntry(String key)
				throws org.json.simple.parser.ParseException, IOException {
			currentName = key;
			start();
			return true;
		}

		@Override
		public boolean startObject() throws org.json.simple.parser.ParseException,
				IOException {
			if (inArray.peek()) {
				start();
			}
			inArray.push(false);
			return true;
		}

		private void start()
				throws IOException {
			try {
				streamWriter.writeStartElement(escapeName(currentName, true));
			} catch (XMLStreamException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void startJSON() throws org.json.simple.parser.ParseException,
				IOException {
			try {
				streamWriter.writeStartDocument();
			} catch (XMLStreamException e) {
				throw new IOException(e);
			}
			inArray.push(false);
			start();
		}

		@Override
		public boolean startArray() throws org.json.simple.parser.ParseException,
				IOException {
			inArray.push(true);
			return true;
		}

		@Override
		public boolean primitive(Object value)
				throws org.json.simple.parser.ParseException, IOException {
			if (inArray.peek()) {
				start();
			}
			try {
				if (value != null) {
					streamWriter.writeCharacters(value.toString());
				} else {
					streamWriter.writeNamespace("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI); //$NON-NLS-1$
					streamWriter.writeAttribute("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI, "nil", "true"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				}
			} catch (XMLStreamException e) {
				throw new IOException(e);
			}
			if (inArray.peek()) {
				end();
			}
			return true;
		}

		private void end()
				throws IOException {
			try {
				streamWriter.writeEndElement();
			} catch (XMLStreamException e) {
				throw new IOException(e);
			}
		}

		@Override
		public boolean endObjectEntry()
				throws org.json.simple.parser.ParseException, IOException {
			end();
			return true;
		}

		@Override
		public boolean endObject() throws org.json.simple.parser.ParseException,
				IOException {
			inArray.pop();
			if (inArray.peek()) {
				end();
			}
			return true;
		}

		@Override
		public void endJSON() throws org.json.simple.parser.ParseException,
				IOException {
			end();
			try {
				streamWriter.writeEndDocument();
			} catch (XMLStreamException e) {
				throw new IOException(e);
			}
		}

		@Override
		public boolean endArray() throws org.json.simple.parser.ParseException,
				IOException {
			inArray.pop();
			return true;
		}
	}

	private static final String P_OUTPUT_VALIDATE_STRUCTURE = "com.ctc.wstx.outputValidateStructure"; //$NON-NLS-1$
    
	public static ClobType xslTransform(CommandContext context, Object xml, Object styleSheet) throws Exception {
    	Source styleSource = null; 
		Source xmlSource = null;
		try {
			styleSource = convertToSource(styleSheet);
			xmlSource = convertToSource(xml);
			final Source xmlParam = xmlSource;
			TransformerFactory factory = TransformerFactory.newInstance();
            final Transformer transformer = factory.newTransformer(styleSource);
            
			//this creates a non-validated sqlxml - it may not be valid xml/root-less xml
			SQLXMLImpl result = XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
				
				@Override
				public void translate(Writer writer) throws TransformerException {
	                //transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
	                // Feed the resultant I/O stream into the XSLT processor
					transformer.transform(xmlParam, new StreamResult(writer));
				}
			});
			return new ClobType(new ClobImpl(result.getStreamFactory(), -1));
		} finally {
			Util.closeSource(styleSource);
			Util.closeSource(xmlSource);
		}
    }

	public static XMLType xmlForest(final CommandContext context, final Evaluator.NameValuePair[] namespaces, final Evaluator.NameValuePair[] values) throws TeiidComponentException, TeiidProcessingException {
		boolean valueExists = false;
		for (Evaluator.NameValuePair nameValuePair : values) {
			if (nameValuePair.value != null) {
				valueExists = true;
				break;
			}
		}
		if (!valueExists) {
			return null;
		}

		XMLType result = new XMLType(XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
				try {
					XMLOutputFactory factory = getOutputFactory();
					XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
					XMLEventFactory eventFactory = XMLEventFactory.newInstance();
					for (Evaluator.NameValuePair nameValuePair : values) {
						if (nameValuePair.value == null) {
							continue;
						}
						addElement(nameValuePair.name, writer, eventWriter, eventFactory, namespaces, null, Collections.singletonList(nameValuePair.value));
					}
					eventWriter.close();
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} 
			}
		}));
		result.setType(Type.CONTENT);
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
			final Evaluator.NameValuePair<String>[] namespaces, final Evaluator.NameValuePair<?>[] attributes, final List<?> contents) throws TeiidComponentException, TeiidProcessingException {
		XMLType result = new XMLType(XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
				try {
					XMLOutputFactory factory = getOutputFactory();
					XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
					XMLEventFactory eventFactory = XMLEventFactory.newInstance();
					addElement(name, writer, eventWriter, eventFactory, namespaces, attributes, contents);
					eventWriter.close();
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} 
			}

		}));
		result.setType(Type.ELEMENT);
		return result;
	}
	
	private static void addElement(final String name, Writer writer, XMLEventWriter eventWriter, XMLEventFactory eventFactory,
			Evaluator.NameValuePair<String> namespaces[], Evaluator.NameValuePair<?> attributes[], List<?> contents) throws XMLStreamException, IOException, TransformerException {
		eventWriter.add(eventFactory.createStartElement("", null, name)); //$NON-NLS-1$
		if (namespaces != null) {
			for (Evaluator.NameValuePair<String> nameValuePair : namespaces) {
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
			for (Evaluator.NameValuePair<?> nameValuePair : attributes) {
				if (nameValuePair.value != null) {
					eventWriter.add(eventFactory.createAttribute(new QName(nameValuePair.name), convertToAtomicValue(nameValuePair.value).getStringValue()));
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
	
	public static XMLType xmlConcat(CommandContext context, final XMLType xml, final Object... other) throws TeiidProcessingException {
		//determine if there is just a single xml value and return it
		XMLType singleValue = xml;
		XMLType.Type type = null;
		for (Object object : other) {
			if (object != null) {
				if (singleValue != null) {
					type = Type.CONTENT;
					break;
				}
				if (object instanceof XMLType) {
					singleValue = (XMLType)object;
				} else {
					type = Type.CONTENT;
					break;
				}
			}
		}
		if (type == null) {
			return singleValue;
		}
		
		XmlConcat concat = new XmlConcat(context.getBufferManager());
		concat.addValue(xml);
		for (Object object : other) {
			concat.addValue(object);
		}
		return concat.close();
	}
	
	public static class XmlConcat {
		private XMLOutputFactory factory;
		private XMLEventWriter eventWriter;
		private XMLEventFactory eventFactory;
		private Writer writer;
		private FileStoreInputStreamFactory fsisf;
		private FileStore fs;
		
		public XmlConcat(BufferManager bm) throws TeiidProcessingException {
			fs = bm.createFileStore("xml"); //$NON-NLS-1$
			fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
		    writer = fsisf.getWriter();
			factory = getOutputFactory();
			try {
				eventWriter = factory.createXMLEventWriter(writer);
			} catch (XMLStreamException e) {
				fs.remove();
				throw new TeiidProcessingException(e);
			}
			eventFactory = XMLEventFactory.newInstance();
		}
		
		public void addValue(Object object) throws TeiidProcessingException {
			try {
				convertValue(writer, eventWriter, eventFactory, object);
			} catch (IOException e) {
				fs.remove();
				throw new TeiidProcessingException(e);
			} catch (XMLStreamException e) {
				fs.remove();
				throw new TeiidProcessingException(e);
			} catch (TransformerException e) {
				fs.remove();
				throw new TeiidProcessingException(e);
			}
		}
		
		public XMLType close() throws TeiidProcessingException {
			try {
				eventWriter.flush();
				writer.close();
			} catch (XMLStreamException e) {
				fs.remove();
				throw new TeiidProcessingException(e);
			} catch (IOException e) {
				fs.remove();
				throw new TeiidProcessingException(e);
			}
	        XMLType result = new XMLType(new SQLXMLImpl(fsisf));
	        result.setType(Type.CONTENT);
	        return result;
		}
		
	}
	
	private static XMLOutputFactory getOutputFactory() throws FactoryConfigurationError {
		XMLOutputFactory factory = XMLOutputFactory.newInstance();
		if (factory.isPropertySupported(P_OUTPUT_VALIDATE_STRUCTURE)) {
			factory.setProperty(P_OUTPUT_VALIDATE_STRUCTURE, false);
		}
		return factory;
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
	
	public static AtomicValue convertToAtomicValue(Object value) throws TransformerException {
		if (value instanceof java.util.Date) { //special handling for time types
        	java.util.Date d = (java.util.Date)value;
        	DateTimeValue tdv = DateTimeValue.fromJavaDate(d);
        	if (value instanceof Date) {
        		value = new DateValue(tdv.getYear(), tdv.getMonth(), tdv.getDay(), tdv.getTimezoneInMinutes());
        	} else if (value instanceof Time) {
        		value = new TimeValue(tdv.getHour(), tdv.getMinute(), tdv.getSecond(), tdv.getMicrosecond(), tdv.getTimezoneInMinutes());
        	} else if (value instanceof Timestamp) {
        		Timestamp ts = (Timestamp)value;
        		value = tdv.add(DayTimeDurationValue.fromMicroseconds(ts.getNanos() / 1000));
        	}
        	return (AtomicValue)value;
        }
		JPConverter converter = JPConverter.allocate(value.getClass(), null);
		return (AtomicValue)converter.convert(value, null);
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
				String val = convertToAtomicValue(object).getStringValue();
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
		switch(type) {
		case CONTENT:
		case ELEMENT: 
		case PI:
		case COMMENT: {//write the value directly to the writer
			eventWriter.flush();
			char[] buf = new char[1 << 13];
			int read = -1;
			while ((read = r.read(buf)) != -1) {
				writer.write(buf, 0, read);
			}
			break;
		}
		case UNKNOWN:  //assume a document
		case DOCUMENT: //filter the doc declaration
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			if (!(r instanceof BufferedReader)) {
				r = new BufferedReader(r);
			}
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
			XMLEventFactory eventFactory = XMLEventFactory.newInstance();
			char[] buf = new char[1 << 13];
			int read = -1;
			while ((read = r.read(buf)) != -1) {
				eventWriter.add(eventFactory.createCharacters(new String(buf, 0, read)));
			}
			break;
		}
	}
	
	public static XMLType xmlComment(String comment) {
		return new XMLType(new SQLXMLImpl("<!--" + comment + "-->")); //$NON-NLS-1$ //$NON-NLS-2$
	}

    public static Source convertToSource(Object value) throws TeiidProcessingException {
    	if (value == null) {
    		return null;
    	}
    	try {
	    	if (value instanceof SQLXML) {
				return ((SQLXML)value).getSource(null);
	    	}
	    	if (value instanceof Clob) {
	    		return new StreamSource(((Clob)value).getCharacterStream());
	    	}
	    	if (value instanceof Blob) {
	    		return new StreamSource(((Blob)value).getBinaryStream());
	    	}
	    	if (value instanceof String) {
	    		return new StreamSource(new StringReader((String)value));
	    	}
    	} catch (SQLException e) {
			throw new TeiidProcessingException(e);
		}
    	throw new AssertionError("Unknown type"); //$NON-NLS-1$
    }

    public static String xpathValue(Object doc, String xpath) throws XPathException, TeiidProcessingException {
    	Source s = null;
        try {
        	s = convertToSource(doc);
            XPathEvaluator eval = new XPathEvaluator();
            // Wrap the string() function to force a string return             
            XPathExpression expr = eval.createExpression(xpath);
            Object o = expr.evaluateSingle(s);
            
            if(o == null) {
                return null;
            }
            
            // Return string value of node type
            if(o instanceof Item) {
                return ((Item)o).getStringValue();
            }  
            
            // Return string representation of non-node value
            return o.toString();
        } finally {
        	Util.closeSource(s);
        }
    }
    
    /**
     * Validate whether the XPath is a valid XPath.  If not valid, an XPathExpressionException will be thrown.
     * @param xpath An xpath expression, for example: a/b/c/getText()
     * @throws XPathExpressionException 
     * @throws XPathException 
     */
    public static void validateXpath(String xpath) throws XPathException {
        if(xpath == null) { 
            return;
        }
        
        XPathEvaluator eval = new XPathEvaluator();
        eval.createExpression(xpath);
    }
    
    public static String escapeName(String name, boolean fully) {
    	StringBuilder sb = new StringBuilder();
    	char[] chars = name.toCharArray();
    	int i = 0;
    	if (fully && name.regionMatches(true, 0, "xml", 0, 3)) { //$NON-NLS-1$
			sb.append(escapeChar(name.charAt(0)));
			sb.append(chars, 1, 2);
			i = 3;
    	}
    	for (; i < chars.length; i++) {
    		char chr = chars[i];
    		switch (chr) {
    		case ':':
    			if (fully || i == 0) {
    				sb.append(escapeChar(chr));
    				continue;
    			} 
    			break;
    		case '_':
    			if (chars.length > i && chars[i+1] == 'x') {
    				sb.append(escapeChar(chr));
    				continue;
    			}
    			break;
    		default:
    			//TODO: there should be handling for surrogates
    			//      and invalid chars
    			if (i == 0) {
    				if (!Name11Checker.getInstance().isNCNameStartChar(chr)) {
    					sb.append(escapeChar(chr));
    					continue;
    				}
    			} else if (!Name11Checker.getInstance().isNCNameChar(chr)) {
    				sb.append(escapeChar(chr));
    				continue;
    			}
    			break;
    		}
			sb.append(chr);
		}
    	return sb.toString();
    }

	private static String escapeChar(char chr) {
		CharBuffer cb = CharBuffer.allocate(7);
		cb.append("_u");  //$NON-NLS-1$
		CharsetUtils.toHex(cb, (byte)(chr >> 8));
		CharsetUtils.toHex(cb, (byte)chr);
		return cb.append("_").flip().toString();  //$NON-NLS-1$
	}

    public static SQLXML jsonToXml(CommandContext context, final String rootName, final Blob json) throws TeiidComponentException, TeiidProcessingException, SQLException, IOException {
		InputStream is = json.getBinaryStream();
		PushbackInputStream pStream = new PushbackInputStream(is, 4);
		byte[] encoding = new byte[3];
		int read = pStream.read(encoding);
		pStream.unread(encoding, 0, read);
		Charset charset = UTF_8;
		if (read > 2) {
			if (encoding[0] == 0) {
				if (encoding[1] == 0) {
					charset = UTF_32BE; 
				} else {
					charset = UTF_16BE;
				}
			} else if (encoding[1] == 0) {
				if (encoding[2] == 0) {
					charset = UTF_32LE; 
				} else {
					charset = UTF_16LE;
				}
			}
		}
		Reader r = new InputStreamReader(pStream, charset);
		return jsonToXml(context, rootName, r);
    }
	
    public static SQLXML jsonToXml(CommandContext context, final String rootName, final Clob json) throws TeiidComponentException, TeiidProcessingException, SQLException {
		return jsonToXml(context, rootName, json.getCharacterStream());
    }

	private static SQLXML jsonToXml(CommandContext context,
			final String rootName, final Reader r) throws TeiidComponentException,
			TeiidProcessingException {
		XMLType result = new XMLType(XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
		    	try {
			    	JSONParser parser = new JSONParser();
					XMLOutputFactory factory = getOutputFactory();
					final XMLStreamWriter streamWriter = factory.createXMLStreamWriter(writer);
			    	
					parser.parse(r, new JsonToXmlContentHandler(escapeName(rootName, true), streamWriter));
		    		
					streamWriter.flush(); //woodstox needs a flush rather than a close
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} catch (ParseException e) {
					throw new TransformerException(e);
				} finally {
		    		try {
	    				r.close();
		    		} catch (IOException e) {
		    			
		    		}
		    	}
			}
		}));
		result.setType(Type.DOCUMENT);
		return result;
	}

	/**
	 * This method saves the given XML object to the buffer manager's disk process
	 * Documents less than the maxMemorySize will be held directly in memory
	 */
	public static SQLXMLImpl saveToBufferManager(BufferManager bufferMgr, XMLTranslator translator) 
	    throws TeiidComponentException, TeiidProcessingException {        
	    boolean success = false;
	    final FileStore lobBuffer = bufferMgr.createFileStore("xml"); //$NON-NLS-1$
	    FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(lobBuffer, Streamable.ENCODING);
	    try{  
	    	Writer writer = fsisf.getWriter();
	        translator.translate(writer);
	        writer.close();
	        success = true;
	        return new SQLXMLImpl(fsisf);
	    } catch(IOException e) {
	        throw new TeiidComponentException(e);
	    } catch(TransformerException e) {
	        throw new TeiidProcessingException(e);
	    } finally {
	    	if (!success && lobBuffer != null) {
	    		lobBuffer.remove();
	    	}
	    }
	}
    
}
