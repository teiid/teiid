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

package com.metamatrix.query.function.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.Calendar;

import javax.xml.namespace.QName;
import javax.xml.stream.EventFilter;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.trans.XPathException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.types.XMLTranslator;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.common.types.XMLType.Type;
import com.metamatrix.internal.core.xml.XPathHelper;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.FunctionMethods;
import com.metamatrix.query.processor.xml.XMLUtil;
import com.metamatrix.query.util.CommandContext;

/** 
 * This class contains scalar system functions supporting for XML manipulation.
 * 
 * @since 4.2
 */
public class XMLSystemFunctions {
	
	public static class NameValuePair {
		String name;
		Object value;
		
		public NameValuePair(String name, Object value) {
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

    public static Object xpathValue(Object document, Object xpathStr) throws FunctionExecutionException {
        Reader stream = null;
        
        if (document instanceof SQLXML) {
            try {
                stream = ((SQLXML)document).getCharacterStream();
            } catch (SQLException e) {
                throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.xpathvalue_takes_only_string", document.getClass().getName())); //$NON-NLS-1$
            }
        } else if(document instanceof String) {
            stream = new StringReader((String)document);
        } else {
            throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.xpathvalue_takes_only_string", document.getClass().getName())); //$NON-NLS-1$
        }
        
        try {
            return XPathHelper.getSingleMatchAsString(stream, (String) xpathStr);
        } catch(IOException e) {
            throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.wrap_exception", xpathStr, e.getMessage())); //$NON-NLS-1$
        } catch(XPathException e) {
            throw new FunctionExecutionException(QueryPlugin.Util.getString("XMLSystemFunctions.wrap_exception", xpathStr, e.getMessage())); //$NON-NLS-1$
        }
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
		
	public static XMLType xmlForest(final CommandContext context, final NameValuePair[] values) throws MetaMatrixComponentException, MetaMatrixProcessingException {
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
						addElement(nameValuePair.name, writer, eventWriter, eventFactory, nameValuePair.value);
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
	 * @throws MetaMatrixComponentException
	 * @throws MetaMatrixProcessingException 
	 */
	public static XMLType xmlElement(CommandContext context, final String name, 
			final Object... contents) throws MetaMatrixComponentException, MetaMatrixProcessingException {
		XMLType result = new XMLType(XMLUtil.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {
			
			@Override
			public void translate(Writer writer) throws TransformerException,
					IOException {
				try {
					XMLOutputFactory factory = XMLOutputFactory.newInstance();
					XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
					XMLEventFactory eventFactory = XMLEventFactory.newInstance();
					addElement(name, writer, eventWriter, eventFactory, contents);
				} catch (XMLStreamException e) {
					throw new TransformerException(e);
				} 
			}

		}, context.getStreamingBatchSize()));
		result.setType(Type.FRAGMENT);
		return result;
	}
	
	private static void addElement(final String name, Writer writer, XMLEventWriter eventWriter, XMLEventFactory eventFactory,
			Object... contents) throws XMLStreamException, IOException, TransformerException {
		eventWriter.add(eventFactory.createStartElement("", null, name)); //$NON-NLS-1$
		int start = 0;
		if (contents.length > 0 && contents[0] instanceof NameValuePair[]) {
			for (NameValuePair nameValuePair : (NameValuePair[])contents[0]) {
				if (nameValuePair.value != null) {
					eventWriter.add(eventFactory.createAttribute(new QName(nameValuePair.name), getStringValue(nameValuePair.value)));
				}
			}
			start = 1;
		}
		//add empty chars to close the start tag
		eventWriter.add(eventFactory.createCharacters("")); //$NON-NLS-1$ 
		for (int i = start; i < contents.length; i++) {
			convertValue(writer, eventWriter, eventFactory, contents[i]);
		}
		eventWriter.add(eventFactory.createEndElement("", null, name)); //$NON-NLS-1$
	}
	
	public static XMLType xmlConcat(CommandContext context, final XMLType xml, final Object... other) throws MetaMatrixComponentException, MetaMatrixProcessingException {
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
		if (object instanceof XMLType) {
			XMLType xml = (XMLType)object;
			Reader r = null;
			try {
				r = xml.getCharacterStream();
				if (!(r instanceof BufferedReader)) {
					r = new BufferedReader(r);
				}
				switch(xml.getType()) {
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
				}
			} catch (SQLException e) {
				throw new IOException(e);
			} finally {
				if (r != null) {
					r.close();
				}
			}
		} else {
			String val = getStringValue(object);
			eventWriter.add(eventFactory.createCharacters(val));
		}
		//TODO: blob - with base64 encoding
		//TODO: full clob?
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
	
}
