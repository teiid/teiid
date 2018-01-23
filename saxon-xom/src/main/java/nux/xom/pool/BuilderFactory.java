/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package nux.xom.pool;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import nu.xom.Builder;
import nu.xom.XMLException;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * Creates and returns new <code>Builder</code> objects that validate against
 * W3C XML Schemas, DTDs, RELAX NG, Schematron or do not validate at all
 * (thread-safe). Features and properties of the underlying {@link XMLReader}
 * can be specified. A node factory can be specified by overriding the protected
 * <code>newBuilder</code> method.
 * <p>
 * This implementation is thread-safe.
 * <p>
 * W3C XML Schema support (method <code>createW3CBuilder</code>) requires
 * Xerces (either external for JDK 1.4, or as packaged inside JDK 1.5); the
 * other methods do not require Xerces.
 * <p>
 * RELAX NG support (method <code>createMSVBuilder</code>) requires MSV. <a
 * target="_blank" href="http://msv.dev.java.net/">Sun Multi-Schema XML
 * Validator (MSV)</a> can validate XML documents against several kinds of XML
 * schemata, including <a target="_blank" href="http://www.relaxng.org/">RELAX
 * NG</a>. MSV support requires the MSV jars in the classpath (BSD-style open
 * source license, part of MSV download). Alternatively, those jar files are
 * also part of the JAXB RI included in the <a target="_blank"
 * href="http://jwsdp.dev.java.net/">JWSDP</a> download 
 * (somewhat repackaged but otherwise identical). If you'd like to use
 * Schematron assertions embedded in a RELAX NG schema, you will also need to
 * download the <a target="_blank" href="http://msv.dev.java.net/">MSV
 * Schematron add-on</a> and add <code>relames.jar</code> to the classpath.
 * Note that MSV does not currently support the RELAX NG Compact Syntax. (The
 * RELAX NG homepage lists converters from RELAX NG Compact Syntax to RELAX NG
 * XML Syntax, including <a target="_blank"
 * href="http://www.thaiopensource.com/relaxng/trang.html">Trang</a>). 
 * Note that we do <i>not </i> recommend using MSV for DTDs or W3C
 * XML Schemas. Instead we recommend using the DTD or W3C XML Schema specific
 * methods of this class, which are more reliable. The other methods of this
 * class do not require MSV.
 * <p>
 * For anything but simple/basic use cases, this API is more robust,
 * configurable and convenient than the underlying XOM Builder constructor API.
 * <p>
 * Example usage:
 * <pre>
 *   // W3C XML Schema validation
 *   Map schemaLocations = new HashMap();
 *   schemaLocations.put(new File("/tmp/p2pio.xsd"), "http://dsd.lbl.gov/p2pio-1.0"); 
 *   Builder builder = new BuilderFactory().createW3CBuilder(schemaLocations);
 *   Document doc = builder.build(new File("/tmp/test.xml"));
 *   System.out.println(doc.toXML());
 * 
 *   // RELAX NG validation for DOCBOOK publishing system
 *   Builder builder = new BuilderFactory().createMSVBuilder(null, new URI("http://www.docbook.org/docbook-ng/ipa/docbook.rng"));
 *   //Builder builder = new BuilderFactory().createMSVBuilder(null, new File("/tmp/docbook/docbook.rng").toURI());
 *   Document doc = builder.build(new File("/tmp/mybook.xml"));
 *   System.out.println(doc.toXML());
 * </pre>
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.63 $, $Date: 2006/02/01 06:43:55 $
 */
public class BuilderFactory {
	
	private final Map featuresAndProperties;
	
	private static final boolean ENABLE_PARSER_GRAMMAR_POOLS = true;
	private static final boolean DEBUG = 
		XOMUtil.getSystemProperty("nux.xom.pool.BuilderFactory.debug", false);
	
	private static final String[] PARSERS = {
		"org.apache.xerces.parsers.SAXParser",
		"com.sun.org.apache.xerces.internal.parsers.SAXParser", // JDK 1.5
		"gnu.xml.aelfred2.XmlReader",
		"org.apache.crimson.parser.XMLReaderImpl",
		"com.bluecast.xml.Piccolo", 
		"oracle.xml.parser.v2.SAXParser",
		"com.jclark.xml.sax.SAX2Driver", 
		"net.sf.saxon.aelfred.SAXDriver",
		"com.icl.saxon.aelfred.SAXDriver", 
		"org.dom4j.io.aelfred2.SAXDriver",
		"org.dom4j.io.aelfred.SAXDriver"
	};

	private static final Map PARSER_GRAMMAR_POOLS;
	
	static {
		PARSER_GRAMMAR_POOLS = new HashMap(2);
		PARSER_GRAMMAR_POOLS.put("org.apache.xerces.parsers.SAXParser", 
			"org.apache.xerces.util.XMLGrammarPoolImpl");
		PARSER_GRAMMAR_POOLS.put("com.sun.org.apache.xerces.internal.parsers.SAXParser", 
			"com.sun.org.apache.xerces.internal.util.XMLGrammarPoolImpl"); // JDK 1.5
	}

	/**
	 * Creates a factory instance for creating Builders with an underlying
	 * {@link XMLReader} that has no particular features and properties.
	 */
	public BuilderFactory() {
		this(null);
	}
	
	/**
	 * Creates a factory instance for creating Builders with an underlying
	 * {@link XMLReader} that has the given features and properties.
	 * <p>
	 * The features and properties are read by this class, and guaranteed to
	 * never be modified.
	 * 
	 * @param featuresAndProperties
	 *            the features and properties the underlying XMLReader should have 
	 * 			(may be <code>null</code>). Keys must not be <code>null</code>.
	 * 			Values of type <code>Boolean</code> are interpreted as features,
	 * 			all other values are interpreted as properties.
	 * 
	 * @see XMLReader#setFeature(java.lang.String, boolean)
	 * @see XMLReader#setProperty(java.lang.String, java.lang.Object)
	 */
	public BuilderFactory(Map featuresAndProperties) {
		this.featuresAndProperties = featuresAndProperties;
	}
	
	/**
	 * Creates and returns a new validating or non-validating
	 * <code>Builder</code>. In validating mode, a
	 * {@link nu.xom.ValidityException} will be thrown when encountering an XML
	 * validation error upon parsing.
	 * 
	 * @param validate
	 *            true if XML validation should be performed.
	 * 
	 * @throws XMLException
	 *             if an appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder createBuilder(boolean validate) {
		return newBuilder(createParser(false), validate); 
	}
	
	/**
	 * Creates and returns a new <code>Builder</code> that validates against
	 * the DTD obtained by the given entity resolver. A
	 * {@link nu.xom.ValidityException} will be thrown when encountering an XML
	 * validation error upon parsing.
	 * <p>
	 * Quite possibly, you will want to use this in conjunction with a <a
	 * target="top" href="http://doctypechanger.sourceforge.net"> DoctypeChanger
	 * </a> helper or a SAX-2.0.1 <code>EntityResolver2</code>.
	 * 
	 * @param resolver
	 *            the entity resolver obtaining the DTD
	 * 
	 * @throws XMLException
	 *             if an appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder createDTDBuilder(EntityResolver resolver) {
		XMLReader parser = createParser(false);
		//parser.setFeature("http://xml.org/sax/features/validation", true); // will be set by XOM anyway
		parser.setEntityResolver(resolver);
		return newBuilder(parser, true); 
	}
	
	/**
	 * Creates and returns a new <code>EntityResolver</code> that validates
	 * against the DTD obtained from the given input stream. Caches the obtained 
	 * DTD stream data for efficient future reuse.
	 * 
	 * @param in
	 *            the input stream for the DTD
	 * 
	 * @throws IOException
	 *             if an I/O error occured while reading the stream
	 * @return a new EntityResolver
	 */
	public EntityResolver createResolver(InputStream in) throws IOException {
		// cache data for efficient future reuse, avoiding repeated I/O on reuse,
		// in particular network I/O if this is a networked stream.
		final byte[] data = FileUtil.toByteArray(in);
		return new EntityResolver() {
			public InputSource resolveEntity(String publicId, String systemId) {
				return new InputSource(new ByteArrayInputStream(data));
			}
		};
	}
	
	/**
	 * Creates and returns a new <code>Builder</code> that validates 
	 * against W3C XML Schemas. A {@link nu.xom.ValidityException} will be thrown when
	 * encountering an XML Schema validation error upon parsing.
	 * <p>
	 * Parameter <code>schemaLocations</code> specifies zero or more 
	 * <code>schemaLocation --> namespace</code> associations.
	 * Each map entry's key and value are interpreted as follows:
	 * <ul>
	 *    <li>schemaLocation (key): the location URL of the external schema (must not be <code>null</code>)</li>
	 *    <li>namespace (value): the namespace URI of the schema (may be <code>null</code>)</li>
	 * </ul>
	 * <p>
	 * <code>schemaLocation.toString()</code> and, if applicable, <code>namespace.toString()</code> are used
	 * to determine string representations. 
	 * Hence, the normal classes String, URI, URL, StringBuffer, etc. can all be used as input.
	 * <p>
	 * Note: This reflects the <code>external-schemaLocation</code> and 
	 * <code>external-noNamespaceSchemaLocation</code> 
	 * <a href="http://xml.apache.org/xerces-j/properties.html">Xerces properties</a>, 
	 * among others.
	 * <p>
	 * Example usage:
	 * <pre>
	 *     // no custom external schema; XMLReader takes schemaLocation declarations from XML document:
	 *     // &lt;ok xmlns="http://dsd.lbl.gov/p2pio-1.0" 
	 *     //         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	 *     //         xsi:schemaLocation="http://dsd.lbl.gov/p2pio-1.0 file:/tmp/p2pio.xsd"&gt;
	 *     //     &lt;transactionID&gt;ec14f115-d97e-4f76-8d0a-40d81de79445&lt;/transactionID&gt;
	 *     // &lt;/ok&gt;
	 *     Builder builder = new BuilderFactory().createW3CBuilder(null);
	 *     Document doc = builder.build(new File("/tmp/test.xml"));
	 *     System.out.println(doc.toXML());
	 * 
	 *     // two custom external schemas with namespaces 
	 *     // for soap message with application payload; 
	 *     // ignores schemaLocation declarations from XML document:
	 *     Map schemaLocations = new HashMap();
	 *     schemaLocations.put(new File("/tmp/soap.xsd"),  "http://www.w3.org/2001/12/soap-envelope");  
	 *     schemaLocations.put(new File("/tmp/p2pio.xsd"), "http://dsd.lbl.gov/p2pio-1.0"); 
	 *     Builder builder = new BuilderFactory().createW3CBuilder(schemaLocations);
	 *     Document doc = builder.build(new File("/tmp/test.xml"));
	 *     System.out.println(doc.toXML());
	 * 
	 *     // a custom external schemas without namespaces;
	 *     // ignores schemaLocation declarations from XML document:
	 *     Map schemaLocations = new HashMap();
	 *     schemaLocations.put("file:/tmp/foo.xsd", null);  
	 *     Builder builder = new BuilderFactory().createW3CBuilder(schemaLocations);
	 *     Document doc = builder.build(new File("/tmp/test.xml"));
	 *     System.out.println(doc.toXML());
	 * </pre>
	 * 
	 * @param schemaLocations 
	 * 			the <code>schemaLocation --> namespace</code> associations 
	 * 			(may be <code>null</code>).
	 * 
	 * @throws XMLException
	 *             if an appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder createW3CBuilder(Map schemaLocations) {
		XMLReader parser = createParser(true);
		try {
			setupW3CParser(parser, schemaLocations);			
		} catch (SAXException e) {
			throw new XMLException(
				"Can't find or create W3C schema validating parser (i.e. Xerces)" + 
				" - check your classpath", 
				e);
		}
		
		try { // improve performance; see http://www-106.ibm.com/developerworks/xml/library/x-perfap3.html
			parser.setFeature("http://apache.org/xml/features/validation/schema/augment-psvi", false);
		} catch (SAXException e) {
			; // we can live with that
		}
		
		return newBuilder(parser, true); 
	}
	
	/**
	 * Creates and returns a new <code>Builder</code> that validates against
	 * the given MSV (Multi-Schema Validator) schema. A
	 * {@link nu.xom.ParsingException} will be thrown when encountering an XML
	 * validation error upon parsing.
	 * <p>
	 * The type of all schemas written in XML-syntax (RELAX NG, W3C XML Schema,
	 * etc) will be auto-detected correctly by MSV no matter what the format is.
	 * <p>
	 * At least one of the parameters <code>schema</code> and
	 * <code>systemID</code> must not be <code>null</code>.
	 * 
	 * @param schema
	 *            the schema to validate against. May be <code>null</code>,
	 *            in which case the <code>systemID</code> parameter is used.
	 * @param systemID
	 *            the URL of the schema. Also used to resolve relative URIs when
	 *            including RELAX NG schema modules via <code>include</code>
	 *            directives into a main schema. Need not be the stream's actual
	 *            URI. May be <code>null</code> in which case it defaults to
	 *            the current working directory.
	 * @throws XMLException
	 *             if the schema contains a syntax or semantic error or an
	 *             appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder createMSVBuilder(InputStream schema, URI systemID) {
		// for background on how all this works see path/to/msv/JAXPmasquerading.html
		// as well as com.sun.msv.verifier.jaxp.SAXParserImpl and 
		// org.iso_relax.verifier.VerifierFactory.compileSchema(InputSource)
		if (schema == null && systemID == null) throw new IllegalArgumentException(
			"At least one of the parameters 'schema' and 'systemId' must not be null");
		InputSource source = new InputSource();
		if (schema != null) source.setByteStream(schema);
		if (systemID != null) source.setSystemId(systemID.toASCIIString());
		
		XMLReader parser;
		try { 
			// somewhat akward work-around to avoid making MSV a compile-time dependency
			SAXParserFactory factory = (SAXParserFactory) ClassLoaderUtil.newInstance(
					"com.sun.msv.verifier.jaxp.SAXParserFactoryImpl");
			factory.setNamespaceAware(true);
			SAXParser saxParser = factory.newSAXParser();
			// would be nice to set the property on the XMLReader (instead of the SAXParser), but that wouldn't work
			saxParser.setProperty("http://www.sun.com/xml/msv/schema", source);
			parser = saxParser.getXMLReader();
			
			// find root filter, if any
			XMLReader filter = parser;
			while (filter instanceof XMLFilter && ((XMLFilter) filter).getParent() instanceof XMLFilter) {
				if (DEBUG) System.err.println("currFilter=" + filter);
				filter = ((XMLFilter) filter).getParent();
			}
			// replace default parser with our own parser, if possible
			if (filter instanceof XMLFilter) { 
				if (DEBUG) System.err.println("rootFilter=" + filter);
				if (DEBUG) System.err.println("rootFilter.getParent()=" + ((XMLFilter) filter).getParent());
				((XMLFilter) filter).setParent(createParser(false));
			}
			else {			
				setupFeaturesAndProps(parser);
			}
			if (DEBUG) System.err.println("using MSV XMLReader=" + parser.getClass().getName());
		} catch (SAXNotRecognizedException ex) { // e.g. syntax or semantic error in schema
			throw new XMLException(ex.toString(), ex);
		} catch (Exception ex) {
			throw new XMLException(
					"Could not find or create a suitable MSV parser" +
					" - check your classpath"
					, ex);
		} catch (NoClassDefFoundError ex) {
			throw new XMLException(
					"Could not find or create a suitable MSV parser" +
					" - check your classpath"
					, ex);
		}
		
		return newBuilder(parser, false);
	}
	
	/**
	 * Callback that creates and returns a new validating or non-validating
	 * Builder for the given parser.
	 * <p>
	 * This default implementation creates Builders with a <code>null</code>
	 * {@link nu.xom.NodeFactory}.
	 * <p>
	 * Override this method if you need custom node factories.
	 * <p>
	 * Note: A node factory may well be stateful and mutable, hence it may well
	 * be unsafe to share a single node factory instance among multiple Builders
	 * in a multi-threaded context. By providing this method, an application can
	 * create new node factories as needed via straightforward
	 * subclassing/overriding of this class.
	 * 
	 * @param parser
	 *            the underlying SAX XML parser
	 * @param validate
	 *            whether or not to validate
	 * @return a new Builder
	 */
	protected Builder newBuilder(XMLReader parser, boolean validate) {
		return new Builder(parser, validate, null); 		
	}
	
	private XMLReader createParser(boolean w3cSchemaParser) {
		XMLReader parser;
		for (int i = 0; i < PARSERS.length; i++) {
			try {
				parser = XMLReaderFactory.createXMLReader(PARSERS[i]);
				if (w3cSchemaParser && ENABLE_PARSER_GRAMMAR_POOLS) {
					String clazz = (String) PARSER_GRAMMAR_POOLS.get(PARSERS[i]);
					if (clazz != null) {
						// This improves performance and, more importantly, prevents xerces
						// bugs/exceptions when the Builder is used more than once (at least for
						// xerces-2.6.2). But it would lead to xerces bugs in non-validating mode with DTDs,
						// at least for xerces-2.7.1, so in this case we're not enabling it.
						// See http://xml.apache.org/xerces2-j/faq-grammars.html
						// See http://www-106.ibm.com/developerworks/xml/library/x-perfap3.html
					 	parser.setProperty("http://apache.org/xml/properties/internal/grammar-pool",
							ClassLoaderUtil.newInstance(clazz));
					}
				}
				setupFeaturesAndProps(parser);				
				//parser.setProperty("http://apache.org/xml/properties/input-buffer-size", new Integer(8192));
				if (DEBUG) System.err.println("using XMLReader=" + parser.getClass().getName());
				return parser;
			} catch (SAXException ex) {
				// keep on trying
			} catch (NoClassDefFoundError err) {
				// keep on trying
			} catch (Exception err) {
				// keep on trying
			}
		}

		try { // SAX default
			parser = XMLReaderFactory.createXMLReader();
			setupFeaturesAndProps(parser);
			if (DEBUG) System.err.println("using default SAX XMLReader=" + parser.getClass().getName());
			return parser;
		} catch (Exception ex) {
			// keep on trying
		}

		try { // JAXP default
			SAXParserFactory factory = SAXParserFactory.newInstance();
			factory.setNamespaceAware(true);
			parser = factory.newSAXParser().getXMLReader();
			setupFeaturesAndProps(parser);
			if (DEBUG) System.err.println("using default JAXP XMLReader=" + parser.getClass().getName());
			return parser;
		} catch (Exception ex) {
			throw new XMLException(
					"Could not find or create a suitable SAX2 parser" + 
					" - check your classpath", 
					ex);
		} catch (NoClassDefFoundError ex) {
			throw new XMLException(
					"Could not find or create a suitable SAX2 parser" + 
					" - check your classpath", 
					ex);
		}
	}
	
	/** 
	 * See http://xml.apache.org/xerces-j/properties.html and 
	 * http://www.jdom.org/docs/faq.html
	 */
	private void setupW3CParser(XMLReader parser, Map schemaLocations) 
		throws SAXNotRecognizedException, SAXNotSupportedException {
		
		parser.setFeature("http://apache.org/xml/features/validation/schema", true);
		//parser.setFeature("http://xml.org/sax/features/validation", true); // will be set by XOM anyway
		
		if (schemaLocations == null || schemaLocations.size() == 0) return;
		
		// construct the property string concatenation as expected by xerces
		// ns1 + " " + schema1 + ... + " " + nsN + " " + schemaN
		StringBuffer withNamespaces = new StringBuffer();
		String withoutNamespaces = null;
		Iterator iter = schemaLocations.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			if (key == null) 
				throw new IllegalArgumentException("schema must not be null: " + schemaLocations);
			if (key instanceof File) 
				key = ((File) key).toURI();
			if (key instanceof URI) {
				URI uri = (URI) key;
				if (uri.getScheme() != null && uri.getScheme().equals("file")) {
					// xerces requires file:/path/to/file instead of file:///path/to/file
					if (uri.getRawPath() != null) key = "file:" + uri.getRawPath();
				}
			}
			String schema = key.toString().trim();
			if (schema.length() == 0) 
				throw new IllegalArgumentException("schema must not have length zero: " + schemaLocations);
			if (DEBUG) System.err.println("schema="+schema);
			String namespace = entry.getValue() == null ? "" : entry.getValue().toString().trim();
			
			if (namespace.length() > 0) {
				if (withNamespaces.length() > 0) withNamespaces.append(" ");
				withNamespaces.append(namespace + " " + schema);
			}
			else {
				if (withoutNamespaces != null) 
					throw new IllegalArgumentException("must not specify more than one schema without namespace: " + schemaLocations);
				withoutNamespaces = schema;				
			}
		}
		
		if (withNamespaces.length() > 0) {
			parser.setProperty(
				"http://apache.org/xml/properties/schema/external-schemaLocation",
				withNamespaces.toString());
			if (DEBUG) System.err.println("withNamespaces='" + withNamespaces + "'");
		}
		
		if (withoutNamespaces != null) {
			parser.setProperty(
				"http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation",
				withoutNamespaces);
			if (DEBUG) System.err.println("withoutNamespaces='" + withoutNamespaces + "'");
		}
	}

	private void setupFeaturesAndProps(XMLReader parser) throws SAXNotRecognizedException, SAXNotSupportedException {
		if (featuresAndProperties == null || featuresAndProperties.size() == 0) return;
		Iterator iter = featuresAndProperties.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			if (entry.getKey() == null) 
				throw new IllegalArgumentException("feature/property name must not be null: " + featuresAndProperties);
			String name = entry.getKey().toString();
			Object value = entry.getValue();
			if (value instanceof Boolean)
				parser.setFeature(name, ((Boolean)value).booleanValue());				
			else
				parser.setProperty(name, value);
		}
	}
		
//	/**
//	 * Creates and returns a <a target="_blank"
//	 * href="http://www.tagsoup.info">TagSoup</a> Builder capable of parsing
//	 * malformed and wellformed HTML. Requires the tagsoup.jar on the classpath.
//	 * 
//	 * @return a tagsoup Builder
//	 */
//	public Builder createTagSoupBuilder() {
//		// TODO: also add corresponding method to BuilderPool?
//		XMLReader parser;
//		try {
//			parser = (XMLReader) ClassLoaderUtil.newInstance("org.ccil.cowan.tagsoup.Parser");
//		} catch (Exception ex) {
//			throw new XMLException("Could not find or create a TagSoup parser", ex);
//		}
//		return newBuilder(parser, false);
//	}
	
}