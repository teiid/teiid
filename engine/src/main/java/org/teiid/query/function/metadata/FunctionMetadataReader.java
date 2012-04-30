package org.teiid.query.function.metadata;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.teiid.core.types.XMLType;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;

public class FunctionMetadataReader {

	List<FunctionMethod> functionMethods = new ArrayList<FunctionMethod>();
	
	public static List<FunctionMethod> loadFunctionMethods(InputStream source) throws XMLStreamException {
		FunctionMetadataReader md = parseFunctions(source);
		return md.functionMethods;
	}

	public static FunctionMetadataReader parseFunctions(InputStream content) throws XMLStreamException {
		 XMLInputFactory inputFactory=XMLType.getXmlInputFactory();
		 XMLStreamReader reader = inputFactory.createXMLStreamReader(content);
		 FunctionMetadataReader fmr = new FunctionMetadataReader();
		 while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
			 switch (Namespace.forUri(reader.getNamespaceURI())) {
			 	case XMI: {
                    Element element = Element.forName(reader.getLocalName());
                    switch (element) {
	                    case XMI:
	               		 parseFunctions(reader, fmr);
	               		 while(reader.hasNext() && reader.next() != XMLStreamConstants.END_DOCUMENT);
	               		 break;
                    }
			 		break;
			 	}
			 }
		 }
		 return fmr;
	}

	private static void parseFunctions(XMLStreamReader reader, FunctionMetadataReader fmr) throws XMLStreamException {
		while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
			String elementName = reader.getLocalName();
	        switch (Element.forName(elementName)) {
	            case SCALAR_FUNCTION:
	            	fmr.functionMethods.add(parseScalarFunction(reader));
	            	break;
	            default:
	            	// skip the elements not needed.
	            	while (reader.hasNext()) {
	            		if (reader.nextTag() == XMLStreamConstants.END_ELEMENT && reader.getLocalName().equals(elementName)) {
	            			break;
	            		}
	            	}
	            	break;
	        }
		}
	}	
	
	
	private static FunctionMethod parseScalarFunction(XMLStreamReader reader) throws XMLStreamException {
		FunctionMethod function = new FunctionMethod();
		if (reader.getAttributeCount() > 0) {
			for(int i=0; i<reader.getAttributeCount(); i++) {
				String attrName = reader.getAttributeLocalName(i);
				String attrValue = reader.getAttributeValue(i);
				if (Element.NAME.getLocalName().equals(attrName)) {
					function.setName(attrValue);
				}
				else if (Element.CATEGORY.getLocalName().equals(attrName)) {
					function.setCategory(attrValue);
				}
				else if (Element.INVOCATION_CLASS.getLocalName().equals(attrName)) {
					function.setInvocationClass(attrValue);
					// TODO: set class loader
					// function.setClassloader();
				}
				else if (Element.INVOCATION_METHOD.getLocalName().equals(attrName)) {
					function.setInvocationMethod(attrValue);
				}
				else if (Element.PUSHDOWN.getLocalName().equals(attrName)) {
					function.setPushDown(attrValue);
				}
				else if (Element.DETERMINISTIC.getLocalName().equals(attrName)) {
					function.setDeterministicBoolean(Boolean.parseBoolean(attrValue));
				}				
			}
		}
		LinkedList<FunctionParameter> inputs = new LinkedList<FunctionParameter>();
		
		while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
			switch (Element.forName(reader.getLocalName())) {
			case INPUT_PARAMTERS:
				inputs.addLast(parseParameter(reader));
				break;
			case RETURN_PARAMETER:
				function.setOutputParameter(parseParameter(reader));
				break;
			}
		}
		function.setInputParameters(inputs);
		return function;
	}

	private static FunctionParameter parseParameter(XMLStreamReader reader) throws XMLStreamException {
		FunctionParameter fp = new FunctionParameter();
		if (reader.getAttributeCount() > 0) {
			for(int i=0; i<reader.getAttributeCount(); i++) {
				String attrName = reader.getAttributeLocalName(i);
				String attrValue = reader.getAttributeValue(i);
				if (Element.NAME.getLocalName().equals(attrName)) {
					fp.setName(attrValue);
				}
				else if (Element.DESCRIPTION.getLocalName().equals(attrName)) {
					fp.setDescription(attrValue);
				}
				else if (Element.TYPE.getLocalName().equals(attrName)) {
					fp.setType(attrValue);
				}
			}
		}
		while(reader.nextTag() != XMLStreamConstants.END_ELEMENT);
		return fp;
	}
	
	enum Element {
	    // must be first
	    UNKNOWN(null),
	    XMI("XMI"),//$NON-NLS-1$
	    SCALAR_FUNCTION("ScalarFunction"),//$NON-NLS-1$
		NAME("name"), //$NON-NLS-1$
		INPUT_PARAMTERS("inputParameters"),//$NON-NLS-1$
		DESCRIPTION("description"),//$NON-NLS-1$
		CATEGORY("category"),//$NON-NLS-1$
		PUSHDOWN("pushDown"),//$NON-NLS-1$
		INVOCATION_CLASS("invocationClass"),//$NON-NLS-1$
		INVOCATION_METHOD("invocationMethod"),//$NON-NLS-1$
		RETURN_PARAMETER("returnParameter"),//$NON-NLS-1$
		DETERMINISTIC("deterministic"),//$NON-NLS-1$
		TYPE("type");//$NON-NLS-1$
		
	    private final String name;

	    Element(final String name) {
	        this.name = name;
	    }

	    /**
	     * Get the local name of this element.
	     *
	     * @return the local name
	     */
	    public String getLocalName() {
	        return name;
	    }

	    private static final Map<String, Element> elements;

	    static {
	        final Map<String, Element> map = new HashMap<String, Element>();
	        for (Element element : values()) {
	            final String name = element.getLocalName();
	            if (name != null) map.put(name, element);
	        }
	        elements = map;
	    }

	    public static Element forName(String localName) {
	        final Element element = elements.get(localName);
	        return element == null ? UNKNOWN : element;
	    }	    
	}		
	
	enum Namespace {
	    // must be first
	    UNKNOWN(null),
	    XMI("http://www.omg.org/XMI"), //$NON-NLS-1$
	    FUNCTION("http://www.metamatrix.com/metamodels/MetaMatrixFunction"); //$NON-NLS-1$
	    
	    private final String uri;

	    Namespace(String uri) {
	        this.uri = uri;
	    }

	    public String getUri() {
	        return uri;
	    }

	    private static final Map<String, Namespace> namespaces;

	    static {
	        final Map<String, Namespace> map = new HashMap<String, Namespace>();
	        for (Namespace namespace : values()) {
	            final String name = namespace.getUri();
	            if (name != null) map.put(name, namespace);
	        }
	        namespaces = map;
	    }

	    public static Namespace forUri(String uri) {
	        final Namespace element = namespaces.get(uri);
	        return element == null ? UNKNOWN : element;
	    }
	}	
}
