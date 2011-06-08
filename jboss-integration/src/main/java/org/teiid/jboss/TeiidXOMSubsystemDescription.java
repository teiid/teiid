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
package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ATTRIBUTES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.CHILDREN;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DEFAULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.MAX_OCCURS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.MIN_OCCURS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUIRED;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.TYPE;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.core.TeiidRuntimeException;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import com.sun.xml.xsom.XSAnnotation;
import com.sun.xml.xsom.XSAttributeDecl;
import com.sun.xml.xsom.XSAttributeUse;
import com.sun.xml.xsom.XSComplexType;
import com.sun.xml.xsom.XSContentType;
import com.sun.xml.xsom.XSElementDecl;
import com.sun.xml.xsom.XSFacet;
import com.sun.xml.xsom.XSModelGroup;
import com.sun.xml.xsom.XSParticle;
import com.sun.xml.xsom.XSRestrictionSimpleType;
import com.sun.xml.xsom.XSSchema;
import com.sun.xml.xsom.XSSchemaSet;
import com.sun.xml.xsom.XSSimpleType;
import com.sun.xml.xsom.XSTerm;
import com.sun.xml.xsom.parser.AnnotationContext;
import com.sun.xml.xsom.parser.AnnotationParser;
import com.sun.xml.xsom.parser.AnnotationParserFactory;
import com.sun.xml.xsom.parser.XSOMParser;

/**
 * Lot of XSD parsing code is from http://it.toolbox.com/blogs/enterprise-web-solutions/parsing-an-xsd-schema-in-java-32565 
 */
public class TeiidXOMSubsystemDescription implements DescriptionProvider {
	
	
	@Override
	public ModelNode getModelDescription(Locale locale) {
		
        ModelNode node = new ModelNode();
        node.get(ModelDescriptionConstants.DESCRIPTION).set("teiid subsystem"); //$NON-NLS-1$
        node.get(ModelDescriptionConstants.HEAD_COMMENT_ALLOWED).set(true);
        node.get(ModelDescriptionConstants.TAIL_COMMENT_ALLOWED).set(true);
        node.get(ModelDescriptionConstants.NAMESPACE).set(Namespace.CURRENT.getUri());
        
        try {
			XSOMParser parser = new XSOMParser();
			URL xsdURL = Thread.currentThread().getContextClassLoader().getResource("schema/jboss-teiid.xsd"); //$NON-NLS-1$
			parser.setAnnotationParser(new AnnotationFactory());
			parser.parse(xsdURL); 
			XSSchemaSet schemaSet = parser.getResult();
			if (schemaSet == null) {
				throw new TeiidRuntimeException("No Schema parsed");
			}
			 XSSchema xsSchema = schemaSet.getSchema(1);
			 Iterator<XSElementDecl> it = xsSchema.iterateElementDecls();
			 while (it.hasNext()) {
				 XSElementDecl element = it.next();
				 parseElement(null, element, node, false);
			 }
		} catch (SAXException e) {
			throw new TeiidRuntimeException(e);
		}        
        
		return node;
	}

	public void describeNode(ModelNode node, String type, String name, String description, String dataType, String defaultValue, 
			SimpleTypeRestriction restriction, boolean required) {
		node.get(type, name, TYPE).set(getModelType(dataType));
        node.get(type, name, DESCRIPTION).set(description);
        node.get(type, name, REQUIRED).set(required);
        node.get(type, name, MAX_OCCURS).set(1);
        if (defaultValue != null) {
        	node.get(type, name, DEFAULT).set(defaultValue);
        }
        
        if (restriction.enumeration != null) {
        	//TODO:
        	//node.get(type, name, "allowed").set(Arrays.asList(restriction.enumeration));
        }
    }
	


	private void parseElement(XSParticle p, XSElementDecl element, ModelNode node, boolean addChild) {
		if (element.getType().isComplexType()) {
			if (addChild) {
				ModelNode childNode = node.get(CHILDREN, element.getName());
				childNode.get(TYPE).set(ModelType.OBJECT);
				childNode.get(DESCRIPTION).set(getDescription(element));
				childNode.get(REQUIRED).set(false);
				childNode.get(MAX_OCCURS).set(p.getMaxOccurs());
				childNode.get(MIN_OCCURS).set(p.getMinOccurs());
				parseComplexType(element, childNode);
			}
			else {
				parseComplexType(element, node);
			}
		 }
		 else {
			 String defaultValue =  null;
			 if (element.getDefaultValue() != null) {
				 defaultValue = element.getDefaultValue().value;
			 }
			 boolean required = false;
			 XSParticle particle = ((XSContentType)element.getType()).asParticle();
			 if (particle != null) {
				 if (particle.getMinOccurs() != 0) {
					 required = true;
				 }
			 }
			 describeNode(node, ATTRIBUTES, element.getName(), getDescription(element), element.getType().getName(), defaultValue, getRestrictions(element.getType().asSimpleType()), required);

		 }
	}

	private void parseComplexType(XSElementDecl element, ModelNode node) {	
		XSComplexType type = element.getType().asComplexType();
		 Iterator<? extends XSAttributeUse> attrIter =  type.iterateAttributeUses();
		 while(attrIter.hasNext()) {
		        XSAttributeDecl attr = attrIter.next().getDecl();
		        String defaultValue =  null;
				 if (attr.getDefaultValue() != null) {
					 defaultValue = attr.getDefaultValue().value;
				 }		        
		        describeNode(node, ATTRIBUTES, attr.getName(), attr.getName(), attr.getType().getName(), defaultValue, getRestrictions(attr.getType().asSimpleType()), true);
		 }
		 
		XSContentType contentType = type.getContentType();
		XSParticle particle = contentType.asParticle();
		if (particle != null) {
			XSTerm term = particle.getTerm();
			if (term.isModelGroup()) {
				XSModelGroup xsModelGroup = term.asModelGroup();
				XSParticle[] particles = xsModelGroup.getChildren();
				for (XSParticle p : particles) {
					XSTerm pterm = p.getTerm();
					if (pterm.isElementDecl()) { 												
						parseElement(p, pterm.asElementDecl(), node, true);
					}
				}
			}
		}
	}

	private ModelType getModelType(String type) {
		if (type == null) {
			return ModelType.STRING;
		}
		if (type.equals("int")) { //$NON-NLS-1$
			return ModelType.INT;
		}
		else if (type.equals("boolean")) { //$NON-NLS-1$
			return ModelType.BOOLEAN;
		}
		return ModelType.STRING;
	}

	private String getDescription(XSElementDecl element) {
		String description = element.getName();
		 XSAnnotation annotation = element.getAnnotation();
		 if (annotation != null) {
			 description = (String)annotation.getAnnotation();
		 }
		return description;
	}


	private class AnnotationFactory implements AnnotationParserFactory{
	    @Override
	    public AnnotationParser create() {
	        return new XsdAnnotationParser();
	    }
	}
	
	private class XsdAnnotationParser extends AnnotationParser {
	    private StringBuilder documentation = new StringBuilder();
	    @Override
	    public ContentHandler getContentHandler(AnnotationContext context, String parentElementName, ErrorHandler handler, EntityResolver resolver) {
	        return new ContentHandler(){
	            private boolean parsingDocumentation = false;
	            @Override
	            public void characters(char[] ch, int start, int length) throws SAXException {
	                if(parsingDocumentation){
	                    documentation.append(ch,start,length);
	                }
	            }
	            @Override
	            public void endElement(String uri, String localName, String name)
	            throws SAXException {
	                if(localName.equals("documentation")){ //$NON-NLS-1$
	                    parsingDocumentation = false;
	                }
	            }
	            @Override
	            public void startElement(String uri, String localName,String name,
	            Attributes atts) throws SAXException {
	                if(localName.equals("documentation")){ //$NON-NLS-1$
	                    parsingDocumentation = true;
	                }
	            }
				@Override
				public void endDocument() throws SAXException {
				}
				@Override
				public void endPrefixMapping(String prefix) throws SAXException {
				}
				@Override
				public void ignorableWhitespace(char[] ch, int start, int length)
						throws SAXException {
				}
				@Override
				public void processingInstruction(String target, String data)
						throws SAXException {
				}
				@Override
				public void setDocumentLocator(Locator locator) {
				}
				@Override
				public void skippedEntity(String name) throws SAXException {
				}
				@Override
				public void startDocument() throws SAXException {
				}
				@Override
				public void startPrefixMapping(String prefix, String uri) throws SAXException {
				}
	        };
	    }
	    @Override
	    public Object getResult(Object existing) {
	        return documentation.toString().trim();
	    }
	}
	
	public class SimpleTypeRestriction{
	    public String[] enumeration = null;
	    public String maxValue = null;
	    public String minValue = null;
	    public String length = null;
	    public String maxLength = null;
	    public String minLength = null;
	    public String pattern = null;
	    public String totalDigits = null;
	}

	private SimpleTypeRestriction getRestrictions(XSSimpleType xsSimpleType){
		SimpleTypeRestriction t = new SimpleTypeRestriction();
	    XSRestrictionSimpleType restriction = xsSimpleType.asRestriction();
	    if(restriction != null){
	        List<String> enumeration = new ArrayList<String>();
	        Iterator<? extends XSFacet> i = restriction.getDeclaredFacets().iterator();
	        while(i.hasNext()){
	            XSFacet facet = i.next();
	            if(facet.getName().equals(XSFacet.FACET_ENUMERATION)){
	                enumeration.add(facet.getValue().value);
	            }
	            if(facet.getName().equals(XSFacet.FACET_MAXINCLUSIVE)){
	                t.maxValue = facet.getValue().value;
	            }
	            if(facet.getName().equals(XSFacet.FACET_MININCLUSIVE)){
	                t.minValue = facet.getValue().value;
	            }
	            if(facet.getName().equals(XSFacet.FACET_MAXEXCLUSIVE)){
	                t.maxValue = String.valueOf(Integer.parseInt(facet.getValue().value) - 1);
	            }
	            if(facet.getName().equals(XSFacet.FACET_MINEXCLUSIVE)){
	                t.minValue = String.valueOf(Integer.parseInt(facet.getValue().value) + 1);
	            }
	            if(facet.getName().equals(XSFacet.FACET_LENGTH)){
	                t.length = facet.getValue().value;
	            }
	            if(facet.getName().equals(XSFacet.FACET_MAXLENGTH)){
	                t.maxLength = facet.getValue().value;
	            }
	            if(facet.getName().equals(XSFacet.FACET_MINLENGTH)){
	                t.minLength = facet.getValue().value;
	            }
	            if(facet.getName().equals(XSFacet.FACET_PATTERN)){
	                t.pattern = facet.getValue().value;
	            }
	            if(facet.getName().equals(XSFacet.FACET_TOTALDIGITS)){
	                t.totalDigits = facet.getValue().value;
	            }
	        }
	        if(enumeration.size() > 0){
	            t.enumeration = enumeration.toArray(new String[]{});
	        }
	    }
	    return t;
	}	
}
