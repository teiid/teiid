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

package org.teiid.client.plan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.jdbc.JDBCPlugin;


/**
 * A PlanNode represents part of processing plan tree.  For relational plans 
 * child PlanNodes may be either subqueries or nodes that feed tuples into the
 * parent. For procedure plans child PlanNodes will be processing instructions,
 * which can in turn contain other relational or procedure plans.
 */
public class PlanNode implements Externalizable {

	/**
	 * A Property is a named value of a {@link PlanNode} that may be
	 * another {@link PlanNode} or a non-null list of values.
	 */
	public static class Property implements Externalizable {
		private String name;
		private List<String> values;
		private PlanNode planNode;
		
		public Property() {
			
		}
		
		public Property(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public List<String> getValues() {
			return values;
		}
		
		public void setValues(List<String> values) {
			this.values = values;
		}
		
		public PlanNode getPlanNode() {
			return planNode;
		}
		
		public void setPlanNode(PlanNode planNode) {
			this.planNode = planNode;
		}
		
		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			this.name = (String)in.readObject();
			this.values = ExternalizeUtil.readList(in, String.class);
			this.planNode = (PlanNode)in.readObject();
		}
		
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeObject(name);
			ExternalizeUtil.writeCollection(out, values);
			out.writeObject(planNode);
		}
		
	}
	
    private List<Property> properties = new LinkedList<Property>();
    private PlanNode parent;
    private String name;
    
    public PlanNode() {
    	
    }
	
    public PlanNode(String name) {
    	this.name = name;
    }
    
    public String getName() {
		return name;
	}
    
    void setParent(PlanNode parent) {
        this.parent = parent;
    }

    public PlanNode getParent() {
        return this.parent;
    }
    
    public List<Property> getProperties() {
		return properties;
	}
    
    public void addProperty(String pname, PlanNode value) {
    	Property p = new Property(pname);
    	p.setPlanNode(value);
    	value.setParent(this);
    	this.properties.add(p);
    }
    
    public void addProperty(String pname, List<String> value) {
    	Property p = new Property(pname);
    	if (value == null) {
    		value = Collections.emptyList();
    	}
    	p.setValues(value);
    	this.properties.add(p);
    }
    
    public void addProperty(String pname, String value) {
    	Property p = new Property(pname);
    	p.setValues(Arrays.asList(value));
    	this.properties.add(p);
    }
    
    /**
     * Converts this PlanNode to XML. See the JAXB bindings for the
     * document form.
     * @return an XML document of this PlanNode
     */
    public String toXml() {
    	try {
			XMLOutputFactory outputFactory = XMLOutputFactory.newFactory();
			StringWriter stringWriter = new StringWriter();
			XMLStreamWriter writer = outputFactory.createXMLStreamWriter(stringWriter);
			writer.writeStartDocument("UTF-8", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$
			writePlanNode(this, writer);
			writer.writeEndDocument();
			return stringWriter.toString();
		} catch (FactoryConfigurationError e) {
			 throw new TeiidRuntimeException(JDBCPlugin.Event.TEIID20002, e);
		} catch (XMLStreamException e) {
			 throw new TeiidRuntimeException(JDBCPlugin.Event.TEIID20003, e);
		}
    }
    
    private void writeProperty(Property property, XMLStreamWriter writer) throws XMLStreamException {
    	writer.writeStartElement("property"); //$NON-NLS-1$
    	writer.writeAttribute("name", property.getName()); //$NON-NLS-1$
    	if (property.getValues() != null) {
	    	for (String value:property.getValues()) {
	    		writeElement(writer, "value", value); //$NON-NLS-1$
	    	}
    	}
    	PlanNode node = property.getPlanNode();
    	if (node != null) {
    		writePlanNode(node, writer);
    	}
    	writer.writeEndElement();
    }
    
    private void writePlanNode(PlanNode node, XMLStreamWriter writer) throws XMLStreamException {
		writer.writeStartElement("node"); //$NON-NLS-1$
		writer.writeAttribute("name", node.getName()); //$NON-NLS-1$
		for (Property p:node.getProperties()) {
			writeProperty(p, writer);
		}
		writer.writeEndElement();
    }
    
    private static void writeElement(final XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        writer.writeStartElement(name);
        writer.writeCharacters(value);
        writer.writeEndElement();
    }    
    
	private static Properties getAttributes(XMLStreamReader reader) {
		Properties props = new Properties();
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			props.setProperty(attrName, attrValue);
    		}
    	}
    	return props;
	}
	
	public static PlanNode fromXml(String planString) throws XMLStreamException {
		XMLInputFactory inputFactory = XMLType.getXmlInputFactory();
		XMLStreamReader reader = inputFactory.createXMLStreamReader(new StringReader(planString));

		while (reader.hasNext()&& (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
			String element = reader.getLocalName();
			if (element.equals("node")) { //$NON-NLS-1$
				Properties props = getAttributes(reader);
				PlanNode planNode = new PlanNode(props.getProperty("name"));//$NON-NLS-1$
				planNode.setParent(null);
				buildNode(reader, planNode);
				return planNode;
			}
			throw new XMLStreamException(JDBCPlugin.Util.gs("unexpected_element", reader.getName(), "node"),reader.getLocation());//$NON-NLS-1$ //$NON-NLS-2$
		}
		return null;
	}

	private static PlanNode buildNode(XMLStreamReader reader, PlanNode node) throws XMLStreamException {
	   while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
		   String property = reader.getLocalName();
		   if (property.equals("property")) {//$NON-NLS-1$
			   Properties props = getAttributes(reader);
			   ArrayList<String> values = new ArrayList<String>();
			   while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
				   String valueNode = reader.getLocalName();
				   if (valueNode.equals("value")) {//$NON-NLS-1$
					   values.add(reader.getElementText());					   
				   }
				   else if (valueNode.equals("node")) {//$NON-NLS-1$
					   values = null;
					   Properties nodeProps = getAttributes(reader);
					   PlanNode childNode = new PlanNode(nodeProps.getProperty("name"));//$NON-NLS-1$
					   node.addProperty(props.getProperty("name"), buildNode(reader, childNode));//$NON-NLS-1$
					   break;
				   }
				   else {
					   throw new XMLStreamException(JDBCPlugin.Util.gs("unexpected_element", reader.getName(), "value"), reader.getLocation());//$NON-NLS-1$ //$NON-NLS-2$
				   }
			   }
			   if (values != null) {
				   node.addProperty(props.getProperty("name"), values);//$NON-NLS-1$
			   }
		   }
		   else {
			   throw new XMLStreamException(JDBCPlugin.Util.gs("unexpected_element", reader.getName(), "property"), reader.getLocation()); //$NON-NLS-1$ //$NON-NLS-2$
		   }
	   }
	   return node;
	}

    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	visitNode(this, 0, builder);
    	return builder.toString();
    }
    
    protected void visitNode(PlanNode node, int nodeLevel, StringBuilder text) {
        for(int i=0; i<nodeLevel; i++) {
            text.append("  "); //$NON-NLS-1$
        }
        text.append(node.getName());        
        text.append("\n"); //$NON-NLS-1$
        
        // Print properties appropriately
        int propTabs = nodeLevel + 1;
        for (PlanNode.Property property : node.getProperties()) {
            // Print leading spaces for prop name
        	for(int t=0; t<propTabs; t++) {
				text.append("  "); //$NON-NLS-1$
			}
            printProperty(nodeLevel, property, text);
        }
    }

    private void printProperty(int nodeLevel, Property p, StringBuilder text) {
        text.append("+ "); //$NON-NLS-1$
        text.append(p.getName());
        
        if (p.getPlanNode() != null) {
	        text.append(":\n"); //$NON-NLS-1$ 
	        visitNode(p.getPlanNode(), nodeLevel + 2, text);
        } else if (p.getValues().size() > 1){
        	text.append(":\n"); //$NON-NLS-1$ 
        	for (int i = 0; i < p.getValues().size(); i++) {
            	for(int t=0; t<nodeLevel+2; t++) {
            		text.append("  "); //$NON-NLS-1$
            	}
                text.append(i);
                text.append(": "); //$NON-NLS-1$
            	text.append(p.getValues().get(i));
                text.append("\n"); //$NON-NLS-1$
			}
        } else if (p.getValues().size() == 1) {
        	text.append(":"); //$NON-NLS-1$
        	text.append(p.getValues().get(0));
        	text.append("\n"); //$NON-NLS-1$
        } else {
        	text.append("\n"); //$NON-NLS-1$
        }
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException,
    		ClassNotFoundException {
    	this.properties = ExternalizeUtil.readList(in, Property.class);
    	this.parent = (PlanNode)in.readObject();
    	this.name = (String)in.readObject();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    	ExternalizeUtil.writeCollection(out, properties);
    	out.writeObject(this.parent);
    	out.writeObject(this.name);
    }
    
}
