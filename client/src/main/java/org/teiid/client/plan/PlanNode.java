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
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.metamatrix.core.util.ExternalizeUtil;

@XmlType
@XmlRootElement(name="node")
public class PlanNode implements Externalizable {

	@XmlType(name = "property")
	public static class Property implements Externalizable {
		@XmlAttribute
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
		
		@XmlElement(name="value")
		public List<String> getValues() {
			return values;
		}
		
		public void setValues(List<String> values) {
			this.values = values;
		}
		
		@XmlElement(name="node")
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
	
	@XmlElement(name="property")
    private List<Property> properties = new LinkedList<Property>();
    private PlanNode parent;
    @XmlAttribute
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
    	p.setValues(value);
    	this.properties.add(p);
    }
    
    public void addProperty(String pname, String value) {
    	Property p = new Property(pname);
    	p.setValues(Arrays.asList(value));
    	this.properties.add(p);
    }
    
    public String toXml() throws JAXBException {
    	JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {PlanNode.class});
		Marshaller marshaller = jc.createMarshaller();
		marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE); //$NON-NLS-1$
		StringWriter writer = new StringWriter();
		marshaller.marshal(this, writer);
		return writer.toString();
    }
    
    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	visitNode(this, 0, builder);
    	return builder.toString();
    }
    
    /* 
     * @see com.metamatrix.jdbc.plan.PlanVisitor#visitNode(com.metamatrix.jdbc.plan.PlanNode)
     */
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
        } else {
        	text.append(":"); //$NON-NLS-1$
        	text.append(p.getValues().get(0));
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
