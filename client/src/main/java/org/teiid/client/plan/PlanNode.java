/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.LinkedHashMap;
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

    private LinkedHashMap<String, Property> properties = new LinkedHashMap<String, PlanNode.Property>();
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
        return new ArrayList<PlanNode.Property>(properties.values());
    }

    public void addProperty(String pname, PlanNode value) {
        Property p = new Property(pname);
        p.setPlanNode(value);
        value.setParent(this);
        this.properties.put(pname, p);
    }

    public void addProperty(String pname, List<String> value) {
        Property p = new Property(pname);
        if (value == null) {
            value = Collections.emptyList();
        }
        p.setValues(value);
        this.properties.put(pname, p);
    }

    public void addProperty(String pname, String value) {
        Property p = new Property(pname);
        if (value == null) {
            p.setValues(new ArrayList<String>(0));
        } else {
            p.setValues(Arrays.asList(value));
        }
        this.properties.put(pname, p);
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
                if (value != null) {
                    writeElement(writer, "value", value); //$NON-NLS-1$
                }
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
        if (value != null) {
            writer.writeCharacters(value);
        }
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
                   if (values != null && valueNode.equals("value")) {//$NON-NLS-1$
                       values.add(reader.getElementText());
                   }
                   else if (valueNode.equals("node")) {//$NON-NLS-1$
                       values = null;
                       Properties nodeProps = getAttributes(reader);
                       PlanNode childNode = new PlanNode(nodeProps.getProperty("name"));//$NON-NLS-1$
                       node.addProperty(props.getProperty("name"), buildNode(reader, childNode));//$NON-NLS-1$
                       continue;
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
        visitNode(this, 0, false, builder);
        return builder.toString();
    }

    public String toYaml() {
        StringBuilder builder = new StringBuilder();
        visitNode(this, 0, true, builder);
        return builder.toString();
    }

    protected void visitNode(PlanNode node, int nodeLevel, boolean yaml, StringBuilder text) {
        for(int i=0; i<nodeLevel; i++) {
            text.append("  "); //$NON-NLS-1$
        }
        text.append(node.getName());
        if (yaml) {
            text.append(":\n"); //$NON-NLS-1$
        } else {
            text.append("\n"); //$NON-NLS-1$
        }

        // Print properties appropriately
        int propTabs = nodeLevel + 1;
        for (PlanNode.Property property : node.getProperties()) {
            // Print leading spaces for prop name
            for(int t=0; t<propTabs; t++) {
                text.append("  "); //$NON-NLS-1$
            }
            printProperty(nodeLevel, property, yaml, text);
        }
    }

    private void printProperty(int nodeLevel, Property p, boolean yaml, StringBuilder text) {
        if (!yaml) {
            text.append("+ "); //$NON-NLS-1$
        }
        text.append(p.getName());

        if (p.getPlanNode() != null) {
            text.append(":\n"); //$NON-NLS-1$
            visitNode(p.getPlanNode(), nodeLevel + 2, yaml, text);
        } else if (p.getValues().size() > 1){
            text.append(":\n"); //$NON-NLS-1$
            for (int i = 0; i < p.getValues().size(); i++) {
                for(int t=0; t<nodeLevel+2; t++) {
                    text.append("  "); //$NON-NLS-1$
                }
                if (yaml) {
                    text.append("- "); //$NON-NLS-1$
                } else {
                    text.append(i);
                    text.append(": "); //$NON-NLS-1$
                }
                text.append(p.getValues().get(i));
                text.append("\n"); //$NON-NLS-1$
            }
        } else if (p.getValues().size() == 1) {
            if (yaml) {
                text.append(": "); //$NON-NLS-1$
            } else {
                text.append(":"); //$NON-NLS-1$
            }
            text.append(p.getValues().get(0));
            text.append("\n"); //$NON-NLS-1$
        } else {
            if (yaml) {
                text.append(": ~\n"); //$NON-NLS-1$
            } else {
                text.append("\n"); //$NON-NLS-1$
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.properties = new LinkedHashMap<String, PlanNode.Property>();
        for (Property p : ExternalizeUtil.readList(in, Property.class)) {
            this.properties.put(p.name, p);
        }
        this.parent = (PlanNode)in.readObject();
        this.name = (String)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ExternalizeUtil.writeCollection(out, this.properties.values());
        out.writeObject(this.parent);
        out.writeObject(this.name);
    }

    public Property getProperty(String pName) {
        return this.properties.get(pName);
    }

}
