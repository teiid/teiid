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
package org.teiid.adminapi.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.teiid.adminapi.AdminPlugin;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.VDBImport;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.core.types.XMLType;
import org.xml.sax.SAXException;

@SuppressWarnings("nls")
public class VDBMetadataParser {

    private boolean writePropertyElements;

    @Deprecated
    public static VDBMetaData unmarshell(InputStream content) throws XMLStreamException {
        return unmarshall(content);
    }
    public static VDBMetaData unmarshall(InputStream content) throws XMLStreamException {
         XMLInputFactory inputFactory=XMLType.getXmlInputFactory();
         XMLStreamReader reader = inputFactory.createXMLStreamReader(content);
         try {
            // elements
            while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
                Element element = Element.forName(reader.getLocalName());
                switch (element) {
                case VDB:
                    VDBMetaData vdb = new VDBMetaData();
                    Properties props = getAttributes(reader);
                    vdb.setName(props.getProperty(Element.NAME.getLocalName()));
                    String version = props.getProperty(Element.VERSION.getLocalName());
                    if (version != null) {
                        vdb.setVersion(version);
                    }
                    parseVDB(reader, vdb);
                    return vdb;
                 default:
                    throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element1",reader.getName(), Element.VDB.getLocalName()), reader.getLocation());
                }
            }
         } finally {
             try {
                content.close();
            } catch (IOException e) {
                Logger.getLogger(VDBMetadataParser.class.getName()).log(Level.FINE, "Exception closing vdb stream", e);
            }
         }
        return null;
    }

    public static void validate(InputStream content) throws SAXException, IOException {
        try {
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = schemaFactory.newSchema(VDBMetaData.class.getResource("/vdb-deployer.xsd")); //$NON-NLS-1$
            Validator v = schema.newValidator();
            v.validate(new StreamSource(content));
        } finally {
            content.close();
        }
    }

    private static void parseVDB(XMLStreamReader reader, VDBMetaData vdb) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case DESCRIPTION:
                vdb.setDescription(reader.getElementText());
                break;
            case CONNECTION_TYPE:
                vdb.setConnectionType(reader.getElementText());
                break;
            case PROPERTY:
                parseProperty(reader, vdb);
                break;
            case MODEL:
                ModelMetaData model = new ModelMetaData();
                parseModel(reader, model);
                vdb.addModel(model);
                break;
            case TRANSLATOR:
                VDBTranslatorMetaData translator = new VDBTranslatorMetaData();
                parseTranslator(reader, translator);
                vdb.addOverideTranslator(translator);
                break;
            case DATA_ROLE:
                DataPolicyMetadata policy = new DataPolicyMetadata();
                parseDataRole(reader, policy);
                vdb.addDataPolicy(policy);
                break;
            case IMPORT_VDB:
                VDBImportMetadata vdbImport = new VDBImportMetadata();
                Properties props = getAttributes(reader);
                vdbImport.setName(props.getProperty(Element.NAME.getLocalName()));
                String version = props.getProperty(Element.VERSION.getLocalName());
                if (version != null) {
                    vdbImport.setVersion(version);
                }
                vdbImport.setImportDataPolicies(Boolean.parseBoolean(props.getProperty(Element.IMPORT_POLICIES.getLocalName(), "true")));
                vdb.getVDBImports().add(vdbImport);
                ignoreTillEnd(reader);
                break;
            case ENTRY:
                EntryMetaData entry = new EntryMetaData();
                parseEntry(reader, entry);
                vdb.getEntries().add(entry);
                break;
             default:
                 throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element5",reader.getName(),
                         Element.DESCRIPTION.getLocalName(),
                         Element.PROPERTY.getLocalName(),
                         Element.MODEL.getLocalName(),
                         Element.TRANSLATOR.getLocalName(),
                         Element.DATA_ROLE.getLocalName()), reader.getLocation());
            }
        }
    }

    private static void ignoreTillEnd(XMLStreamReader reader)
            throws XMLStreamException {
        while(reader.nextTag() != XMLStreamConstants.END_ELEMENT) {
            ;
        }
    }

    private static void parseProperty(XMLStreamReader reader, AdminObjectImpl anObj)
            throws XMLStreamException {
        boolean text = false;
        if (reader.getAttributeCount() > 0) {
            String key = null;
            String value = null;
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                if (attrName.equals(Element.NAME.getLocalName())) {
                    key = attrValue;
                } else if (attrName.equals(Element.VALUE.getLocalName())) {
                    value = attrValue;
                }
            }
            if (value == null) {
                value = reader.getElementText();
                text = true;
            }
            anObj.addProperty(key, value);
        }
        if (!text) {
            ignoreTillEnd(reader);
        }
    }

    private static void parseDataRole(XMLStreamReader reader, DataPolicyMetadata policy) throws XMLStreamException {
        Properties props = getAttributes(reader);
        policy.setName(props.getProperty(Element.NAME.getLocalName()));
        policy.setAnyAuthenticated(Boolean.parseBoolean(props.getProperty(Element.DATA_ROLE_ANY_ATHENTICATED_ATTR.getLocalName())));
        policy.setGrantAll(Boolean.parseBoolean(props.getProperty(Element.DATA_ROLE_GRANT_ALL_ATTR.getLocalName())));
        policy.setAllowCreateTemporaryTables(Boolean.parseBoolean(props.getProperty(Element.DATA_ROLE_ALLOW_TEMP_TABLES_ATTR.getLocalName())));

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case DESCRIPTION:
                policy.setDescription(reader.getElementText());
                break;
            case PERMISSION:
                PermissionMetaData permission = new PermissionMetaData();
                parsePermission(reader, permission);
                policy.addPermission(permission);
                break;
            case MAPPED_ROLE_NAME:
                policy.addMappedRoleName(reader.getElementText());
                break;
             default:
                 throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element2",reader.getName(),
                         Element.DESCRIPTION.getLocalName(),
                         Element.PERMISSION.getLocalName(),
                         Element.MAPPED_ROLE_NAME.getLocalName()), reader.getLocation());
            }
        }
    }

    private static void parsePermission(XMLStreamReader reader, PermissionMetaData permission) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case RESOURCE_NAME:
                permission.setResourceName(reader.getElementText());
                break;
            case RESOURCE_TYPE:
                permission.setResourceType(ResourceType.valueOf(reader.getElementText()));
                break;
            case ALLOW_ALTER:
                permission.setAllowAlter(Boolean.parseBoolean(reader.getElementText()));
                break;
            case ALLOW_CREATE:
                permission.setAllowCreate(Boolean.parseBoolean(reader.getElementText()));
                break;
            case ALLOW_LANGUAGE:
                permission.setAllowLanguage(Boolean.parseBoolean(reader.getElementText()));
                break;
            case ALLOW_DELETE:
                permission.setAllowDelete(Boolean.parseBoolean(reader.getElementText()));
                break;
            case ALLOW_EXECUTE:
                permission.setAllowExecute(Boolean.parseBoolean(reader.getElementText()));
                break;
            case ALLOW_READ:
                permission.setAllowRead(Boolean.parseBoolean(reader.getElementText()));
                break;
            case ALLOW_UPADTE:
                permission.setAllowUpdate(Boolean.parseBoolean(reader.getElementText()));
                break;
            case CONDITION:
                if (reader.getAttributeCount() > 0) {
                    permission.setConstraint(Boolean.valueOf(reader.getAttributeValue(0)));
                }
                permission.setCondition(reader.getElementText());
                break;
            case MASK:
                if (reader.getAttributeCount() > 0) {
                    permission.setOrder(Integer.valueOf(reader.getAttributeValue(0)));
                }
                permission.setMask(reader.getElementText());
                break;
             default:
                 throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element7",reader.getName(),
                         Element.RESOURCE_NAME.getLocalName(),
                         Element.ALLOW_ALTER.getLocalName(),
                         Element.ALLOW_CREATE.getLocalName(),
                         Element.ALLOW_DELETE.getLocalName(),
                         Element.ALLOW_EXECUTE.getLocalName(),
                         Element.ALLOW_READ.getLocalName(),
                         Element.ALLOW_UPADTE.getLocalName(), Element.ALLOW_LANGUAGE.getLocalName(), Element.CONSTRAINT.getLocalName(),
                         Element.CONDITION.getLocalName(), Element.MASK.getLocalName()), reader.getLocation());
            }
        }
    }

    private static void parseTranslator(XMLStreamReader reader, VDBTranslatorMetaData translator) throws XMLStreamException {
        Properties props = getAttributes(reader);
        translator.setName(props.getProperty(Element.NAME.getLocalName()));
        translator.setType(props.getProperty(Element.TYPE.getLocalName()));
        translator.setDescription(props.getProperty(Element.DESCRIPTION.getLocalName()));

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case PROPERTY:
                parseProperty(reader, translator);
                break;
             default:
                 throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element1",reader.getName(),
                         Element.PROPERTY.getLocalName()), reader.getLocation());
            }
        }
    }

    private static void parseEntry(XMLStreamReader reader, EntryMetaData entry) throws XMLStreamException {
        Properties props = getAttributes(reader);
        entry.setPath(props.getProperty(Element.PATH.getLocalName()));
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case DESCRIPTION:
                entry.setDescription(reader.getElementText());
                break;
            case PROPERTY:
                parseProperty(reader, entry);
                break;
            default:
                throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element2",reader.getName(),
                        Element.DESCRIPTION.getLocalName(),
                        Element.PROPERTY.getLocalName()));
           }
       }
    }

    private static void parseModel(XMLStreamReader reader, ModelMetaData model) throws XMLStreamException {
        Properties props = getAttributes(reader);
        model.setName(props.getProperty(Element.NAME.getLocalName()));
        model.setModelType(props.getProperty(Element.TYPE.getLocalName(), "PHYSICAL"));
        model.setVisible(Boolean.parseBoolean(props.getProperty(Element.VISIBLE.getLocalName(), "true")));
        model.setPath(props.getProperty(Element.PATH.getLocalName()));

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case DESCRIPTION:
                model.setDescription(reader.getElementText());
                break;
            case PROPERTY:
                parseProperty(reader, model);
                break;
            case SOURCE:
                Properties sourceProps = getAttributes(reader);
                String name = sourceProps.getProperty(Element.NAME.getLocalName());
                String translatorName = sourceProps.getProperty(Element.SOURCE_TRANSLATOR_NAME_ATTR.getLocalName());
                String connectionName = sourceProps.getProperty(Element.SOURCE_CONNECTION_JNDI_NAME_ATTR.getLocalName());
                model.addSourceMapping(name, translatorName, connectionName);
                ignoreTillEnd(reader);
                break;
            case VALIDATION_ERROR:
                Properties validationProps = getAttributes(reader);
                String msg =  reader.getElementText();
                String severity = validationProps.getProperty(Element.VALIDATION_SEVERITY_ATTR.getLocalName());
                String path = validationProps.getProperty(Element.PATH.getLocalName());
                Message ve = new Message(Severity.valueOf(severity), msg);
                ve.setPath(path);
                model.addMessage(ve);
                break;
            case METADATA:
                Properties metdataProps = getAttributes(reader);
                String type = metdataProps.getProperty(Element.TYPE.getLocalName(), "DDL");
                String text = reader.getElementText();
                model.addSourceMetadata(type, text);
                break;
             default:
                 throw new XMLStreamException(AdminPlugin.Util.gs("unexpected_element5",reader.getName(),
                         Element.DESCRIPTION.getLocalName(),
                         Element.PROPERTY.getLocalName(),
                         Element.SOURCE.getLocalName(),
                         Element.METADATA.getLocalName(),
                         Element.VALIDATION_ERROR.getLocalName()), reader.getLocation());
            }
        }
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

    enum Element {
        // must be first
        UNKNOWN(null),
        VDB("vdb"),
        NAME("name"),
        VERSION("version"),
        DESCRIPTION("description"),
        CONNECTION_TYPE("connection-type"),
        PROPERTY("property"),
        VALUE("value"),
        MODEL("model"),
        IMPORT_VDB("import-vdb"),
        IMPORT_POLICIES("import-data-policies"),
        TYPE("type"),
        VISIBLE("visible"),
        PATH("path"),
        SOURCE("source"),
        SOURCE_TRANSLATOR_NAME_ATTR("translator-name"),
        SOURCE_CONNECTION_JNDI_NAME_ATTR("connection-jndi-name"),
        VALIDATION_ERROR("validation-error"),
        VALIDATION_SEVERITY_ATTR("severity"),
        TRANSLATOR("translator"),
        DATA_ROLE("data-role"),
        DATA_ROLE_ANY_ATHENTICATED_ATTR("any-authenticated"),
        DATA_ROLE_GRANT_ALL_ATTR("grant-all"),
        DATA_ROLE_ALLOW_TEMP_TABLES_ATTR("allow-create-temporary-tables"),
        PERMISSION("permission"),
        RESOURCE_NAME("resource-name"),
        RESOURCE_TYPE("resource-type"),
        ALLOW_CREATE("allow-create"),
        ALLOW_READ("allow-read"),
        ALLOW_UPADTE("allow-update"),
        ALLOW_DELETE("allow-delete"),
        ALLOW_EXECUTE("allow-execute"),
        ALLOW_ALTER("allow-alter"),
        ALLOW_LANGUAGE("allow-language"),
        CONDITION("condition"),
        MASK("mask"),
        ORDER("order"),
        CONSTRAINT("constraint"),
        MAPPED_ROLE_NAME("mapped-role-name"),
        ENTRY("entry"),
        METADATA("metadata");

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
                if (name != null) {
                    map.put(name, element);
                }
            }
            elements = map;
        }

        public static Element forName(String localName) {
            final Element element = elements.get(localName);
            return element == null ? UNKNOWN : element;
        }
    }

    @Deprecated
    public static void marshell(VDBMetaData vdb, OutputStream out) throws XMLStreamException, IOException {
        marshall(vdb, out);
    }

    public static void marshall(VDBMetaData vdb, OutputStream out) throws XMLStreamException, IOException {
        VDBMetadataParser parser = new VDBMetadataParser();
        parser.writeVDB(vdb, out);
    }

    private void writeVDB(VDBMetaData vdb, OutputStream out) throws XMLStreamException, IOException {
        XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(out);

        writer.writeStartDocument();
        writer.writeStartElement(Element.VDB.getLocalName());
        writeAttribute(writer, Element.NAME.getLocalName(), vdb.getName());
        writeAttribute(writer, Element.VERSION.getLocalName(), String.valueOf(vdb.getVersion()));

        if (vdb.getDescription() != null) {
            writeElement(writer, Element.DESCRIPTION, vdb.getDescription());
        }
        writeElement(writer, Element.CONNECTION_TYPE, vdb.getConnectionType().name());
        writeProperties(writer, vdb.getPropertiesMap());

        for (VDBImport vdbImport : vdb.getVDBImports()) {
            writer.writeStartElement(Element.IMPORT_VDB.getLocalName());
            writeAttribute(writer, Element.NAME.getLocalName(), vdbImport.getName());
            writeAttribute(writer, Element.VERSION.getLocalName(), String.valueOf(vdbImport.getVersion()));
            writeAttribute(writer, Element.IMPORT_POLICIES.getLocalName(), String.valueOf(vdbImport.isImportDataPolicies()));
            writer.writeEndElement();
        }

        // models
        Collection<ModelMetaData> models = vdb.getModelMetaDatas().values();
        for (ModelMetaData model:models) {
            if (vdb.getImportedModels().contains(model.getName())) {
                continue;
            }
            writeModel(writer, model);
        }

        // override translators
        for(VDBTranslatorMetaData translator:vdb.getOverrideTranslatorsMap().values()) {
            writeTranslator(writer, translator);
        }

        // data-roles
        for (DataPolicy dp:vdb.getDataPolicies()) {
            writeDataPolicy(writer, dp);
        }

        // entry
        // designer only
        for (EntryMetaData em:vdb.getEntries()) {
            writer.writeStartElement(Element.ENTRY.getLocalName());
            writeAttribute(writer, Element.PATH.getLocalName(), em.getPath());
            if (em.getDescription() != null) {
                writeElement(writer, Element.DESCRIPTION, em.getDescription());
            }
            writeProperties(writer, em.getPropertiesMap());
            writer.writeEndElement();
        }

        writer.writeEndElement();
        writer.writeEndDocument();
        writer.close();
        out.close();
    }

    private void writeDataPolicy(XMLStreamWriter writer, DataPolicy dp)  throws XMLStreamException {
        writer.writeStartElement(Element.DATA_ROLE.getLocalName());

        writeAttribute(writer, Element.NAME.getLocalName(), dp.getName());
        writeAttribute(writer, Element.DATA_ROLE_ANY_ATHENTICATED_ATTR.getLocalName(), String.valueOf(dp.isAnyAuthenticated()));
        writeAttribute(writer, Element.DATA_ROLE_GRANT_ALL_ATTR.getLocalName(), String.valueOf(dp.isGrantAll()));

        if(dp.isAllowCreateTemporaryTables() !=null) {
            writeAttribute(writer, Element.DATA_ROLE_ALLOW_TEMP_TABLES_ATTR.getLocalName(), String.valueOf(dp.isAllowCreateTemporaryTables()));
        }

        if (dp.getDescription() != null) {
            writeElement(writer, Element.DESCRIPTION, dp.getDescription());
        }

        // permission
        for (DataPolicy.DataPermission permission: dp.getPermissions()) {
            writer.writeStartElement(Element.PERMISSION.getLocalName());
            writeElement(writer, Element.RESOURCE_NAME, permission.getResourceName());
            if (permission.getResourceType() != null) {
                writeElement(writer, Element.RESOURCE_TYPE, permission.getResourceType().name());
            }
            if (permission.getAllowCreate() != null) {
                writeElement(writer, Element.ALLOW_CREATE, permission.getAllowCreate().toString());
            }
            if (permission.getAllowRead() != null) {
                writeElement(writer, Element.ALLOW_READ, permission.getAllowRead().toString());
            }
            if (permission.getAllowUpdate() != null) {
                writeElement(writer, Element.ALLOW_UPADTE, permission.getAllowUpdate().toString());
            }
            if (permission.getAllowDelete() != null) {
                writeElement(writer, Element.ALLOW_DELETE, permission.getAllowDelete().toString());
            }
            if (permission.getAllowExecute() != null) {
                writeElement(writer, Element.ALLOW_EXECUTE, permission.getAllowExecute().toString());
            }
            if (permission.getAllowAlter() != null) {
                writeElement(writer, Element.ALLOW_ALTER, permission.getAllowAlter().toString());
            }
            if (permission.getAllowLanguage() != null) {
                writeElement(writer, Element.ALLOW_LANGUAGE, permission.getAllowLanguage().toString());
            }
            if (permission.getCondition() != null) {
                if (permission.getConstraint() != null) {
                    writeElement(writer, Element.CONDITION, permission.getCondition(), new String[] {Element.CONSTRAINT.getLocalName(), String.valueOf(permission.getConstraint())});
                } else {
                    writeElement(writer, Element.CONDITION, permission.getCondition());
                }
            }
            if (permission.getMask() != null) {
                if (permission.getOrder() != null) {
                    writeElement(writer, Element.MASK, permission.getMask(), new String[] {Element.ORDER.getLocalName(), String.valueOf(permission.getOrder())});
                } else {
                    writeElement(writer, Element.MASK, permission.getMask());
                }
            }
            writer.writeEndElement();
        }

        // mapped role names
        for (String roleName:dp.getMappedRoleNames()) {
            writeElement(writer, Element.MAPPED_ROLE_NAME, roleName);
        }

        writer.writeEndElement();
    }

    private void writeTranslator(final XMLStreamWriter writer, VDBTranslatorMetaData translator)  throws XMLStreamException  {
        writer.writeStartElement(Element.TRANSLATOR.getLocalName());

        writeAttribute(writer, Element.NAME.getLocalName(), translator.getName());
        writeAttribute(writer, Element.TYPE.getLocalName(), translator.getType());
        writeAttribute(writer, Element.DESCRIPTION.getLocalName(), translator.getDescription());

        writeProperties(writer, translator.getPropertiesMap());

        writer.writeEndElement();
    }

    private void writeModel(final XMLStreamWriter writer, ModelMetaData model) throws XMLStreamException {
        writer.writeStartElement(Element.MODEL.getLocalName());
        writeAttribute(writer, Element.NAME.getLocalName(), model.getName());
        writeAttribute(writer, Element.TYPE.getLocalName(), model.getModelType().name());

        writeAttribute(writer, Element.VISIBLE.getLocalName(), String.valueOf(model.isVisible()));
        writeAttribute(writer, Element.PATH.getLocalName(), model.getPath());

        if (model.getDescription() != null) {
            writeElement(writer, Element.DESCRIPTION, model.getDescription());
        }
        writeProperties(writer, model.getPropertiesMap());

        // source mappings
        for (SourceMappingMetadata source:model.getSourceMappings()) {
            writer.writeStartElement(Element.SOURCE.getLocalName());
            writeAttribute(writer, Element.NAME.getLocalName(), source.getName());
            writeAttribute(writer, Element.SOURCE_TRANSLATOR_NAME_ATTR.getLocalName(), source.getTranslatorName());
            writeAttribute(writer, Element.SOURCE_CONNECTION_JNDI_NAME_ATTR.getLocalName(), source.getConnectionJndiName());
            writer.writeEndElement();
        }

        for (int i = 0; i < model.getSourceMetadataType().size(); i++) {
            writer.writeStartElement(Element.METADATA.getLocalName());
            writeAttribute(writer, Element.TYPE.getLocalName(), model.getSourceMetadataType().get(i));
            if (model.getSourceMetadataText().get(i) != null) {
                writer.writeCData(model.getSourceMetadataText().get(i));
            }
            writer.writeEndElement();
        }

        // model validation errors
        for (Message ve:model.getMessages(false)) {
            if (ve.getSeverity() == Severity.INFO) {
                continue; //info should be ephemeral
            }
            writer.writeStartElement(Element.VALIDATION_ERROR.getLocalName());
            writeAttribute(writer, Element.VALIDATION_SEVERITY_ATTR.getLocalName(), ve.getSeverity().name());
            writeAttribute(writer, Element.PATH.getLocalName(), ve.getPath());
            writer.writeCharacters(ve.getValue());
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }

    private void writeProperties(final XMLStreamWriter writer, Map<String, String> props)  throws XMLStreamException  {
        for (Map.Entry<String, String> prop : props.entrySet()) {
            writer.writeStartElement(Element.PROPERTY.getLocalName());
            String key = prop.getKey();
            String value = prop.getValue();
            writeAttribute(writer, Element.NAME.getLocalName(), key);
            if (value != null) {
                if (writePropertyElements) {
                    writer.writeCharacters(value);
                } else {
                    writeAttribute(writer, Element.VALUE.getLocalName(), value);
                }
            }
            writer.writeEndElement();
        }
    }

    private void writeAttribute(XMLStreamWriter writer,
            String localName, String value) throws XMLStreamException {
        if (value != null) {
            writer.writeAttribute(localName, value);
        }
    }

    private void writeElement(final XMLStreamWriter writer, final Element element, String value, String[] ... attributes) throws XMLStreamException {
        writer.writeStartElement(element.getLocalName());
        for (String[] attribute : attributes) {
            writeAttribute(writer, attribute[0], attribute[1]);
        }
        writer.writeCharacters(value);
        writer.writeEndElement();
    }

}
