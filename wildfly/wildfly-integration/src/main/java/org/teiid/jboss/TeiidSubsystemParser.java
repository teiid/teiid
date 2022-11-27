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
package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;
import static org.jboss.as.controller.parsing.ParseUtils.*;
import static org.teiid.jboss.TeiidConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.as.controller.persistence.SubsystemMarshallingContext;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLElementWriter;
import org.jboss.staxmapper.XMLExtendedStreamReader;
import org.jboss.staxmapper.XMLExtendedStreamWriter;

class TeiidSubsystemParser implements XMLStreamConstants, XMLElementReader<List<ModelNode>>, XMLElementWriter<SubsystemMarshallingContext> {
    public static TeiidSubsystemParser INSTANCE = new TeiidSubsystemParser();

    @Override
    public void writeContent(final XMLExtendedStreamWriter writer, final SubsystemMarshallingContext context) throws XMLStreamException {
        context.startSubsystemElement(Namespace.CURRENT.getUri(), false);
        ModelNode node = context.getModelNode();
        if (!node.isDefined()) {
            return;
        }

        ALLOW_ENV_FUNCTION_ELEMENT.marshallAsElement(node, false, writer);

        if (like(node, Element.ASYNC_THREAD_POOL_ELEMENT)){
            writer.writeStartElement(Element.ASYNC_THREAD_POOL_ELEMENT.getLocalName());
            writeThreadConfiguration(writer, node);
            writer.writeEndElement();
        }

        if (like(node, Element.BUFFER_MANAGER_ELEMENT) || like(node, Element.BUFFER_SERVICE_ELEMENT)){
            writer.writeStartElement(Element.BUFFER_MANAGER_ELEMENT.getLocalName());
            writeBufferManager(writer, node);
            writer.writeEndElement();
        }

        MAX_THREADS_ELEMENT.marshallAsElement(node, false, writer);
        MAX_ACTIVE_PLANS_ELEMENT.marshallAsElement(node, false, writer);
        USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.marshallAsElement(node, false, writer);
        TIME_SLICE_IN_MILLI_ELEMENT.marshallAsElement(node, false, writer);
        MAX_ROWS_FETCH_SIZE_ELEMENT.marshallAsElement(node, false, writer);
        LOB_CHUNK_SIZE_IN_KB_ELEMENT.marshallAsElement(node, false, writer);
        QUERY_THRESHOLD_IN_SECS_ELEMENT.marshallAsElement(node, false, writer);
        MAX_SOURCE_ROWS_ELEMENT.marshallAsElement(node, false, writer);
        EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.marshallAsElement(node, false, writer);
        DETECTING_CHANGE_EVENTS_ELEMENT.marshallAsElement(node, false, writer);
        QUERY_TIMEOUT.marshallAsElement(node, false, writer);
        WORKMANAGER.marshallAsElement(node, false, writer);

        DATA_ROLES_REQUIRED_ELEMENT.marshallAsElement(node, false, writer);
        AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.marshallAsElement(node, false, writer);
        POLICY_DECIDER_MODULE_ELEMENT.marshallAsElement(node, false, writer);

        PREPARSER_MODULE_ELEMENT.marshallAsElement(node, false, writer);

        if (like(node, Element.RESULTSET_CACHE_ELEMENT)){
            writer.writeStartElement(Element.RESULTSET_CACHE_ELEMENT.getLocalName());
            writeResultsetCacheConfiguration(writer, node);
            writer.writeEndElement();
        }

        if (like(node, Element.PREPAREDPLAN_CACHE_ELEMENT)){
            writer.writeStartElement(Element.PREPAREDPLAN_CACHE_ELEMENT.getLocalName());
            writePreparedPlanCacheConfiguration(writer, node);
            writer.writeEndElement();
        }

        if (like(node, Element.DISTRIBUTED_CACHE)){
            writer.writeStartElement(Element.DISTRIBUTED_CACHE.getLocalName());
            writeObjectReplicatorConfiguration(writer, node);
            writer.writeEndElement();
        }

        // authentication
        if (like(node, Element.AUTHENTICATION_ELEMENT)) {
            writer.writeStartElement(Element.AUTHENTICATION_ELEMENT.getLocalName());
            AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            AUTHENTICATION_TYPE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            writer.writeEndElement();
        }

        if (has(node, Element.TRANSPORT_ELEMENT.getLocalName())) {
            ArrayList<String> transports = new ArrayList<String>(node.get(Element.TRANSPORT_ELEMENT.getLocalName()).keys());
            if (!transports.isEmpty()) {
                for (String transport:transports) {
                    writer.writeStartElement(Element.TRANSPORT_ELEMENT.getLocalName());
                    writeTransportConfiguration(writer, node.get(Element.TRANSPORT_ELEMENT.getLocalName(), transport), transport);
                    writer.writeEndElement();
                }
            }
        }

        if (has(node, Element.TRANSLATOR_ELEMENT.getLocalName())) {
            ArrayList<String> translators = new ArrayList<String>(node.get(Element.TRANSLATOR_ELEMENT.getLocalName()).keys());
            if (!translators.isEmpty()) {
                for (String translator:translators) {
                    writer.writeStartElement(Element.TRANSLATOR_ELEMENT.getLocalName());
                    writeTranslator(writer, node.get(Element.TRANSLATOR_ELEMENT.getLocalName(), translator), translator);
                    writer.writeEndElement();
                }
            }
        }

        writer.writeEndElement(); // End of subsystem element
    }

    private void writeThreadConfiguration(XMLExtendedStreamWriter writer,
            ModelNode node) throws XMLStreamException {
        THREAD_COUNT_ATTRIBUTE.marshallAsAttribute(node, false, writer);
    }

    private void writeObjectReplicatorConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
        DC_STACK_ATTRIBUTE.marshallAsAttribute(node, false, writer);
    }

    private void writeTranslator(XMLExtendedStreamWriter writer, ModelNode node, String translatorName) throws XMLStreamException {
        writer.writeAttribute(Element.TRANSLATOR_NAME_ATTRIBUTE.getLocalName(), translatorName);
        TRANSLATOR_MODULE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        TRANSLATOR_SLOT_ATTRIBUTE.marshallAsAttribute(node, false, writer);
    }

    // write the elements according to the schema defined.
    private void writeTransportConfiguration( XMLExtendedStreamWriter writer, ModelNode node, String transportName) throws XMLStreamException {

        writer.writeAttribute(Element.TRANSPORT_NAME_ATTRIBUTE.getLocalName(), transportName);
        TRANSPORT_SOCKET_BINDING_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        TRANSPORT_PROTOCOL_ATTRIBUTE.marshallAsAttribute(node, true, writer);
        TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);

        if (like(node, Element.PG_ELEMENT)) {
            writer.writeStartElement(Element.PG_ELEMENT.getLocalName());
            PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.marshallAsAttribute(node, false, writer);
            writer.writeEndElement();
        }

        if (like(node, Element.SSL_ELEMENT)) {
            writer.writeStartElement(Element.SSL_ELEMENT.getLocalName());

            SSL_MODE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            SSL_AUTH_MODE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            SSL_SSL_PROTOCOL_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.marshallAsAttribute(node, false, writer);
            SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.marshallAsAttribute(node, false, writer);

            if (like(node, Element.SSL_KETSTORE_ELEMENT)) {
                writer.writeStartElement(Element.SSL_KETSTORE_ELEMENT.getLocalName());
                SSL_KETSTORE_NAME_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                SSL_KETSTORE_PASSWORD_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                SSL_KETSTORE_TYPE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                SSL_KETSTORE_ALIAS_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                writer.writeEndElement();
            }

            if (like(node, Element.SSL_TRUSTSTORE_ELEMENT)) {
                writer.writeStartElement(Element.SSL_TRUSTSTORE_ELEMENT.getLocalName());
                SSL_TRUSTSTORE_NAME_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                SSL_TRUSTSTORE_CHECK_EXPIRED_ATTRIBUTE.marshallAsAttribute(node, false, writer);
                writer.writeEndElement();
            }
            writer.writeEndElement();
        }
    }

    private void writeBufferManager(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
        //if using a cli add, we end up here - the xml name has already been changed
        USE_DISK_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        INLINE_LOBS.marshallAsAttribute(node, false, writer);
        PROCESSOR_BATCH_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        MAX_PROCESSING_KB_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        MAX_FILE_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        MAX_BUFFER_SPACE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        MAX_OPEN_FILES_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        MEMORY_BUFFER_SPACE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        ENCRYPT_FILES_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        //values need adjusted
        writeAdjustedValue(writer, node, MAX_RESERVED_KB_ATTRIBUTE);
        writeAdjustedValue(writer, node, MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE);

        BUFFER_MANAGER_USE_DISK_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_INLINE_LOBS.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE.marshallAsAttribute(node, false, writer);
    }

    private void writeAdjustedValue(XMLExtendedStreamWriter writer,
            ModelNode node, SimpleAttributeDefinition element) throws XMLStreamException {
        String name = element.getName();
        if (node.hasDefined(name)) {
            int value = node.get(name).asInt();
            if (value > 0) {
                value = value/1024;
            }
            writer.writeAttribute(element.getXmlName(), String.valueOf(value));
        }
    }

    private void writeResultsetCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
        RSC_NAME_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        RSC_CONTAINER_NAME_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        RSC_ENABLE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        RSC_MAX_STALENESS_ATTRIBUTE.marshallAsAttribute(node, false, writer);
    }

    private void writePreparedPlanCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
        PPC_NAME_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        PPC_CONTAINER_NAME_ATTRIBUTE.marshallAsAttribute(node, false, writer);
        PPC_ENABLE_ATTRIBUTE.marshallAsAttribute(node, false, writer);
    }

    private boolean has(ModelNode node, String name) {
        return node.has(name) && node.get(name).isDefined();
    }

    private boolean like(ModelNode node, Element element) {
        if (node.isDefined()) {
            Set<String> keys = node.keys();
            for (String key:keys) {
                if (key.startsWith(element.getLocalName()) && node.get(key).isDefined()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void readElement(final XMLExtendedStreamReader reader, final List<ModelNode> list) throws XMLStreamException {
        final ModelNode address = new ModelNode();
        address.add(SUBSYSTEM, TeiidExtension.TEIID_SUBSYSTEM);
        address.protect();

        final ModelNode bootServices = new ModelNode();
        bootServices.get(OP).set(ADD);
        bootServices.get(OP_ADDR).set(address);
        list.add(bootServices);

        // no attributes
        requireNoAttributes(reader);

        // elements
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            switch (Namespace.forUri(reader.getNamespaceURI())) {
                case TEIID_1_2:
                case TEIID_1_1: {
                    Element element = Element.forName(reader.getLocalName());
                    switch (element) {
                    case ALLOW_ENV_FUNCTION_ELEMENT:
                    case EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT:
                    case DETECTING_CHANGE_EVENTS_ELEMENT:
                    case DATA_ROLES_REQUIRED_ELEMENT:
                        bootServices.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
                        break;

                    case POLICY_DECIDER_MODULE_ELEMENT:
                    case AUTHORIZATION_VALIDATOR_MODULE_ELEMENT:
                    case PREPARSER_MODULE_ELEMENT:
                    case WORKMANAGER:
                        bootServices.get(reader.getLocalName()).set(reader.getElementText());
                        break;
                    case MAX_THREADS_ELEMENT:
                    case MAX_ACTIVE_PLANS_ELEMENT:
                    case USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT:
                    case TIME_SLICE_IN_MILLI_ELEMENT:
                    case MAX_ROWS_FETCH_SIZE_ELEMENT:
                    case LOB_CHUNK_SIZE_IN_KB_ELEMENT:
                    case QUERY_THRESHOLD_IN_SECS_ELEMENT:
                    case MAX_SOURCE_ROWS_ELEMENT:
                    case QUERY_TIMEOUT:
                        bootServices.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
                        break;

                    case ASYNC_THREAD_POOL_ELEMENT:
                        parseAsyncThreadConfiguration(reader, bootServices);
                        break;

                      // complex types
                    case DISTRIBUTED_CACHE:
                        parseObjectReplicator(reader, bootServices);
                        break;

                    case BUFFER_SERVICE_ELEMENT:
                        if (Namespace.forUri(reader.getNamespaceURI()) == Namespace.TEIID_1_2) {
                            throw ParseUtils.unexpectedElement(reader);
                        }
                        parseBufferService(reader, bootServices);
                        break;

                    case BUFFER_MANAGER_ELEMENT:
                        parseBufferManager(reader, bootServices);
                        break;

                    case PREPAREDPLAN_CACHE_ELEMENT:
                        parsePreparedPlanCacheConfiguration(reader, bootServices);
                        break;

                    case RESULTSET_CACHE_ELEMENT:
                        parseResultsetCacheConfiguration(reader, bootServices);
                        break;

                    case TRANSPORT_ELEMENT:
                        ModelNode transport = new ModelNode();

                        String name = parseTransport(reader, transport);
                        if (name != null) {
                            final ModelNode transportAddress = address.clone();
                            transportAddress.add("transport", name); //$NON-NLS-1$
                            transportAddress.protect();
                            transport.get(OP).set(ADD);
                            transport.get(OP_ADDR).set(transportAddress);

                            list.add(transport);
                        }
                        else {
                            throw new XMLStreamException();
                        }
                        break;

                    case TRANSLATOR_ELEMENT:
                        ModelNode translatorNode = new ModelNode();

                        String translatorName = parseTranslator(reader, translatorNode);

                        if (translatorName != null) {
                            final ModelNode translatorAddress = address.clone();
                            translatorAddress.add("translator", translatorName); //$NON-NLS-1$
                            translatorAddress.protect();
                            translatorNode.get(OP).set(ADD);
                            translatorNode.get(OP_ADDR).set(translatorAddress);

                            list.add(translatorNode);
                        }
                        else {
                            throw new XMLStreamException();
                        }
                        break;

                    case AUTHENTICATION_ELEMENT:
                        parseAuthentication(reader, bootServices);
                        break;
                     default:
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
    }

    private ModelNode parseAsyncThreadConfiguration(XMLExtendedStreamReader reader,
            ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.ASYNC_THREAD_POOL_ELEMENT);
                switch(element) {
                case THREAD_COUNT_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parseObjectReplicator(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);

                Element element = Element.forName(attrName, Element.DISTRIBUTED_CACHE);
                switch(element) {
                case DC_STACK_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private String parseTransport(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        String transportName = null;
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName);
                switch(element) {
                case TRANSPORT_NAME_ATTRIBUTE:
                case TRANSLATOR_NAME_ATTRIBUTE:
                    transportName = attrValue;
                    break;
                case TRANSPORT_SOCKET_BINDING_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                case TRANSPORT_PROTOCOL_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                case TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case PG_ELEMENT:
                    parsePg(reader, node);
                    break;
                case SSL_ELEMENT:
                    parseSSL(reader, node);
                    break;

                default:
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
        return transportName;
    }

    private ModelNode parseAuthentication(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.AUTHENTICATION_ELEMENT);

                switch(element) {
                case AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                case AUTHENTICATION_TYPE_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;

                case AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;

                case AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;

                case AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parsePg(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.PG_ELEMENT);

                switch(element) {
                case PG_MAX_LOB_SIZE_ALLOWED_ELEMENT:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parseSSL(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.SSL_ELEMENT);

                switch(element) {
                case SSL_MODE_ATTRIBUTE:
                case SSL_AUTH_MODE_ATTRIBUTE:
                case SSL_SSL_PROTOCOL_ATTRIBUTE:
                case SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE:
                case SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE:
                case SSL_KETSTORE_ALIAS_ATTRIBUTE:
                case SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case SSL_KETSTORE_ELEMENT:
                    parseKeystore(reader, node);
                    break;
                case SSL_TRUSTSTORE_ELEMENT:
                    parseTruststore(reader, node);
                    break;

                default:
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
        return node;
    }

    private ModelNode parseKeystore(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.SSL_KETSTORE_ELEMENT);

                switch(element) {
                case SSL_KETSTORE_NAME_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;

                case SSL_KETSTORE_PASSWORD_ATTRIBUTE:
                    node.get(element.getModelName()).setExpression(attrValue);
                    break;

                case SSL_KETSTORE_TYPE_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;

                case SSL_KETSTORE_ALIAS_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;

                case SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE:
                    node.get(element.getModelName()).setExpression(attrValue);
                    break;

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parseTruststore(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.SSL_TRUSTSTORE_ELEMENT);

                switch(element) {
                case SSL_TRUSTSTORE_NAME_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;

                case SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE:
                    node.get(element.getModelName()).setExpression(attrValue);
                    break;

                case SSL_TRUSTSTORE_CHECK_EXIRIED_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parseBufferService(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {

        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.BUFFER_SERVICE_ELEMENT);

                switch(element) {
                case USE_DISK_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_USE_DISK_ATTRIBUTE.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case INLINE_LOBS:
                    node.get(Element.BUFFER_MANAGER_INLINE_LOBS.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case PROCESSOR_BATCH_SIZE_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case MAX_PROCESSING_KB_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case MAX_RESERVED_KB_ATTRIBUTE:
                    int val = Integer.parseInt(attrValue);
                    if (val > 0) {
                        val = val / 1024;
                    }
                    node.get(Element.BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE.getModelName()).set(val);
                    break;
                case MAX_OPEN_FILES_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case MAX_FILE_SIZE_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE.getModelName()).set(Long.parseLong(attrValue));
                    break;
                case MAX_BUFFER_SPACE_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE.getModelName()).set(Long.parseLong(attrValue));
                    break;
                case MEMORY_BUFFER_SPACE_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case MEMORY_BUFFER_OFFHEAP_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE:
                    val = Integer.parseInt(attrValue);
                    if (val > 0) {
                        val = val / 1024;
                    }
                    node.get(Element.BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.getModelName()).set(val);
                    break;
                case ENCRYPT_FILES_ATTRIBUTE:
                    node.get(Element.BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parseBufferManager(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {

        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.BUFFER_MANAGER_ELEMENT);

                switch(element) {
                case BUFFER_MANAGER_USE_DISK_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case BUFFER_MANAGER_INLINE_LOBS:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Long.parseLong(attrValue));
                    break;
                case BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Long.parseLong(attrValue));
                    break;
                case BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parsePreparedPlanCacheConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.PREPAREDPLAN_CACHE_ELEMENT);
                switch(element) {
                case PPC_CONTAINER_NAME_ELEMENT:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                case PPC_ENABLE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case PPC_NAME_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private ModelNode parseResultsetCacheConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);
                Element element = Element.forName(attrName, Element.RESULTSET_CACHE_ELEMENT);
                switch(element) {
                case RSC_CONTAINER_NAME_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                case RSC_ENABLE_ATTRIBUTE:
                    node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
                    break;
                case RSC_MAX_STALENESS_ATTRIBUTE:
                    node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                    break;
                case RSC_NAME_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
        return node;
    }

    private String parseTranslator(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        String translatorName = null;
        if (reader.getAttributeCount() > 0) {
            for(int i=0; i<reader.getAttributeCount(); i++) {
                String attrName = reader.getAttributeLocalName(i);
                String attrValue = reader.getAttributeValue(i);

                Element element = Element.forName(attrName);
                switch(element) {
                case TRANSPORT_NAME_ATTRIBUTE:
                case TRANSLATOR_NAME_ATTRIBUTE:
                    translatorName = attrValue;
                    break;
                case TRANSLATOR_MODULE_ATTRIBUTE:
                case TRANSLATOR_SLOT_ATTRIBUTE:
                    node.get(element.getModelName()).set(attrValue);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            throw ParseUtils.unexpectedElement(reader);
        }
        return translatorName;
    }
}
