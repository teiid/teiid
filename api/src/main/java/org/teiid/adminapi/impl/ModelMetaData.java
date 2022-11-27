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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.CopyOnWriteLinkedHashMap;


public class ModelMetaData extends AdminObjectImpl implements Model {

    private static final List<String> RESERVED_NAMES = Collections.unmodifiableList(Arrays.asList(
            CoreConstants.INFORMATION_SCHEMA, CoreConstants.ODBC_MODEL,
            CoreConstants.SYSTEM_ADMIN_MODEL, CoreConstants.SYSTEM_MODEL));

    private static final int DEFAULT_ERROR_HISTORY = 10;
    private static final String SUPPORTS_MULTI_SOURCE_BINDINGS_KEY_OLD = "supports-multi-source-bindings"; //$NON-NLS-1$
    private static final String SUPPORTS_MULTI_SOURCE_BINDINGS_KEY = "multisource"; //$NON-NLS-1$
    private static final long serialVersionUID = 3714234763056162230L;

    protected Map<String, SourceMappingMetadata> sources = new CopyOnWriteLinkedHashMap<String, SourceMappingMetadata>();
    protected String modelType = Type.PHYSICAL.name();
    protected String description;
    protected String path;
    protected boolean visible = true;
    protected List<Message> messages;
    protected transient List<Message> runtimeMessages;
    protected List<String> sourceMetadataType = new ArrayList<String>();
    protected List<String> sourceMetadataText = new ArrayList<String>();
    protected MetadataStatus metadataStatus = MetadataStatus.LOADING;

    @Override
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean isSource() {
        return getModelType() == Model.Type.PHYSICAL;
    }

    @Override
    public boolean isVisible() {
        return this.visible;
    }

    @Override
    public Type getModelType() {
        try {
            return Type.valueOf(modelType);
        } catch(IllegalArgumentException e) {
            return Type.OTHER;
        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public boolean isSupportsMultiSourceBindings() {
        return this.isSource() &&
                (this.sources.size() > 1 || Boolean.parseBoolean(getPropertyValue(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY)) || Boolean.parseBoolean(getPropertyValue(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY_OLD)));
    }

    public void setSupportsMultiSourceBindings(boolean supports) {
        addProperty(SUPPORTS_MULTI_SOURCE_BINDINGS_KEY, Boolean.toString(supports));
    }

    public void setModelType(Model.Type modelType) {
        this.modelType = modelType.name();
    }

    public void setModelType(String modelType) {
        if (modelType != null) {
            this.modelType = modelType.toUpperCase();
        } else {
            this.modelType = null;
        }
    }

    @Override
    public MetadataStatus getMetadataStatus() {
        return metadataStatus;
    }

    public void setMetadataStatus(Model.MetadataStatus status) {
        this.metadataStatus = status;
    }

    public void setMetadataStatus(String status) {
        if (status != null) {
            this.metadataStatus = Model.MetadataStatus.valueOf(status);
        }
    }

    public String toString() {
        return getName() + this.sources;
    }

    public void setVisible(boolean value) {
        this.visible = value;
    }

    public Collection<SourceMappingMetadata> getSourceMappings(){
        return this.sources.values();
    }

    public Map<String, SourceMappingMetadata> getSources() {
        return sources;
    }

    public SourceMappingMetadata getSourceMapping(String sourceName){
        return this.sources.get(sourceName);
    }

    public void setSourceMappings(List<SourceMappingMetadata> sources){
        this.sources.clear();
        for (SourceMappingMetadata source: sources) {
            addSourceMapping(source.getName(), source.getTranslatorName(), source.getConnectionJndiName());
        }
    }

    @Override
    public List<String> getSourceNames() {
        return new ArrayList<String>(this.sources.keySet());
    }

    @Override
    public String getSourceConnectionJndiName(String sourceName) {
        SourceMappingMetadata s = this.sources.get(sourceName);
        if (s == null) {
            return null;
        }
        return s.getConnectionJndiName();
    }

    @Override
    public String getSourceTranslatorName(String sourceName) {
        SourceMappingMetadata s = this.sources.get(sourceName);
        if (s == null) {
            return null;
        }
        return s.getTranslatorName();
    }

    public SourceMappingMetadata addSourceMapping(String name, String translatorName, String connJndiName) {
        return this.sources.put(name, new SourceMappingMetadata(name, translatorName, connJndiName));
    }

    public void addSourceMapping(SourceMappingMetadata source) {
        this.addSourceMapping(source.getName(), source.getTranslatorName(), source.getConnectionJndiName());
    }

    public synchronized boolean hasErrors() {
        if (this.messages != null) {
            for (Message error : this.messages) {
                if (error.getSeverity() == Severity.ERROR) {
                    return true;
                }
            }
        }
        if (this.runtimeMessages != null) {
            for (Message error : this.runtimeMessages) {
                if (error.getSeverity() == Severity.ERROR) {
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized List<Message> getMessages(){
        return getMessages(true);
    }

    public synchronized List<Message> getMessages(boolean includeRuntime){
        if (this.messages == null && this.runtimeMessages == null) {
            return Collections.emptyList();
        }
        List<Message> list = new ArrayList<Message>();
        if (this.messages != null) {
            list.addAll(messages);
        }
        if (includeRuntime && this.runtimeMessages != null) {
            list.addAll(runtimeMessages);
        }
        return list;
    }

    public Message addMessage(String severity, String message) {
        Message ve = new Message(Severity.valueOf(severity), message);
        addMessage(ve);
        return ve;
    }

    public synchronized boolean hasRuntimeMessages(){
        return this.runtimeMessages != null && !this.runtimeMessages.isEmpty();
    }

    public synchronized Message addRuntimeError(String message) {
        return addRuntimeMessage(Severity.ERROR, message);
    }

    public synchronized Message addRuntimeMessage(Severity severity, String message) {
        Message ve = new Message(severity, message);
        if (this.runtimeMessages == null) {
            this.runtimeMessages = new LinkedList<Message>();
        }
        this.runtimeMessages.add(ve);
        if (this.runtimeMessages.size() > DEFAULT_ERROR_HISTORY) {
            this.runtimeMessages.remove(0);
        }
        return ve;
    }

    public synchronized Message addMessage(Message ve) {
        if (this.messages == null) {
            this.messages = new LinkedList<Message>();
        }
        this.messages.add(ve);
        return ve;
    }

    public synchronized void clearRuntimeMessages() {
        runtimeMessages = null;
    }

    public synchronized void clearMessages() {
        clearRuntimeMessages();
        this.messages = null;
    }

    public static class Message implements Serializable{
        private static final long serialVersionUID = 2044197069467559527L;

        public enum Severity {ERROR, WARNING, INFO};

        protected String value;
        protected Severity severity;
        protected String path;

        public Message() {};

        public Message(Severity severity, String msg) {
            this.severity = severity;
            Assertion.isNotNull(msg);
            this.value = msg;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            Assertion.isNotNull(value);
            this.value = value;
        }

        public Severity getSeverity() {
            return severity;
        }

        public void setSeverity(Severity severity) {
            this.severity = severity;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Message other = (Message) obj;
            if (severity == null) {
                if (other.severity != null)
                    return false;
            } else if (!severity.equals(other.severity))
                return false;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }
    }

    public void addSourceMetadata(String type, String text) {
        this.sourceMetadataType.add(type);
        this.sourceMetadataText.add(text);
    }

    /**
     * @see #getSourceMetadataType()
     */
    @Deprecated
    public String getSchemaSourceType() {
        if (!sourceMetadataType.isEmpty()) {
            return sourceMetadataType.get(0);
        }
        return null;
    }

    /**
     * @see #addSourceMetadata(String, String)
     */
    @Deprecated
    public void setSchemaSourceType(String schemaSourceType) {
        if (!sourceMetadataType.isEmpty()) {
            sourceMetadataType.set(0, schemaSourceType);
        } else {
            sourceMetadataType.add(schemaSourceType);
        }
    }

    /**
     * @see #getSourceMetadataText()
     */
    @Deprecated
    public String getSchemaText() {
        if (!sourceMetadataText.isEmpty()) {
            return sourceMetadataText.get(0);
        }
        return null;
    }

    /**
     * @see #addSourceMetadata(String, String)
     */
    @Deprecated
    public void setSchemaText(String schemaText) {
        if (!sourceMetadataText.isEmpty()) {
            sourceMetadataText.set(0, schemaText);
        } else {
            sourceMetadataText.add(schemaText);
        }
    }

    public List<String> getSourceMetadataType() {
        return sourceMetadataType;
    }

    public List<String> getSourceMetadataText() {
        return sourceMetadataText;
    }

    @Override
    public List<String> getValidityErrors() {
        List<String> allErrors = new ArrayList<String>();
        List<Message> errors = getMessages();
        if (errors != null && !errors.isEmpty()) {
            for (Message m:errors) {
                if (m.getSeverity() == Severity.ERROR) {
                    allErrors.add(m.getValue());
                }
            }
        }
        return allErrors;
    }

    public static Collection<String> getReservedNames() {
        return RESERVED_NAMES;
    }

}
