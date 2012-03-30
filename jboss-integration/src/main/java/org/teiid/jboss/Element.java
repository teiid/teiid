/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DEFAULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUIRED;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.TYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.core.TeiidRuntimeException;

@SuppressWarnings("nls")
enum Element {
    // must be first
    UNKNOWN(null),

    // VM wide elements
    ASYNC_THREAD_POOL_ELEMENT("async-thread-pool", "async-thread-pool", ModelType.STRING, true, null),
    ALLOW_ENV_FUNCTION_ELEMENT("allow-env-function", "allow-env-function", ModelType.BOOLEAN, false, "false"),
            	
	MAX_THREADS_ELEMENT("max-threads", "max-threads", ModelType.INT, false, "64"),
	MAX_ACTIVE_PLANS_ELEMENT("max-active-plans", "max-active-plans", ModelType.INT, false, "20"),
	USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT("thread-count-for-source-concurrency", "thread-count-for-source-concurrency", ModelType.INT, false, "0"),
	TIME_SLICE_IN_MILLI_ELEMENT("time-slice-in-millseconds", "time-slice-in-millseconds", ModelType.INT, false, "2000"),
	MAX_ROWS_FETCH_SIZE_ELEMENT("max-row-fetch-size", "max-row-fetch-size", ModelType.INT, false, "20480"),
	LOB_CHUNK_SIZE_IN_KB_ELEMENT("lob-chunk-size-in-kb", "lob-chunk-size-in-kb", ModelType.INT, false, "100"),
	QUERY_THRESHOLD_IN_SECS_ELEMENT("query-threshold-in-seconds", "query-threshold-in-seconds", ModelType.INT, false, "600"),
	MAX_SOURCE_ROWS_ELEMENT("max-source-rows-allowed", "max-source-rows-allowed", ModelType.INT, false, "-1"),
	EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT("exception-on-max-source-rows", "exception-on-max-source-rows", ModelType.BOOLEAN, false, "true"),	
	DETECTING_CHANGE_EVENTS_ELEMENT("detect-change-events", "detect-change-events", ModelType.BOOLEAN, false, "true"),
    QUERY_TIMEOUT("query-timeout", "query-timeout", ModelType.LONG, false, "0"),
    WORKMANAGER("workmanager", "workmanager", ModelType.STRING, false, "default"),
    
    POLICY_DECIDER_MODULE_ELEMENT("policy-decider-module", "policy-decider-module", ModelType.STRING, false, null),
    AUTHORIZATION_VALIDATOR_MODULE_ELEMENT("authorization-validator-module", "authorization-validator-module", ModelType.STRING, false, null),
	
	// buffer manager
	BUFFER_SERVICE_ELEMENT("buffer-service"),
	USE_DISK_ATTRIBUTE("use-disk", "buffer-service-use-disk", ModelType.BOOLEAN, false, "true"),
	PROCESSOR_BATCH_SIZE_ATTRIBUTE("processor-batch-size", "buffer-service-processor-batch-size", ModelType.INT, false, "256"),
	CONNECTOR_BATCH_SIZE_ATTRIBUTE("connector-batch-size", "buffer-service-connector-batch-size", ModelType.INT, false, "512"),
	MAX_PROCESSING_KB_ATTRIBUTE("max-processing-kb", "buffer-service-max-processing-kb", ModelType.INT, false, "-1"),
	MAX_RESERVED_KB_ATTRIBUTE("max-reserve-kb", "buffer-service-max-reserve-kb", ModelType.INT, false, "-1"),
	MAX_FILE_SIZE_ATTRIBUTE("max-file-size", "buffer-service-max-file-size", ModelType.LONG, false, "2048"),
	MAX_BUFFER_SPACE_ATTRIBUTE("max-buffer-space", "buffer-service-max-buffer-space", ModelType.LONG, false, "51200"),
	MAX_OPEN_FILES_ATTRIBUTE("max-open-files", "buffer-service-max-open-files", ModelType.INT, false, "64"),
	MEMORY_BUFFER_SPACE_ATTRIBUTE("memory-buffer-space", "buffer-service-memory-buffer-space", ModelType.INT, false, "-1"),
	MEMORY_BUFFER_OFFHEAP_ATTRIBUTE("memory-buffer-off-heap", "buffer-service-memory-buffer-off-heap", ModelType.BOOLEAN, false, "false"),
	MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE("max-storage-object-size", "buffer-service-max-storage-object-size", ModelType.INT, false, "8388608"),
	INLINE_LOBS("inline-lobs", "buffer-service-inline-lobs", ModelType.BOOLEAN, false, "true"),
	
	//prepared-plan-cache-config
	PREPAREDPLAN_CACHE_ELEMENT("preparedplan-cache"),
	PPC_MAX_ENTRIES_ATTRIBUTE("max-entries", "preparedplan-cache-max-entries", ModelType.INT, false, "512"),
	PPC_MAX_AGE_IN_SECS_ATTRIBUTE("max-age-in-seconds", "preparedplan-cache-max-age-in-seconds", ModelType.INT, false, "28800"),
	
	// Object Replicator
	DISTRIBUTED_CACHE("distributed-cache"),
	DC_STACK_ATTRIBUTE("jgroups-stack", "distributed-cache-jgroups-stack", ModelType.STRING, false, null),
	
	// Result set cache	
	RESULTSET_CACHE_ELEMENT("resultset-cache"),
	RSC_ENABLE_ATTRIBUTE("enable", "resultset-cache-enable", ModelType.BOOLEAN, false, "true"),
	RSC_NAME_ELEMENT("name", "resultset-cache-name", ModelType.STRING, false, null),
	RSC_CONTAINER_NAME_ELEMENT("infinispan-container", "resultset-cache-infinispan-container", ModelType.STRING, false, null),
	RSC_MAX_STALENESS_ELEMENT("max-staleness", "resultset-cache-max-staleness", ModelType.INT, false, "60"),
	
	//transport
	TRANSPORT_ELEMENT("transport"),
	TRANSPORT_PROTOCOL_ATTRIBUTE("protocol", "protocol", ModelType.STRING, false, "teiid"),
	TRANSPORT_NAME_ATTRIBUTE("name", "name", ModelType.STRING, true, null),
	TRANSPORT_SOCKET_BINDING_ATTRIBUTE("socket-binding", "socket-binding", ModelType.STRING, true, null),
	TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE("max-socket-threads", "max-socket-threads", ModelType.INT, false, "0"),
	TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE("input-buffer-size", "input-buffer-size",ModelType.INT, false, "0"),
	TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE("output-buffer-size", "output-buffer-size", ModelType.INT, false, "0"),
	
	AUTHENTICATION_ELEMENT("authentication"),
	AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE("security-domain", "authentication-security-domain", ModelType.STRING, false, null),	
	AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE("max-sessions-allowed", "authentication-max-sessions-allowed",ModelType.INT, false, "5000"),
	AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE("sessions-expiration-timelimit", "authentication-sessions-expiration-timelimit", ModelType.INT, false, "0"),
	AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE("krb5-domain", "authentication-krb5-domain", ModelType.STRING, false, null),
	
	PG_ELEMENT("pg"), //$NON-NLS-1$
	PG_MAX_LOB_SIZE_ALLOWED_ELEMENT("max-lob-size-in-bytes", "pg-max-lob-size-in-bytes", ModelType.INT, false, "5242880"), //$NON-NLS-1$ //$NON-NLS-2$
	
	SSL_ELEMENT("ssl"),
	SSL_ENABLE_ATTRIBUTE("enable", "ssl-enable", ModelType.BOOLEAN, false, "false"),
	SSL_MODE_ATTRIBUTE("mode", "ssl-mode", ModelType.STRING, false, "login"),
	SSL_AUTH_MODE_ATTRIBUTE("authentication-mode", "ssl-authentication-mode", ModelType.STRING, false, "anonymous"),
	SSL_SSL_PROTOCOL_ATTRIBUTE("ssl-protocol", "ssl-ssl-protocol", ModelType.STRING, false, "SSLv3"),
	SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE("keymanagement-algorithm", "ssl-keymanagement-algorithm", ModelType.STRING, false, null),
	SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE("enabled-cipher-suites", "enabled-cipher-suites", ModelType.STRING, false, null),
	SSL_KETSTORE_ELEMENT("keystore"),
	SSL_KETSTORE_NAME_ATTRIBUTE("name", "keystore-name", ModelType.STRING, false, null),
	SSL_KETSTORE_PASSWORD_ATTRIBUTE("password", "keystore-password", ModelType.STRING, false, null),
	SSL_KETSTORE_TYPE_ATTRIBUTE("type", "keystore-type", ModelType.STRING, false, "JKS"),
	SSL_TRUSTSTORE_ELEMENT("truststore"),
	SSL_TRUSTSTORE_NAME_ATTRIBUTE("name", "truststore-name", ModelType.STRING, false, null),
	SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE("password", "truststore-password", ModelType.STRING, false, null),	

	// Translator
    TRANSLATOR_ELEMENT("translator"),
    TRANSLATOR_NAME_ATTRIBUTE("name", "name", ModelType.STRING, true, null),
    TRANSLATOR_MODULE_ATTRIBUTE("module", "module", ModelType.STRING, true, null);
    
    private final String name;
    private final String modelName;
    private final boolean required;
    private final ModelType modelType;
    private final String defaultValue;

    private Element(String name) {
    	this.name = name;
    	this.modelName = name;
    	this.required = false;
    	this.modelType = null;
    	this.defaultValue = null;
    }
    
    Element(final String name, String modelName, ModelType type, boolean required, String defltValue) {
        this.name = name;
        this.modelName = modelName;
        this.modelType = type;
        this.required = required;
        this.defaultValue = defltValue;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }
    
    public String getModelName() {
    	return this.modelName;
    }
    
    private static final Map<String, Element> elements;

    static {
        final Map<String, Element> map = new HashMap<String, Element>();
        for (Element element : values()) {
            final String name = element.getModelName();
            if (name != null) map.put(name, element);
        }
        elements = map;
    }

    public static Element forName(String localName, Element parentNode) {
    	String modelName = parentNode.getLocalName()+"-"+localName;
        final Element element = elements.get(modelName);
        return element == null ? UNKNOWN : element;
    }
    
    public static Element forName(String localName) {
        final Element element = elements.get(localName);
        return element == null ? UNKNOWN : element;
    }    
    
    public void describe(ModelNode node, String type, ResourceBundle bundle) {
		String name = getModelName();
		node.get(type, name, TYPE).set(this.modelType);
        node.get(type, name, DESCRIPTION).set(getDescription(bundle));
        node.get(type, name, REQUIRED).set(this.required);
        //node.get(type, name, MAX_OCCURS).set(1);
        
        if (this.defaultValue != null) {
        	if (ModelType.INT == this.modelType) {
        		node.get(type, name, DEFAULT).set(Integer.parseInt(this.defaultValue));
        	}
        	else if (ModelType.BOOLEAN == this.modelType) {
        		node.get(type, name, DEFAULT).set(Boolean.parseBoolean(this.defaultValue));
        	}
        	else if (ModelType.LONG == this.modelType) {
        		node.get(type, name, DEFAULT).set(Long.parseLong(this.defaultValue));
        	}        	
        	else if (ModelType.STRING == this.modelType) {
        		node.get(type, name, DEFAULT).set(this.defaultValue);
        	}
        	else {
        		 throw new TeiidRuntimeException(IntegrationPlugin.Event.TEIID50045);
        	}
        }        
    }
    
    public void populate(ModelNode operation, ModelNode model) {
    	if (getModelName() == null) {
    		return;
    	}
    	
    	if (operation.hasDefined(getModelName())) {
    		if (ModelType.STRING == this.modelType) {
    			model.get(getModelName()).set(operation.get(getModelName()).asString());
    		}
    		else if (ModelType.INT == this.modelType) {
    			model.get(getModelName()).set(operation.get(getModelName()).asInt());
    		}
    		else if (ModelType.LONG == this.modelType) {
    			model.get(getModelName()).set(operation.get(getModelName()).asLong());
    		}
    		else if (ModelType.BOOLEAN == this.modelType) {
    			model.get(getModelName()).set(operation.get(getModelName()).asBoolean());
    		}
    		else {
    			 throw new TeiidRuntimeException(IntegrationPlugin.Event.TEIID50046);
    		}
    	}
    }
    
    public boolean isDefined(ModelNode node) {
    	return node.hasDefined(getModelName());
    }
    
    public int asInt(ModelNode node) {
    	return node.get(getModelName()).asInt();
    }
    
    public long asLong(ModelNode node) {
    	return node.get(getModelName()).asLong();
    }
    
    public String asString(ModelNode node) {
    	return node.get(getModelName()).asString();
    }
    
    public boolean asBoolean(ModelNode node) {
    	return node.get(getModelName()).asBoolean();
    }
    
    public boolean isLike(ModelNode node) {
    	Set<String> keys = node.keys();
    	for(String key:keys) {
    		if (key.startsWith(this.name)) {
    			return true;
    		}
    	}
    	return false; 
    }
    
    public String getDescription(ResourceBundle bundle) {
    	return bundle.getString(this.modelName+".describe");
    }

	public boolean sameAsDefault(String value) {
		if (this.defaultValue == null) {
			return (value == null);
		}
		return this.defaultValue.equalsIgnoreCase(value);
	}
}

