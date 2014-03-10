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

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.net.socket.SocketUtil;
import org.teiid.transport.SSLConfiguration;

@SuppressWarnings("nls")
public class TeiidConstants {
	// Non persistent attributes
	public static TeiidAttribute  ACTIVE_SESSION_COUNT = new TeiidAttribute("active-session-count", "active-session-count", null, ModelType.INT, true, false, MeasurementUnit.NONE); //$NON-NLS-1$
	public static TeiidAttribute  RUNTIME_VERSION = new TeiidAttribute("runtime-version", "runtime-version", null, ModelType.STRING, true, false, MeasurementUnit.NONE); //$NON-NLS-1$
	
    // VM wide elements
	public static TeiidAttribute ASYNC_THREAD_POOL_ELEMENT = new TeiidAttribute(Element.ASYNC_THREAD_POOL_ELEMENT, null, ModelType.STRING, false, false, MeasurementUnit.NONE);
	public static TeiidAttribute ALLOW_ENV_FUNCTION_ELEMENT = new TeiidAttribute(Element.ALLOW_ENV_FUNCTION_ELEMENT, new ModelNode("false"),  ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_THREADS_ELEMENT = new TeiidAttribute(Element.MAX_THREADS_ELEMENT, new ModelNode(64), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_ACTIVE_PLANS_ELEMENT = new TeiidAttribute(Element.MAX_ACTIVE_PLANS_ELEMENT, new ModelNode(20), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT = new TeiidAttribute(Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, new ModelNode(0), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute TIME_SLICE_IN_MILLI_ELEMENT = new TeiidAttribute(Element.TIME_SLICE_IN_MILLI_ELEMENT, new ModelNode(2000), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_ROWS_FETCH_SIZE_ELEMENT = new TeiidAttribute(Element.MAX_ROWS_FETCH_SIZE_ELEMENT, new ModelNode(20480), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute LOB_CHUNK_SIZE_IN_KB_ELEMENT = new TeiidAttribute(Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT, new ModelNode(100), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute QUERY_THRESHOLD_IN_SECS_ELEMENT = new TeiidAttribute(Element.QUERY_THRESHOLD_IN_SECS_ELEMENT, new ModelNode(600), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_SOURCE_ROWS_ELEMENT = new TeiidAttribute(Element.MAX_SOURCE_ROWS_ELEMENT, new ModelNode(-1),ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT = new TeiidAttribute(Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, new ModelNode(true), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);	
	public static TeiidAttribute DETECTING_CHANGE_EVENTS_ELEMENT = new TeiidAttribute("detect-change-events", "detect-change-events", new ModelNode(true), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
    public static TeiidAttribute QUERY_TIMEOUT = new TeiidAttribute(Element.QUERY_TIMEOUT, new ModelNode(0), ModelType.LONG, true, false, MeasurementUnit.NONE);
    public static TeiidAttribute WORKMANAGER = new TeiidAttribute(Element.WORKMANAGER, new ModelNode("default"), ModelType.STRING, true, false, MeasurementUnit.NONE);

    public static TeiidAttribute POLICY_DECIDER_MODULE_ELEMENT = new TeiidAttribute(Element.POLICY_DECIDER_MODULE_ELEMENT, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
    public static TeiidAttribute AUTHORIZATION_VALIDATOR_MODULE_ELEMENT = new TeiidAttribute(Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT, null, ModelType.STRING, true, false, MeasurementUnit.NONE);

    // buffer manager
	// BUFFER_SERVICE_ELEMENT("buffer-service",true, false, MeasurementUnit.NONE);
	public static TeiidAttribute USE_DISK_ATTRIBUTE = new TeiidAttribute(Element.USE_DISK_ATTRIBUTE, new ModelNode(true), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute PROCESSOR_BATCH_SIZE_ATTRIBUTE = new TeiidAttribute(Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE, new ModelNode(256), ModelType.INT, true, false, MeasurementUnit.NONE);
	@Deprecated
	public static TeiidAttribute CONNECTOR_BATCH_SIZE_ATTRIBUTE = new TeiidAttribute(Element.CONNECTOR_BATCH_SIZE_ATTRIBUTE, new ModelNode(512), ModelType.INT,true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_PROCESSING_KB_ATTRIBUTE = new TeiidAttribute(Element.MAX_PROCESSING_KB_ATTRIBUTE, new ModelNode(-1), ModelType.INT, true, false, MeasurementUnit.KILOBYTES);
	public static TeiidAttribute MAX_RESERVED_KB_ATTRIBUTE = new TeiidAttribute(Element.MAX_RESERVED_KB_ATTRIBUTE, new ModelNode(-1), ModelType.INT, true, false, MeasurementUnit.KILOBYTES);
	public static TeiidAttribute MAX_FILE_SIZE_ATTRIBUTE = new TeiidAttribute(Element.MAX_FILE_SIZE_ATTRIBUTE, new ModelNode(2048), ModelType.LONG, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_BUFFER_SPACE_ATTRIBUTE = new TeiidAttribute(Element.MAX_BUFFER_SPACE_ATTRIBUTE, new ModelNode(51200), ModelType.LONG, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_OPEN_FILES_ATTRIBUTE = new TeiidAttribute(Element.MAX_OPEN_FILES_ATTRIBUTE, new ModelNode(64), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MEMORY_BUFFER_SPACE_ATTRIBUTE = new TeiidAttribute(Element.MEMORY_BUFFER_SPACE_ATTRIBUTE, new ModelNode(-1), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MEMORY_BUFFER_OFFHEAP_ATTRIBUTE = new TeiidAttribute(Element.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE, new ModelNode(false), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE = new TeiidAttribute(Element.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE, new ModelNode(8388608), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute INLINE_LOBS = new TeiidAttribute(Element.INLINE_LOBS, new ModelNode(true), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute ENCRYPT_FILES_ATTRIBUTE = new TeiidAttribute(Element.ENCRYPT_FILES_ATTRIBUTE, new ModelNode(false), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	
	// prepared-plan-cache-config
	// PREPAREDPLAN_CACHE_ELEMENT("preparedplan-cache",true, false, MeasurementUnit.NONE);
	public static TeiidAttribute PPC_ENABLE_ATTRIBUTE = new TeiidAttribute(Element.PPC_ENABLE_ATTRIBUTE, new ModelNode(true), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute PPC_NAME_ATTRIBUTE = new TeiidAttribute(Element.PPC_NAME_ATTRIBUTE, new ModelNode("preparedplan"), ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute PPC_CONTAINER_NAME_ATTRIBUTE = new TeiidAttribute(Element.PPC_CONTAINER_NAME_ELEMENT, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	
	// Object Replicator
	// DISTRIBUTED_CACHE("distributed-cache",true, false, MeasurementUnit.NONE);
	public static TeiidAttribute DC_STACK_ATTRIBUTE = new TeiidAttribute(Element.DC_STACK_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	
	// Result set cache	
	//public static TeiidAttribute RESULTSET_CACHE_ELEMENT = new TeiidAttribute("resultset-cache",true, false, MeasurementUnit.NONE);
	public static TeiidAttribute RSC_ENABLE_ATTRIBUTE = new TeiidAttribute(Element.RSC_ENABLE_ATTRIBUTE, new ModelNode(true), ModelType.BOOLEAN, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute RSC_NAME_ATTRIBUTE = new TeiidAttribute(Element.RSC_NAME_ATTRIBUTE, new ModelNode("resultset"), ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute RSC_CONTAINER_NAME_ATTRIBUTE = new TeiidAttribute(Element.RSC_CONTAINER_NAME_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute RSC_MAX_STALENESS_ATTRIBUTE = new TeiidAttribute(Element.RSC_MAX_STALENESS_ATTRIBUTE, new ModelNode(60), ModelType.INT, true, false, MeasurementUnit.NONE);
	
	//transport
	//TRANSPORT_ELEMENT("transport",true, false, MeasurementUnit.NONE);
	public static TeiidAttribute TRANSPORT_PROTOCOL_ATTRIBUTE = new TeiidAttribute(Element.TRANSPORT_PROTOCOL_ATTRIBUTE, new ModelNode("teiid"), ModelType.STRING, false, false, MeasurementUnit.NONE);
	public static TeiidAttribute TRANSPORT_NAME_ATTRIBUTE = new TeiidAttribute(Element.TRANSPORT_NAME_ATTRIBUTE, null, ModelType.STRING, false, false, MeasurementUnit.NONE);
	public static TeiidAttribute TRANSPORT_SOCKET_BINDING_ATTRIBUTE = new TeiidAttribute(Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE = new TeiidAttribute(Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE, new ModelNode(0), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE = new TeiidAttribute(Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE, new ModelNode(0), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE = new TeiidAttribute(Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE, new ModelNode(0), ModelType.INT, true, false, MeasurementUnit.NONE);
	
	//AUTHENTICATION_ELEMENT("authentication",false, false, MeasurementUnit.NONE);
	public static TeiidAttribute AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE = new TeiidAttribute(Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);	
	public static TeiidAttribute AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE = new TeiidAttribute(Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE, new ModelNode(5000), ModelType.INT, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE = new TeiidAttribute(Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE, new ModelNode(0), ModelType.INT, true, false, MeasurementUnit.NONE);
	@Deprecated
	public static TeiidAttribute AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE = new TeiidAttribute(Element.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute AUTHENTICATION_TYPE_ATTRIBUTE = new TeiidAttribute(Element.AUTHENTICATION_TYPE_ATTRIBUTE, new ModelNode(AuthenticationType.USERPASSWORD.name()), ModelType.STRING, true, false, MeasurementUnit.NONE);
	
	//PG_ELEMENT("pg"), //$NON-NLS-1$
	public static TeiidAttribute PG_MAX_LOB_SIZE_ALLOWED_ELEMENT = new TeiidAttribute(Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT, new ModelNode(5242880), ModelType.INT, true, false, MeasurementUnit.NONE); //$NON-NLS-1$ //$NON-NLS-2$
	
	//SSL_ELEMENT("ssl"),
	public static TeiidAttribute SSL_MODE_ATTRIBUTE = new TeiidAttribute(Element.SSL_MODE_ATTRIBUTE, new ModelNode(SSLConfiguration.LOGIN), ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_AUTH_MODE_ATTRIBUTE = new TeiidAttribute(Element.SSL_AUTH_MODE_ATTRIBUTE, new ModelNode(SSLConfiguration.ONEWAY), ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_SSL_PROTOCOL_ATTRIBUTE = new TeiidAttribute(Element.SSL_SSL_PROTOCOL_ATTRIBUTE, new ModelNode(SocketUtil.DEFAULT_PROTOCOL), ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE = new TeiidAttribute(Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE = new TeiidAttribute(Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	//SSL_KETSTORE_ELEMENT("keystore",false, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_KETSTORE_NAME_ATTRIBUTE = new TeiidAttribute(Element.SSL_KETSTORE_NAME_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_KETSTORE_ALIAS_ATTRIBUTE = new TeiidAttribute(Element.SSL_KETSTORE_ALIAS_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE = new TeiidAttribute(Element.SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE, null, ModelType.STRING, true, true, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_KETSTORE_PASSWORD_ATTRIBUTE = new TeiidAttribute(Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE, null, ModelType.STRING, true, true, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_KETSTORE_TYPE_ATTRIBUTE = new TeiidAttribute(Element.SSL_KETSTORE_TYPE_ATTRIBUTE, new ModelNode("JKS"), ModelType.STRING, true, false, MeasurementUnit.NONE);
	//SSL_TRUSTSTORE_ELEMENT("truststore",false, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_TRUSTSTORE_NAME_ATTRIBUTE = new TeiidAttribute(Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
	public static TeiidAttribute SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE = new TeiidAttribute(Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE, null, ModelType.STRING, true, true, MeasurementUnit.NONE);	

	// Translator
	// TRANSLATOR_ELEMENT("translator"),
    public static TeiidAttribute TRANSLATOR_NAME_ATTRIBUTE = new TeiidAttribute(Element.TRANSLATOR_NAME_ATTRIBUTE, null, ModelType.STRING, false, false, MeasurementUnit.NONE);
    public static TeiidAttribute TRANSLATOR_MODULE_ATTRIBUTE = new TeiidAttribute(Element.TRANSLATOR_MODULE_ATTRIBUTE, null, ModelType.STRING, true, false, MeasurementUnit.NONE);
    
    
    static class TeiidAttribute extends SimpleAttributeDefinition {
    	
    	public TeiidAttribute(String modelName, String xmlName, final ModelNode defaultValue, final ModelType type,
                final boolean allowNull, final boolean allowExpression, final MeasurementUnit measurementUnit) {
    		super(modelName, xmlName, defaultValue, type, allowNull, allowExpression, measurementUnit);
    	}
    	
    	public TeiidAttribute(Element element, final ModelNode defaultValue, final ModelType type,
                final boolean allowNull, final boolean allowExpression, final MeasurementUnit measurementUnit) {
    		super(element.getModelName(), element.getXMLName(), defaultValue, type, allowNull, allowExpression, measurementUnit);
    	}
    	
        public boolean isDefined(final ModelNode  model, final OperationContext context) throws OperationFailedException {
            ModelNode resolvedNode = resolveModelAttribute(context, model);
            return resolvedNode.isDefined();    	
        }    	
        
        public Integer asInt(final ModelNode node, final OperationContext context) throws OperationFailedException {
            ModelNode resolvedNode = resolveModelAttribute(context, node);
            return resolvedNode.isDefined() ? resolvedNode.asInt() : null;
        }
        
        public Long asLong(ModelNode node, OperationContext context) throws OperationFailedException {
            ModelNode resolvedNode = resolveModelAttribute(context, node);
            return resolvedNode.isDefined() ? resolvedNode.asLong() : null;
        }
        
        public String asString(ModelNode node, OperationContext context) throws OperationFailedException {
            ModelNode resolvedNode = resolveModelAttribute(context, node);
            return resolvedNode.isDefined() ? resolvedNode.asString() : null;
        }
        
        public Boolean asBoolean(ModelNode node, OperationContext context) throws OperationFailedException {
            ModelNode resolvedNode = resolveModelAttribute(context, node);
            return resolvedNode.isDefined() ? resolvedNode.asBoolean() : null;
        }        
    }
}
