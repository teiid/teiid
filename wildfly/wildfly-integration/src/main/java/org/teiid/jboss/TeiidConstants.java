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

import org.jboss.as.controller.ModelVersion;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.net.socket.SocketUtil;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SSLConfiguration.ClientAuth;
import org.teiid.transport.WireProtocol;

@SuppressWarnings("nls")
public class TeiidConstants {
    private static final ModelVersion BUFFER_SERVICE_VERSION = ModelVersion.create(1, 2);

    // Non persistent attributes
    public static SimpleAttributeDefinition ACTIVE_SESSION_COUNT = new SimpleAttributeDefinitionBuilder("active-session-count", ModelType.INT)
        .setXmlName("active-session-count")
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition RUNTIME_VERSION = new SimpleAttributeDefinitionBuilder("runtime-version", ModelType.STRING)
        .setXmlName("runtime-version")
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    // VM wide elements
    public static SimpleAttributeDefinition ASYNC_THREAD_POOL_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.ASYNC_THREAD_POOL_ELEMENT.getModelName(), ModelType.STRING)
        .setXmlName(Element.ASYNC_THREAD_POOL_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition THREAD_COUNT_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.THREAD_COUNT_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.THREAD_COUNT_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition ALLOW_ENV_FUNCTION_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.ALLOW_ENV_FUNCTION_ELEMENT.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.ALLOW_ENV_FUNCTION_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(false))
        .build();

    public static SimpleAttributeDefinition MAX_THREADS_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.MAX_THREADS_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.MAX_THREADS_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(64))
        .build();

    public static SimpleAttributeDefinition MAX_ACTIVE_PLANS_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.MAX_ACTIVE_PLANS_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.MAX_ACTIVE_PLANS_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(20))
        .build();

    public static SimpleAttributeDefinition USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(0))
        .build();

    public static SimpleAttributeDefinition TIME_SLICE_IN_MILLI_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.TIME_SLICE_IN_MILLI_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.TIME_SLICE_IN_MILLI_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(2000))
        .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
        .build();

    public static SimpleAttributeDefinition MAX_ROWS_FETCH_SIZE_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.MAX_ROWS_FETCH_SIZE_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.MAX_ROWS_FETCH_SIZE_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(20480))
        .build();

    public static SimpleAttributeDefinition LOB_CHUNK_SIZE_IN_KB_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(100))
        .setMeasurementUnit(MeasurementUnit.KILOBYTES)
        .build();

    public static SimpleAttributeDefinition QUERY_THRESHOLD_IN_SECS_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.QUERY_THRESHOLD_IN_SECS_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.QUERY_THRESHOLD_IN_SECS_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(600))
        .setMeasurementUnit(MeasurementUnit.SECONDS)
        .build();

    public static SimpleAttributeDefinition MAX_SOURCE_ROWS_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.MAX_SOURCE_ROWS_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.MAX_SOURCE_ROWS_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(-1))
        .build();

    public static SimpleAttributeDefinition EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    public static SimpleAttributeDefinition DETECTING_CHANGE_EVENTS_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.DETECTING_CHANGE_EVENTS_ELEMENT.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.DETECTING_CHANGE_EVENTS_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    public static SimpleAttributeDefinition QUERY_TIMEOUT = new SimpleAttributeDefinitionBuilder(Element.QUERY_TIMEOUT.getModelName(), ModelType.LONG)
        .setXmlName(Element.QUERY_TIMEOUT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(0))
        .build();

    public static SimpleAttributeDefinition WORKMANAGER = new SimpleAttributeDefinitionBuilder(Element.WORKMANAGER.getModelName(), ModelType.STRING)
        .setXmlName(Element.WORKMANAGER.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode("default"))
        .build();

    public static SimpleAttributeDefinition POLICY_DECIDER_MODULE_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.POLICY_DECIDER_MODULE_ELEMENT.getModelName(), ModelType.STRING)
        .setXmlName(Element.POLICY_DECIDER_MODULE_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition DATA_ROLES_REQUIRED_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.DATA_ROLES_REQUIRED_ELEMENT.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.DATA_ROLES_REQUIRED_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition AUTHORIZATION_VALIDATOR_MODULE_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.getModelName(), ModelType.STRING)
        .setXmlName(Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition PREPARSER_MODULE_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.PREPARSER_MODULE_ELEMENT.getModelName(), ModelType.STRING)
        .setXmlName(Element.PREPARSER_MODULE_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    /*
     * Deprecated buffer-service attributes
     */
    @Deprecated
    public static SimpleAttributeDefinition USE_DISK_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.USE_DISK_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_USE_DISK_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition PROCESSOR_BATCH_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MAX_PROCESSING_KB_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MAX_PROCESSING_KB_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setMeasurementUnit(MeasurementUnit.KILOBYTES)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MAX_RESERVED_KB_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MAX_RESERVED_KB_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setMeasurementUnit(MeasurementUnit.KILOBYTES)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MAX_FILE_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MAX_FILE_SIZE_ATTRIBUTE.getModelName(), ModelType.LONG)
        .setXmlName(Element.BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MAX_BUFFER_SPACE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MAX_BUFFER_SPACE_ATTRIBUTE.getModelName(), ModelType.LONG)
        .setXmlName(Element.BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setMeasurementUnit(MeasurementUnit.MEGABYTES)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MAX_OPEN_FILES_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MAX_OPEN_FILES_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MEMORY_BUFFER_SPACE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MEMORY_BUFFER_SPACE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setMeasurementUnit(MeasurementUnit.MEGABYTES)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MEMORY_BUFFER_OFFHEAP_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(false))
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setMeasurementUnit(MeasurementUnit.BYTES)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition INLINE_LOBS = new SimpleAttributeDefinitionBuilder(Element.INLINE_LOBS.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_INLINE_LOBS.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    @Deprecated
    public static SimpleAttributeDefinition ENCRYPT_FILES_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.ENCRYPT_FILES_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDeprecated(BUFFER_SERVICE_VERSION)
        .build();

    // buffer manager
    public static SimpleAttributeDefinition BUFFER_MANAGER_USE_DISK_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_USE_DISK_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_USE_DISK_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(256))
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(-1))
        .setMeasurementUnit(MeasurementUnit.KILOBYTES)
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(-1))
        .setMeasurementUnit(MeasurementUnit.MEGABYTES)
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE.getModelName(), ModelType.LONG)
        .setXmlName(Element.BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(2048))
        .setMeasurementUnit(MeasurementUnit.MEGABYTES)
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE.getModelName(), ModelType.LONG)
        .setXmlName(Element.BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setMeasurementUnit(MeasurementUnit.MEGABYTES)
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(64))
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(-1))
        .setMeasurementUnit(MeasurementUnit.MEGABYTES)
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(false))
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(8196))
        .setMeasurementUnit(MeasurementUnit.KILOBYTES)
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_INLINE_LOBS = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_INLINE_LOBS.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_INLINE_LOBS.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    public static SimpleAttributeDefinition BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(false))
        .build();

    // prepared-plan-cache-config
    // PREPAREDPLAN_CACHE_ELEMENT("preparedplan-cache",true, false, MeasurementUnit.NONE);
    public static SimpleAttributeDefinition PPC_ENABLE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.PPC_ENABLE_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.PPC_ENABLE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    public static SimpleAttributeDefinition PPC_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.PPC_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.PPC_NAME_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode("preparedplan"))
        .build();

    public static SimpleAttributeDefinition PPC_CONTAINER_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.PPC_CONTAINER_NAME_ELEMENT.getModelName(), ModelType.STRING)
        .setXmlName(Element.PPC_CONTAINER_NAME_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    // Object Replicator
    // DISTRIBUTED_CACHE("distributed-cache",true, false, MeasurementUnit.NONE);
    public static SimpleAttributeDefinition DC_STACK_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.DC_STACK_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.DC_STACK_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    // Result set cache
    public static SimpleAttributeDefinition RSC_ENABLE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.RSC_ENABLE_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.RSC_ENABLE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    public static SimpleAttributeDefinition RSC_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.RSC_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.RSC_NAME_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode("resultset"))
        .build();

    public static SimpleAttributeDefinition RSC_CONTAINER_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.RSC_CONTAINER_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.RSC_CONTAINER_NAME_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition RSC_MAX_STALENESS_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.RSC_MAX_STALENESS_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.RSC_MAX_STALENESS_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(DQPConfiguration.DEFAULT_MAX_STALENESS_SECONDS))
        .build();

    //transport
    //TRANSPORT_ELEMENT("transport",true, false, MeasurementUnit.NONE);
    public static SimpleAttributeDefinition TRANSPORT_PROTOCOL_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSPORT_PROTOCOL_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.TRANSPORT_PROTOCOL_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(WireProtocol.teiid.name()))
        .setAllowedValues(new ModelNode(WireProtocol.teiid.name()), new ModelNode(WireProtocol.pg.name()))
        .build();

    public static SimpleAttributeDefinition TRANSPORT_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSPORT_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.TRANSPORT_NAME_ATTRIBUTE.getXMLName())
        .setAllowNull(false)
        .setAllowExpression(false)
        .build();

    public static SimpleAttributeDefinition TRANSPORT_SOCKET_BINDING_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_BINDING_REF)*/
        .build();

    public static SimpleAttributeDefinition TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.getModelName(), ModelType.INT)
       .setXmlName(Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.getXMLName())
       .setRequired(false)
       .setAllowExpression(false)
       .setDefaultValue(new ModelNode(0))
       .build();

    public static SimpleAttributeDefinition TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.getModelName(), ModelType.INT)
       .setXmlName(Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.getXMLName())
       .setRequired(false)
       .setAllowExpression(false)
       .setDefaultValue(new ModelNode(0))
       .build();

    public static SimpleAttributeDefinition TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(0))
        .build();

    //AUTHENTICATION_ELEMENT("authentication",false, false, MeasurementUnit.NONE);
    public static SimpleAttributeDefinition AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SECURITY_DOMAIN_REF)*/
        .build();

    public static SimpleAttributeDefinition AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(10000))
        .build();

    public static SimpleAttributeDefinition AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.getModelName(), ModelType.INT)
        .setXmlName(Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(0))
        .build();

    public static SimpleAttributeDefinition AUTHENTICATION_TYPE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.AUTHENTICATION_TYPE_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.AUTHENTICATION_TYPE_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(AuthenticationType.USERPASSWORD.name()))
        .setAllowedValues(new ModelNode(AuthenticationType.USERPASSWORD.name()), new ModelNode(AuthenticationType.GSS.name()), new ModelNode(AuthenticationType.SSL.name()))
        .build();

    public static SimpleAttributeDefinition AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(true))
        .build();

    //PG_ELEMENT("pg"), //$NON-NLS-1$
    public static SimpleAttributeDefinition PG_MAX_LOB_SIZE_ALLOWED_ELEMENT = new SimpleAttributeDefinitionBuilder(Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.getModelName(), ModelType.INT)
        .setXmlName(Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.getXMLName())
        .setRequired(false)
        .setAllowExpression(false)
        .setDefaultValue(new ModelNode(5242880))
        .build();

    //SSL_ELEMENT("ssl"),
    public static SimpleAttributeDefinition SSL_MODE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_MODE_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_MODE_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        .setDefaultValue(new ModelNode(SSLConfiguration.LOGIN))
        .setAllowedValues(SSLConfiguration.LOGIN, SSLConfiguration.ENABLED, SSLConfiguration.DISABLED)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_AUTH_MODE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_AUTH_MODE_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_AUTH_MODE_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        .setDefaultValue(new ModelNode(ClientAuth.NONE.name()))
        .setAllowedValues(SSLConfiguration.ONEWAY, SSLConfiguration.TWOWAY, SSLConfiguration.ANONYMOUS, ClientAuth.NEED.name(), ClientAuth.NONE.name(), ClientAuth.WANT.name())
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_SSL_PROTOCOL_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_SSL_PROTOCOL_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_SSL_PROTOCOL_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        .setDefaultValue(new ModelNode(SocketUtil.DEFAULT_PROTOCOL))
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    //SSL_KETSTORE_ELEMENT("keystore",false, false, MeasurementUnit.NONE);
    public static SimpleAttributeDefinition SSL_KETSTORE_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_KETSTORE_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_KETSTORE_NAME_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_KETSTORE_ALIAS_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_KETSTORE_ALIAS_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_KETSTORE_ALIAS_ATTRIBUTE.getXMLName())
        .setAllowExpression(true)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.CREDENTIAL)
        .addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE.getXMLName())
        .setAllowExpression(true)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.CREDENTIAL)
        .addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_KETSTORE_PASSWORD_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE.getXMLName())
        .setAllowExpression(true)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.CREDENTIAL)
        .addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_KETSTORE_TYPE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_KETSTORE_TYPE_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_KETSTORE_TYPE_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        .setDefaultValue(new ModelNode(SocketUtil.DEFAULT_KEYSTORE_TYPE))
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    //SSL_TRUSTSTORE_ELEMENT("truststore",false, false, MeasurementUnit.NONE);
    public static SimpleAttributeDefinition SSL_TRUSTSTORE_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.getXMLName())
        .setAllowExpression(true)
        .setRequired(false)
        /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
        .build();

    public static SimpleAttributeDefinition SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.getModelName(), ModelType.STRING)
         .setXmlName(Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.getXMLName())
         .setAllowExpression(true)
         .setRequired(false)
         .setRequires(Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.getModelName())
         /*.addAccessConstraint(SensitiveTargetAccessConstraintDefinition.CREDENTIAL)
         .addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_CONFIG)*/
         .build();

    public static SimpleAttributeDefinition SSL_TRUSTSTORE_CHECK_EXPIRED_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.SSL_TRUSTSTORE_CHECK_EXIRIED_ATTRIBUTE.getModelName(), ModelType.BOOLEAN)
        .setXmlName(Element.SSL_TRUSTSTORE_CHECK_EXIRIED_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        .setDefaultValue(new ModelNode(false))
        .build();

    // Translator
    // TRANSLATOR_ELEMENT("translator"),
    public static SimpleAttributeDefinition TRANSLATOR_NAME_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSLATOR_NAME_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.TRANSLATOR_NAME_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setAllowNull(false)
        .build();

    public static SimpleAttributeDefinition TRANSLATOR_MODULE_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSLATOR_MODULE_ATTRIBUTE.getModelName(), ModelType.STRING)
        .setXmlName(Element.TRANSLATOR_MODULE_ATTRIBUTE.getXMLName())
        .setAllowExpression(false)
        .setRequired(false)
        .build();

    public static SimpleAttributeDefinition TRANSLATOR_SLOT_ATTRIBUTE = new SimpleAttributeDefinitionBuilder(Element.TRANSLATOR_SLOT_ATTRIBUTE.getModelName(), ModelType.STRING)
            .setXmlName(Element.TRANSLATOR_SLOT_ATTRIBUTE.getXMLName())
            .setAllowExpression(false)
            .setRequired(false)
            .build();

    public static boolean isDefined(final SimpleAttributeDefinition attr, final ModelNode  model, final OperationContext context) throws OperationFailedException {
        ModelNode resolvedNode = attr.resolveModelAttribute(context, model);
        return resolvedNode.isDefined();
    }

    public static Integer asInt(final SimpleAttributeDefinition attr, final ModelNode node, final OperationContext context) throws OperationFailedException {
        ModelNode resolvedNode = attr.resolveModelAttribute(context, node);
        return resolvedNode.isDefined() ? resolvedNode.asInt() : null;
    }

    public static Long asLong(final SimpleAttributeDefinition attr, ModelNode node, OperationContext context) throws OperationFailedException {
        ModelNode resolvedNode = attr.resolveModelAttribute(context, node);
        return resolvedNode.isDefined() ? resolvedNode.asLong() : null;
    }

    public static String asString(final SimpleAttributeDefinition attr, ModelNode node, OperationContext context) throws OperationFailedException {
        ModelNode resolvedNode = attr.resolveModelAttribute(context, node);
        return resolvedNode.isDefined() ? resolvedNode.asString() : null;
    }

    public static Boolean asBoolean(final SimpleAttributeDefinition attr, ModelNode node, OperationContext context) throws OperationFailedException {
        ModelNode resolvedNode = attr.resolveModelAttribute(context, node);
        return resolvedNode.isDefined() ? resolvedNode.asBoolean() : null;
    }
}
