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

import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.dmr.ModelType;

class OperationsConstants {
	public static final SimpleAttributeDefinition SESSION = new SimpleAttributeDefinition("session", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition VDB_NAME = new SimpleAttributeDefinition("vdb-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition VDB_VERSION = new SimpleAttributeDefinition("vdb-version", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition EXECUTION_ID = new SimpleAttributeDefinition("execution-id", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition CACHE_TYPE = new SimpleAttributeDefinition("cache-type", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition XID = new SimpleAttributeDefinition("xid", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition DATA_ROLE = new SimpleAttributeDefinition("data-role", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition MAPPED_ROLE = new SimpleAttributeDefinition("mapped-role", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition CONNECTION_TYPE = new SimpleAttributeDefinition("connection-type", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition MODEL_NAME = new SimpleAttributeDefinition("model-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition SOURCE_NAME = new SimpleAttributeDefinition("source-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition DS_NAME = new SimpleAttributeDefinition("ds-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition RAR_NAME = new SimpleAttributeDefinition("rar-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition MODEL_NAMES = new SimpleAttributeDefinition("model-names", ModelType.STRING, true); //$NON-NLS-1$
	public static final SimpleAttributeDefinition SOURCE_VDBNAME = new SimpleAttributeDefinition("source-vdb-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition SOURCE_VDBVERSION = new SimpleAttributeDefinition("source-vdb-version", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition TARGET_VDBNAME = new SimpleAttributeDefinition("target-vdb-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition TARGET_VDBVERSION = new SimpleAttributeDefinition("target-vdb-version", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition SQL_QUERY = new SimpleAttributeDefinition("sql-query", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition TIMEOUT_IN_MILLI = new SimpleAttributeDefinition("timeout-in-milli", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition TRANSLATOR_NAME = new SimpleAttributeDefinition("translator-name", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition PROPERTY_TYPE = new SimpleAttributeDefinition("type", ModelType.STRING, false); //$NON-NLS-1$
	public static final SimpleAttributeDefinition ENTITY_TYPE = new SimpleAttributeDefinition("entity-type", ModelType.STRING, true); //$NON-NLS-1$
	public static final SimpleAttributeDefinition ENTITY_PATTERN = new SimpleAttributeDefinition("entity-pattern", ModelType.STRING, true); //$NON-NLS-1$
	public static final SimpleAttributeDefinition INCLUDE_SOURCE = new SimpleAttributeDefinition("include-source", ModelType.STRING, true); //$NON-NLS-1$
	
	public static final SimpleAttributeDefinition OPTIONAL_VDB_NAME = new SimpleAttributeDefinition("vdb-name", ModelType.STRING, true); //$NON-NLS-1$
	public static final SimpleAttributeDefinition OPTIONAL_VDB_VERSION = new SimpleAttributeDefinition("vdb-version", ModelType.STRING, true); //$NON-NLS-1$
	
}
