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
package org.teiid.jboss;

import org.jboss.as.controller.Extension;
import org.jboss.as.controller.ExtensionContext;
import org.jboss.as.controller.SubsystemRegistration;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.teiid.logging.LogManager;

public class TeiidExtension implements Extension {
		
	public static final String TEIID_SUBSYSTEM = "teiid"; //$NON-NLS-1$
	public static final int MAJOR_VERSION = 1;
	public static final int MINOR_VERSION = 0;
	
	
    public static ResourceDescriptionResolver getResourceDescriptionResolver(final String keyPrefix) {
        return new StandardResourceDescriptionResolver(keyPrefix, IntegrationPlugin.BUNDLE_NAME, TeiidExtension.class.getClassLoader(), true, false);
    }
    
	@Override
	public void initialize(ExtensionContext context) {
		final SubsystemRegistration subsystem = context.registerSubsystem(TEIID_SUBSYSTEM, MAJOR_VERSION, MINOR_VERSION);
		
		LogManager.setLogListener(new JBossLogger());
		
		subsystem.registerXMLElementWriter(TeiidSubsystemParser.INSTANCE);

		// Main Teiid system, with children query engine and translators, register only if this is a server
		subsystem.registerSubsystemModel(new TeiidSubsytemResourceDefinition(context.getProcessType().isServer()));
	}

	@Override
	public void initializeParsers(ExtensionParsingContext context) {
		context.setSubsystemXmlMapping(TEIID_SUBSYSTEM, Namespace.TEIID_1_0.getUri(), TeiidSubsystemParser.INSTANCE);
	}
}
