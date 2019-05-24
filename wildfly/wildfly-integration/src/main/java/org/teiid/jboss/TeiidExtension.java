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

import org.jboss.as.controller.Extension;
import org.jboss.as.controller.ExtensionContext;
import org.jboss.as.controller.ModelVersion;
import org.jboss.as.controller.SubsystemRegistration;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.teiid.logging.LogManager;
import org.teiid.runtime.JBossLogger;

public class TeiidExtension implements Extension {

    public static final String TEIID_SUBSYSTEM = "teiid"; //$NON-NLS-1$
    public static ModelVersion TEIID_VERSION = ModelVersion.create(1, 2);

    public static ResourceDescriptionResolver getResourceDescriptionResolver(final String keyPrefix) {
        return new StandardResourceDescriptionResolver(keyPrefix,
                IntegrationPlugin.BUNDLE_NAME,
                TeiidExtension.class.getClassLoader(), true, false);
    }

    @Override
    public void initialize(ExtensionContext context) {
        final SubsystemRegistration subsystem = context.registerSubsystem(TEIID_SUBSYSTEM, TEIID_VERSION);

        LogManager.setLogListener(new JBossLogger());

        subsystem.registerXMLElementWriter(TeiidSubsystemParser.INSTANCE);

        // Main Teiid system, with children query engine and translators, register only if this is a server
        subsystem.registerSubsystemModel(new TeiidSubsytemResourceDefinition(context.getProcessType().isServer()));
    }

    @Override
    public void initializeParsers(ExtensionParsingContext context) {
        context.setSubsystemXmlMapping(TEIID_SUBSYSTEM, Namespace.CURRENT.getUri(), TeiidSubsystemParser.INSTANCE);
        context.setSubsystemXmlMapping(TEIID_SUBSYSTEM, Namespace.TEIID_1_1.getUri(), TeiidSubsystemParser.INSTANCE);
    }
}
