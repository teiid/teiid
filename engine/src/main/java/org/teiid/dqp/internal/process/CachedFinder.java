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

package org.teiid.dqp.internal.process;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.translator.SourceSystemFunctions;


/**
 */
public class CachedFinder implements CapabilitiesFinder {

    static class InvalidCaps extends BasicSourceCapabilities {

    };
    private static BasicSourceCapabilities SYSTEM_CAPS = new BasicSourceCapabilities();
    static {
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_IN, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_ONLY_LITERAL_COMPARE, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_LIKE_REGEX, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.CRITERIA_SIMILAR, true);
        SYSTEM_CAPS.setCapabilitySupport(Capability.NO_PROJECTION, true);
        SYSTEM_CAPS.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        SYSTEM_CAPS.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 100000);
        SYSTEM_CAPS.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
    }

    private ConnectorManagerRepository connectorRepo;
    private VDBMetaData vdb;

    private Map<String, SourceCapabilities> userCache = new HashMap<String, SourceCapabilities>();

    public CachedFinder(ConnectorManagerRepository repo, VDBMetaData vdb) {
        this.connectorRepo = repo;
        this.vdb = vdb;
        userCache.put(CoreConstants.SYSTEM_MODEL, SYSTEM_CAPS);
        userCache.put(CoreConstants.ODBC_MODEL, SYSTEM_CAPS);
        userCache.put(CoreConstants.SYSTEM_ADMIN_MODEL, SYSTEM_CAPS);
    }

    /**
     * Find capabilities used the cache if possible, otherwise do the lookup.
     */
    public SourceCapabilities findCapabilities(String modelName) throws TeiidComponentException {
        SourceCapabilities caps = userCache.get(modelName);
        if(caps != null) {
            return caps;
        }
        ModelMetaData model = vdb.getModel(modelName);
        List<String> sourceNames = model.getSourceNames();
        if (sourceNames.isEmpty()) {
            throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30499, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30499, modelName));
        }
        TeiidException cause = null;
        for (String sourceName:sourceNames) {
            //TOOD: in multi-source mode it may be necessary to compute minimal capabilities across the sources
            ConnectorManager mgr = this.connectorRepo.getConnectorManager(sourceName);
            if (mgr == null) {
                throw new TeiidComponentException(QueryPlugin.Event.TEIID30497, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30497, modelName, sourceName));
            }
            try {
                caps = mgr.getCapabilities();
                break;
            } catch(TeiidException e) {
                cause = e;
                LogManager.logDetail(LogConstants.CTX_DQP, e, "Could not obtain capabilities for" + sourceName); //$NON-NLS-1$
            }
        }

        if (caps == null) {
            InvalidCaps ic = new InvalidCaps();
            ic.setSourceProperty(Capability.INVALID_EXCEPTION, cause);
            caps = ic;
        }

        userCache.put(modelName, caps);
        return caps;
    }

    public boolean isValid(String modelName) {
        SourceCapabilities caps = userCache.get(modelName);
        return caps != null && !(caps instanceof InvalidCaps);
    }

}
