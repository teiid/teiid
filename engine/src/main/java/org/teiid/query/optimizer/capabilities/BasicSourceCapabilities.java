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

package org.teiid.query.optimizer.capabilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ExecutionFactory.Format;

/**
 */
public class BasicSourceCapabilities implements SourceCapabilities, Serializable {

    private static final long serialVersionUID = -1779069588746365579L;

    private Map<Capability, Boolean> capabilityMap = new HashMap<Capability, Boolean>();
    private Map<String, Boolean> functionMap = new TreeMap<String, Boolean>(String.CASE_INSENSITIVE_ORDER);
    private Map<Capability, Object> propertyMap = new HashMap<Capability, Object>();
    private ExecutionFactory<?, ?> translator;

    /**
     * Construct a basic capabilities object.
     */
    public BasicSourceCapabilities() {
    }

    public boolean supportsCapability(Capability capability) {
        Boolean supports = capabilityMap.get(capability);
        return (supports == null) ? false : supports.booleanValue();
    }

    public boolean supportsFunction(String functionName) {
        Boolean supports = functionMap.get(functionName);
        return (supports == null) ? false : supports.booleanValue();
    }

    public void setCapabilitySupport(Capability capability, boolean supports) {
        if (supports && capability == Capability.QUERY_AGGREGATES) {
            capabilityMap.put(Capability.QUERY_GROUP_BY, true);
            capabilityMap.put(Capability.QUERY_HAVING, true);
        } else {
            capabilityMap.put(capability, supports);
        }
    }

    public void setFunctionSupport(String function, boolean supports) {
        functionMap.put(function, Boolean.valueOf(supports));
    }

    public String toString() {
        return "BasicSourceCapabilities<caps=" + capabilityMap + ", funcs=" + functionMap + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * This method adds the Source Property to the Property Map
     * @param propertyName
     * @param value
     * @since 4.4
     */
    public void setSourceProperty(Capability propertyName, Object value) {
        this.propertyMap.put(propertyName, value);
    }

    @Override
    public Object getSourceProperty(Capability propertyName) {
        return this.propertyMap.get(propertyName);
    }

    @Override
    public boolean supportsConvert(int sourceType, int targetType) {
        if (this.translator == null) {
            return true;
        }
        return this.translator.supportsConvert(sourceType, targetType);
    }

    public void setTranslator(ExecutionFactory<?, ?> translator) {
        this.translator = translator;
    }

    public boolean supportsFormatLiteral(String literal, Format format) {
        if (this.translator == null) {
            return false;
        }
        return this.translator.supportsFormatLiteral(literal, format);
    }

}
