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

package org.teiid.adminapi;

import java.util.List;


/**
 * Represents a metadata model in the Teiid system.
 *
 * @since 4.3
 */
public interface Model extends AdminObject {

    enum Type {PHYSICAL, VIRTUAL, FUNCTION, OTHER};
    enum MetadataStatus {LOADING, LOADED, FAILED, RETRYING};

    /**
     * Description about the model
     * @return
     */
    String getDescription();

    /**
     * Determine if this model is a Source model.
     *
     * @return <code>true</code> if it contains physical group(s).
     */
    boolean isSource();

    /**
     * Determine whether this model is exposed for querying.
     *
     * <br>Note: for imported models, this may be overriden.  See {@link VDB#isVisible(String)}
     *
     * @return <code>true</code> if the model is visible
     * for querying.
     */
    boolean isVisible();

    /**
     * Retrieve the model type.
     * @return model type
     */
    Type getModelType();

    /**
     * Determine whether this model can support more than one source.
     *
     * @return <code>true</code> if this model supports multiple sources
     */
    boolean isSupportsMultiSourceBindings();

    /**
     * Associated Source Names for the Models
     * @return String
     */
    List<String> getSourceNames();

    /**
     * Get the configured JNDI name for the given source name.
     * @param sourceName - name of the source
     * @return null if none configured.
     */
    String getSourceConnectionJndiName(String sourceName);


    /**
     * Get the configured translator name for the given source
     * @param sourceName
     * @return
     */
    String getSourceTranslatorName(String sourceName);

    /**
     * Shows any validity errors present in the model
     * @return
     */
    List<String> getValidityErrors();

    /**
     * Metadata Load status of the model.
     * @return
     */
    MetadataStatus getMetadataStatus();
}