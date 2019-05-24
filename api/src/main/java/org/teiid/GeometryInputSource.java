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

package org.teiid;

import java.io.InputStream;
import java.io.Reader;

import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;

/**
 * Used to abstract how geometry and geography values are retrieved.
 *
 * Converted by the engine into a {@link GeometryType} or {@link GeographyType}
 *
 */
public abstract class GeometryInputSource {

    public InputStream getEwkb() throws Exception {
        return null;
    }

    public Integer getSrid() {
        return null;
    }

    public Reader getGml() throws Exception {
        return null;
    }

}
