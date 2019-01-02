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

import java.util.HashMap;
import java.util.Map;

enum Namespace {
    // must be first
    UNKNOWN(null),
    TEIID_1_1("urn:jboss:domain:teiid:1.1"), //$NON-NLS-1$
    TEIID_1_2("urn:jboss:domain:teiid:1.2"); //$NON-NLS-1$

    /**
     * The current namespace version.
     */
    public static final Namespace CURRENT = TEIID_1_2;

    private final String uri;

    Namespace(String uri) {
        this.uri = uri;
    }

    /**
     * Get the URI of this namespace.
     *
     * @return the URI
     */
    public String getUri() {
        return uri;
    }

    private static final Map<String, Namespace> namespaces;

    static {
        final Map<String, Namespace> map = new HashMap<String, Namespace>();
        for (Namespace namespace : values()) {
            final String name = namespace.getUri();
            if (name != null) map.put(name, namespace);
        }
        namespaces = map;
    }

    /**
     * Converts the specified uri to a {@link Namespace}.
     * @param uri a namespace uri
     * @return the matching namespace enum.
     */
    public static Namespace forUri(String uri) {
        final Namespace element = namespaces.get(uri);
        return element == null ? UNKNOWN : element;
    }
}
