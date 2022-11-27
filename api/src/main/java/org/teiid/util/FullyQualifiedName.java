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

package org.teiid.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;

/**
 * Class for creating fully qualified names
 */
public class FullyQualifiedName {

    public static final String SEPARATOR = "/"; //$NON-NLS-1$

    private StringBuilder builder = new StringBuilder();

    public FullyQualifiedName() {

    }

    public FullyQualifiedName(String name, String value) {
        super();
        append(name, value);
    }

    public FullyQualifiedName append(String name, String value) {
        Assertion.isNotNull(name);
        Assertion.isNotNull(value);
        if (builder.length() > 0) {
            builder.append(SEPARATOR);
        }
        try {
            builder.append(URLEncoder.encode(name, "UTF-8")).append("=").append(URLEncoder.encode(value, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } catch (UnsupportedEncodingException e) {
            throw new TeiidRuntimeException(e);
        }
        return this;
    }

    @Override
    public String toString() {
        return builder.toString();
    }

}
