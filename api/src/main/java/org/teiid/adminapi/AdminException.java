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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidException;


/**
 * <code>AdminException</code> is the base exception for the admin package.
 * Many *Admin methods throw this exception.
 * Instances will be one of the concrete subtypes:
 * {@link AdminComponentException} or {@link AdminProcessingException}
 *
 * <p><code>AdminException</code>s may contain multiple child exceptions. An example
 * of this could be when performing an admin action results in multiple failures. Admin
 * clients should be aware of this and use the {@link #hasMultiple()} method to
 * determine if they need to check the child exceptions.
 */
public abstract class AdminException extends TeiidException {

    private static final long serialVersionUID = -4446936145500241358L;
    // List of Admin exceptions in
    // case of multiple failure
    private List<AdminException> children;

    /**
     * No-arg ctor.
     *
     * @since 4.3
     */
    AdminException() {
        super();
    }

    /**
     * Construct with a message.
     * @param msg the error message.
     * @since 4.3
     */
    AdminException(String msg) {
        super(msg);
    }

    AdminException(Throwable cause) {
        super(cause);
    }

    /**
     * Construct with an optional error code and a message.
     * @param code an optional error code
     * @param msg the error message.
     * @since 4.3
     */
    AdminException(BundleUtil.Event code, String msg) {
        super(code, msg);
    }

    AdminException(String msg, Throwable cause) {
        super(cause, msg);
    }

    AdminException(BundleUtil.Event code, Throwable cause, String msg) {
        super(code, cause, msg);
    }

    AdminException(BundleUtil.Event code, Throwable cause) {
        super(code, cause);
    }

    /**
     * Determine whether this exception is representing
     * mutliple component failures.
     * @return <code>true</code> iff this exception contains multiple
     * component failure exceptions.
     * @since 4.3
     */
    public boolean hasMultiple() {
        return (children != null && children.size() > 0);
    }

    /**
     * Returns a non-null list of failures (<code>AdminException</code>s), one for each
     * component that failed.
     *
     * <p>The list will have members when {@link #hasMultiple()} returns <code>true</code>.
     * @return The non-null list of failures.
     * @since 4.3
     */
    public List<AdminException> getChildren() {
        if (children == null) {
            return Collections.emptyList();
        }
        return children;
    }

    /**
     * Add a child <code>AdminException</code> for a particular failure
     * if and action resulted in multiple failures.
     *
     * @param child a specific failure
     * @since 4.3
     */
    public void addChild(AdminException child) {
        if ( children == null ) {
            children = new ArrayList<AdminException>();
        }
        children.add(child);
    }
}
