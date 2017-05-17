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
package org.teiid.olingo.service;

import org.teiid.core.BundleUtil.Event;
import org.teiid.core.TeiidProcessingException;

@SuppressWarnings("serial")
public class TeiidNotImplementedException extends TeiidProcessingException {

    public TeiidNotImplementedException() {
        super();
    }

    public TeiidNotImplementedException(Event code, String message) {
        super(code, message);
    }

    public TeiidNotImplementedException(Event code, Throwable t, String message) {
        super(code, t, message);
    }

    public TeiidNotImplementedException(Event code, Throwable t) {
        super(code, t);
    }

    public TeiidNotImplementedException(String message) {
        super(message);
    }

    public TeiidNotImplementedException(Throwable e, String message) {
        super(e, message);
    }

    public TeiidNotImplementedException(Throwable e) {
        super(e);
    }
}
