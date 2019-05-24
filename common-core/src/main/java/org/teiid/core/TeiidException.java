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

package org.teiid.core;

import java.sql.SQLException;



/**
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class TeiidException extends Exception {

    private static final long serialVersionUID = -3033427629587497938L;
    protected String code;
    private transient String originalType;

    public TeiidException() {
    }

    public TeiidException(String message) {
        super(message);
    }

    public TeiidException(BundleUtil.Event code, final String message) {
        super(message);
        setCode(code.toString());
    }

    public TeiidException(BundleUtil.Event code, Throwable t, final String message) {
        super(message, t);
        if (message != null && t != null && message.equals(t.getMessage())) {
            setCode(code, t);
        } else {
            setCode(code.toString());
        }
    }

    public TeiidException(BundleUtil.Event code, Throwable t) {
        super(t);
        setCode(code, t);
    }

    private void setCode(BundleUtil.Event code, Throwable t) {
        String codeStr = code.toString();
        if (t instanceof TeiidException) {
            TeiidException te = (TeiidException)t;
            if (te.getCode() != null) {
                codeStr = te.getCode();
            }
        }
        setCode(codeStr);
    }

    public TeiidException(Throwable e) {
        this(e, e != null? e.getMessage() : null);
    }

    public TeiidException(Throwable e, String message) {
        super(message, e);
        setCode(getCode(e));
    }

    public String getCode() {
        return this.code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getOriginalType() {
        return originalType;
    }

    public void setOriginalType(String originalType) {
        this.originalType = originalType;
    }

    static String getCode(Throwable e) {
        if (e instanceof TeiidException) {
            return (((TeiidException) e).getCode());
        } else if (e instanceof TeiidRuntimeException) {
            return ((TeiidRuntimeException) e).getCode();
        } else if (e instanceof SQLException) {
            return ((SQLException)e).getSQLState();
        }
        return null;
    }

    public String getMessage() {
        String message = super.getMessage();
        if (message == null) {
            return code;
        }
        if (code == null || code.length() == 0 || message.startsWith(code)) {
            return message;
        }
        return code+" "+message; //$NON-NLS-1$
    }

}
