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

package org.teiid.client.xa;

import javax.transaction.xa.XAException;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;


/**
 * Exception which occurs if an error occurs within the server that is
 * XA transaction-related.
 */
public class XATransactionException extends TeiidProcessingException {
    private static final long serialVersionUID = 5685144848609237877L;
    private int errorCode = XAException.XAER_RMERR;

    public XATransactionException(Throwable e) {
        super(e);
    }

    public XATransactionException(BundleUtil.Event event, int code, Throwable e) {
        super( event, e);
        this.errorCode = code;
    }

    public XATransactionException(BundleUtil.Event event, int code, Throwable e, String msg) {
        super(event, e, msg);
        this.errorCode = code;
    }

    public XATransactionException(BundleUtil.Event event, Throwable e) {
        super(event, e);
    }

    public XATransactionException(BundleUtil.Event event, int code, String msg) {
        super(event, msg);
        this.errorCode = code;
    }

    public XATransactionException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }

    public XAException getXAException() {
        Throwable actualException = getCause();
        if (actualException instanceof XAException) {
            return (XAException)actualException;
        }
        return new XAException(errorCode);
    }

} // END CLASS

