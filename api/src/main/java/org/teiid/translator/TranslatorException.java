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

package org.teiid.translator;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidException;

/**
 * An exception the connector writer can return in case of an
 * error while using the connector.
 */
public class TranslatorException extends TeiidException{

    private static final long serialVersionUID = -5980862789340592219L;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public TranslatorException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TranslatorException( String message ) {
        super( message );
    }


    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public TranslatorException( Throwable e, String message ) {
        super(e, message);
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public TranslatorException(Throwable e) {
        this(e, getMessage(e));
    }

    public TranslatorException(BundleUtil.Event event, Throwable e) {
        this(event, e, getMessage(e));
    }

    private static String getMessage(Throwable e) {
        if (e == null) {
            return null;
        }
        String message = e.getMessage();
        if (message != null) {
            return message;
        }
        //the class name can sometimes help the user not dig for the full stacktrace
        return e.getClass().toString();
    }

    public TranslatorException(BundleUtil.Event event, Throwable e, String message) {
        super(event, e, message);
    }

    public TranslatorException(BundleUtil.Event event, String message) {
        super(event, message);
    }
}
