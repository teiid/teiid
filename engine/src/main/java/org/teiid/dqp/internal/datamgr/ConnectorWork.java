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

package org.teiid.dqp.internal.datamgr;

import org.teiid.common.buffer.BlockedException;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.TranslatorException;


/**
 * Represents a connector execution in batched form.
 */
public interface ConnectorWork {

    void cancel(boolean abnormal);

    AtomicResultsMessage more() throws TranslatorException, BlockedException;

    void close();

    void execute() throws TranslatorException, BlockedException;

    boolean isDataAvailable();

    CacheDirective getCacheDirective() throws TranslatorException;

    boolean isForkable();

    boolean isThreadBound();

    AtomicRequestID getId();
}