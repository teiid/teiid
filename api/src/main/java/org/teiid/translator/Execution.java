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


/**
 * An execution represents the state and lifecycle for a particular
 * command execution.  The methods provided on this interface define
 * standard lifecycle methods.
 * When execution completes, the {@link #close()} will be called.  If
 * execution must be aborted, due to user or administrator action, the
 * {@link #cancel()} will be called.
 */
public interface Execution {

    /**
     * Terminates the execution normally.
     */
    void close();

    /**
     * Cancels the execution abnormally.  This will happen via
     * a different thread from the one performing the execution, so
     * should be expected to happen in a multi-threaded scenario.
     */
    void cancel() throws TranslatorException;

    /**
     * Execute the associated command.  Results will be retrieved through a specific sub-interface call.
     * @throws TranslatorException
     */
    void execute() throws TranslatorException;

}
