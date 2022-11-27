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

package org.teiid.jdbc;

import java.sql.Statement;

/**
 * A callback for continuous result processing.
 * {@link Statement#close()} must still be called to release
 * statement resources.
 *
 * Statement methods, such as cancel, are perfectly valid
 * even when using a callback.
 */
public interface ContinuousStatementCallback extends StatementCallback {

    /**
     * Called before the next execution iteration has begun.
     * There is no valid row at the time of this call.  Any attempt to access the current row
     * will result in an exception.
     * @param s
     * @throws Exception
     */
    void beforeNextExecution(Statement s) throws Exception;

}
