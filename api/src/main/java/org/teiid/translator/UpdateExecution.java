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

import org.teiid.language.BatchedUpdates;
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.Update;


/**
 * The update execution represents the case where a connector can
 * execute an {@link Insert}, {@link Update}, {@link Delete}, or {@link BatchedUpdates} command.
 */
public interface UpdateExecution extends Execution {

    /**
     * Returns the update counts for the execution.
     * <br>A single positive integer value is expected for non bulk/batch commands.
     * <br>bulk/batch should return an integer for each value/command.  0 or greater for successful update count, -2 for no info, -3 failure
     * @return the update counts corresponding to the command executed
     * @throws DataNotAvailableException
     * @throws TranslatorException
     */
    int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException;

}
