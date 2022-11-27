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

package org.teiid.translator.google.api.result;

import java.util.List;


/**
 * Executable query that will retrieve just specified portion of results (rows).
 *
 * For example to get rows starting at row 10 and retrieves 5 rows (included) use this interface:
 *
 *   partialResultExecutor.getResultBatch(10,5)
 *
 * @author fnguyen
 */
public interface PartialResultExecutor {

    /**
     *  Returns part of the result.
     *
     * @return null or empty list if no more results are in the batch. Maximum amount of sheet rows in the result
     * is amount
     */
    List<SheetRow> getResultsBatch(int startIndex, int amount);
}
