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

package org.teiid.query.optimizer.relational;


public final class PlanHints {

    // This flag indicates that the plan has a criteria somewhere
    public boolean hasCriteria = false;

    // This flag indicates that the plan has a join somewhere
    public boolean hasJoin = false;

    // This flag indicates that the plan has a virtual group somewhere
    public boolean hasVirtualGroups = false;

    // flag indicates that the plan has a union somewhere
    public boolean hasSetQuery = false;

    // flag indicating that the plan has a grouping node somewhere
    public boolean hasAggregates = false;

    public boolean hasLimit = false;

    public boolean hasRelationalProc = false;

    public boolean hasFunctionBasedColumns;

    public boolean hasRowBasedSecurity;

    public PlanHints() { }

    public String toString(){
        return "PlanHints"; //$NON-NLS-1$
    }
}
