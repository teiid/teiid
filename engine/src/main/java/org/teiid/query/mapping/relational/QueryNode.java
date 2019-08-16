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

package org.teiid.query.mapping.relational;

import org.teiid.query.sql.lang.Command;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;


/**
 * <p>The QueryNode represents a virtual or temporary group in the modeler.  QueryNodes may
 * be nested to indicate data queries built from other virtual or temporary groups.  The
 * root node of a tree of QueryNode objects should be defining a virtual group.  Leaves
 * should be other physical or virtual groups.  Internal nodes of the tree are temporary
 * groups.
 *
 * <p>A QueryNode must have a group name and a query.  It may have a command (just used
 * for convenient storage during conversion - this is not persisted).
 */
public class QueryNode {

    // Initial state
    private String query;
    // After parsing and resolution
    private Command command;
    private UpdateInfo updateInfo;

    /**
     * Construct a query node with the required parameters.
     * @param query SQL query
     */
    public QueryNode(String query) {
        this.query = query;
    }

    /**
     * Get SQL query
     * @return SQL query
     */
    public String getQuery() {
        return this.query;
    }

    /**
     * Set the SQL query
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * Set command - this is provided as a convenient place to cache this command
     * during conversion.
     * @param command Command corresponding to query
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * Get command corresponding to query, may be null
     * @return command Command corresponding to query
     */
    public Command getCommand() {
        return this.command;
    }

    /**
     * Print plantree structure starting at this node
     * @return String representing this node and all children under this node
     */
    public String toString() {
        return query;
    }

    public UpdateInfo getUpdateInfo() {
        return updateInfo;
    }

    public void setUpdateInfo(UpdateInfo updateInfo) {
        this.updateInfo = updateInfo;
    }

}
