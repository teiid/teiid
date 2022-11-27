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

package org.teiid.language;

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a batch of INSERT, UPDATE and DELETE commands to be executed together.
 */
public class BatchedUpdates extends BaseLanguageObject implements Command {

    private static final int MAX_COMMANDS_TOSTRING = 5;
    private List<Command> updateCommands;
    private boolean singleResult;

    public BatchedUpdates(List<Command> updateCommands) {
        this.updateCommands = updateCommands;
    }

    /**
     * @return a list of IInsert, IUpdate and IDelete commands in this batched update.
     */
    public List<Command> getUpdateCommands() {
        return updateCommands;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Whether the batch represents a logical unit of work
     * It is not required that this be treated as atomic, but
     * the translator can use this as hint
     * @return
     */
    public boolean isSingleResult() {
        return singleResult;
    }

    public void setSingleResult(boolean atomic) {
        this.singleResult = atomic;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean allCommands) {
        StringBuffer result = new StringBuffer();
        int toDisplay = updateCommands.size();
        boolean overMax = false;
        if (!allCommands && toDisplay > MAX_COMMANDS_TOSTRING) {
            toDisplay = MAX_COMMANDS_TOSTRING;
            overMax = true;
        }
        for (int i = 0; i < toDisplay; i++) {
            if (i > 0) {
                result.append("\n"); //$NON-NLS-1$
            }
            result.append(updateCommands.get(i).toString());
            result.append(";"); //$NON-NLS-1$
        }
        if (overMax) {
            result.append("\n..."); //$NON-NLS-1$
        }
        return result.toString();
    }

}
