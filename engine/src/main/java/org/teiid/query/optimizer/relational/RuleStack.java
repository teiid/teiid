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

import java.util.LinkedList;

public class RuleStack {

    private RelationalPlanner planner;
    private LinkedList<OptimizerRule> rules = new LinkedList<OptimizerRule>();

    public void push(OptimizerRule rule) {
        rules.addFirst(rule);
    }

    public void addLast(OptimizerRule rule) {
        rules.addLast(rule);
    }

    public OptimizerRule pop() {
        if(rules.isEmpty()) {
            return null;
        }
        return rules.removeFirst();
    }

    public boolean isEmpty() {
        return rules.isEmpty();
    }

    public int size() {
        return rules.size();
    }

    /**
     * Remove all occurrences of this rule in the stack
     * @param rule The rule to remove
     * @since 4.2
     */
    public void remove(OptimizerRule rule) {
        while(rules.remove(rule)) {}
    }

    public boolean contains(OptimizerRule rule) {
        return rules.contains(rule);
    }

    public void setPlanner(RelationalPlanner planner) {
        this.planner = planner;
    }

    public RelationalPlanner getPlanner() {
        return planner;
    }

    public RuleStack clone() {
        RuleStack clone = new RuleStack();
        clone.rules.addAll(this.rules);
        clone.planner = this.planner;
        return clone;
    }

}