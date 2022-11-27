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

package org.teiid.query.optimizer.relational.rules;

import org.teiid.query.optimizer.relational.OptimizerRule;

public final class RuleConstants {

    private RuleConstants() { }

    public static final OptimizerRule PLACE_ACCESS = new RulePlaceAccess();
    public static final OptimizerRule PUSH_SELECT_CRITERIA = new RulePushSelectCriteria();
    public static final OptimizerRule ACCESS_PATTERN_VALIDATION = new RuleAccessPatternValidation();
    public static final OptimizerRule MERGE_VIRTUAL = new RuleMergeVirtual();
    public static final OptimizerRule CHOOSE_JOIN_STRATEGY = new RuleChooseJoinStrategy();
    public static final OptimizerRule RAISE_ACCESS = new RuleRaiseAccess();
    public static final OptimizerRule CHOOSE_DEPENDENT = new RuleChooseDependent();
    public static final OptimizerRule COLLAPSE_SOURCE = new RuleCollapseSource();
    public static final OptimizerRule COPY_CRITERIA = new RuleCopyCriteria();
    public static final OptimizerRule CLEAN_CRITERIA = new RuleCleanCriteria();
    public static final OptimizerRule VALIDATE_WHERE_ALL = new RuleValidateWhereAll();
    public static final OptimizerRule REMOVE_OPTIONAL_JOINS = new RuleRemoveOptionalJoins();
    public static final OptimizerRule PUSH_NON_JOIN_CRITERIA = new RulePushNonJoinCriteria(true);
    public static final OptimizerRule RAISE_NULL = new RuleRaiseNull();
    public static final OptimizerRule PLAN_JOINS = new RulePlanJoins();
    public static final OptimizerRule IMPLEMENT_JOIN_STRATEGY = new RuleImplementJoinStrategy();
    public static final OptimizerRule PUSH_LIMIT = new RulePushLimit();
    public static final OptimizerRule PLAN_UNIONS = new RulePlanUnions();
    public static final OptimizerRule PLAN_PROCEDURES = new RulePlanProcedures();
    public static final OptimizerRule CALCULATE_COST = new RuleCalculateCost();
    public static final OptimizerRule PLAN_SORTS = new RulePlanSorts();
    public static final OptimizerRule DECOMPOSE_JOIN = new RuleDecomposeJoin();
    public static final OptimizerRule SUBSTITUTE_EXPRESSIONS = new RuleSubstituteExpressions();
    public static final OptimizerRule PLAN_OUTER_JOINS = new RulePlanOuterJoins();
    public static final OptimizerRule PUSH_LARGE_IN = new RulePushLargeIn();
    public static final OptimizerRule MERGE_CRITERIA = new RuleMergeCriteria();
}
