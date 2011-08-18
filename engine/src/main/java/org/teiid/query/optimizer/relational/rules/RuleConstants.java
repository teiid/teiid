/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
	public static final OptimizerRule ASSIGN_OUTPUT_ELEMENTS = new RuleAssignOutputElements();
    public static final OptimizerRule COPY_CRITERIA = new RuleCopyCriteria();
    public static final OptimizerRule CLEAN_CRITERIA = new RuleCleanCriteria();    
    public static final OptimizerRule VALIDATE_WHERE_ALL = new RuleValidateWhereAll();    
    public static final OptimizerRule REMOVE_OPTIONAL_JOINS = new RuleRemoveOptionalJoins();
    public static final OptimizerRule PUSH_NON_JOIN_CRITERIA = new RulePushNonJoinCriteria();
    public static final OptimizerRule RAISE_NULL = new RuleRaiseNull();
    public static final OptimizerRule PLAN_JOINS = new RulePlanJoins();
    public static final OptimizerRule IMPLEMENT_JOIN_STRATEGY = new RuleImplementJoinStrategy();
    public static final OptimizerRule PUSH_LIMIT = new RulePushLimit();
    public static final OptimizerRule PLAN_UNIONS = new RulePlanUnions();
    public static final OptimizerRule PLAN_PROCEDURES = new RulePlanProcedures();
    public static final OptimizerRule CALCULATE_COST = new RuleCalculateCost();
    public static final OptimizerRule PLAN_SORTS = new RulePlanSorts();
    public static final OptimizerRule DECOMPOSE_JOIN = new RuleDecomposeJoin();
}
