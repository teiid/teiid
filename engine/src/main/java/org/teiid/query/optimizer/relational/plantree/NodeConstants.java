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

package org.teiid.query.optimizer.relational.plantree;

public final class NodeConstants {

    // Can't instantiate
    private NodeConstants() { }

    /** Types of nodes */
    public static final class Types {
        private Types() {}

        static final int NO_TYPE = 0;
        public static final int ACCESS = 1<<0;
        public static final int DUP_REMOVE = 1<<1;
        public static final int JOIN = 1<<2;
        public static final int PROJECT = 1<<3;
        public static final int SELECT = 1<<4;
        public static final int SORT = 1<<5;
        public static final int SOURCE = 1<<6;
        public static final int GROUP = 1<<7;
        public static final int SET_OP = 1<<8;
        public static final int NULL = 1<<9;
        public static final int TUPLE_LIMIT = 1<<10;
    }

    /**
     * Convert a type code into a type string.
     * @param type Type code, as defined in NodeConstants.Types
     * @return String representation for code
     */
    public static final String getNodeTypeString(int type) {
        switch(type) {
            case NodeConstants.Types.ACCESS:        return "Access"; //$NON-NLS-1$
            case NodeConstants.Types.DUP_REMOVE:    return "DupRemoval"; //$NON-NLS-1$
            case NodeConstants.Types.JOIN:          return "Join"; //$NON-NLS-1$
            case NodeConstants.Types.PROJECT:       return "Project"; //$NON-NLS-1$
            case NodeConstants.Types.SELECT:        return "Select"; //$NON-NLS-1$
            case NodeConstants.Types.SORT:          return "Sort"; //$NON-NLS-1$
            case NodeConstants.Types.SOURCE:        return "Source"; //$NON-NLS-1$
            case NodeConstants.Types.GROUP:         return "Group"; //$NON-NLS-1$
            case NodeConstants.Types.SET_OP:        return "SetOperation"; //$NON-NLS-1$
            case NodeConstants.Types.NULL:          return "Null"; //$NON-NLS-1$
            case NodeConstants.Types.TUPLE_LIMIT:   return "TupleLimit"; //$NON-NLS-1$
            default:                                return "Unknown: " + type; //$NON-NLS-1$
        }
    }

    /** Property names for type-specific node properties */
    public enum Info {
        ATOMIC_REQUEST,      // Command
        MODEL_ID,            // Object (model ID)
        PROCEDURE_CRITERIA,
        PROCEDURE_INPUTS,
        PROCEDURE_DEFAULTS,
        IS_MULTI_SOURCE,
        SOURCE_NAME,
        CONFORMED_SOURCES, //Set <model id>
        SUB_PLAN,
        SUB_PLANS,

        // Set operation properties
        SET_OPERATION,      // SetQuery.Operation
        USE_ALL,            // Boolean

        // Join node properties
        JOIN_CRITERIA,      // List <CompareCriteria>
        JOIN_TYPE,          // JoinType
        SINGLE_MATCH,       // Boolean
        JOIN_STRATEGY,      // JoinStrategyType
        LEFT_EXPRESSIONS,   // List <SingleElementSymbol>
        RIGHT_EXPRESSIONS,  // List <SingleElementSymbol>
        DEPENDENT_VALUE_SOURCE, // String
        NON_EQUI_JOIN_CRITERIA,      // List <CompareCriteria>
        SORT_LEFT,  // SortOption
        SORT_RIGHT,     // SortOption
        IS_OPTIONAL,          // Boolean
        IS_LEFT_DISTINCT,     // Boolean
        IS_RIGHT_DISTINCT,     // Boolean
        IS_SEMI_DEP,        // Boolean
        PRESERVE,
        RIGHT_NESTED_REFERENCES,
        // Project node properties
        PROJECT_COLS,       // List <SingleElementSymbol>
        INTO_GROUP,         // GroupSymbol
        HAS_WINDOW_FUNCTIONS,        // Boolean
        CONSTRAINT,
        UPSERT, //Boolean

        // Select node properties
        SELECT_CRITERIA,    // Criteria
        IS_HAVING,          // Boolean
        //phantom nodes represent the previous position of criteria that has been pushed across a source, group, or union node.
        //phantom nodes are used by RuleCopyCriteria and removed by RuleCleanCriteria.
        IS_PHANTOM,         // Boolean
        IS_TEMPORARY,         // Boolean
        IS_COPIED,           // Boolean - used in CopyCriteria to mark which selects have already been copied
        IS_PUSHED,           // true if this node has already been pushed
        IS_DEPENDENT_SET, // Boolean - only used with dependent joins

        // Sort node properties
        SORT_ORDER,         // OrderBy
        UNRELATED_SORT,     // Boolean
        IS_DUP_REMOVAL,        // Boolean

        // Source node properties
        SYMBOL_MAP,         // SymbolMap
        PARTITION_INFO,        // Map<ElementSymbol, List<Set<Constant>>> - it will only be consistent in the initial stages of planning
        VIRTUAL_COMMAND,    // Command
        MAKE_DEP,           // Option.Makedep
        PROCESSOR_PLAN,     // ProcessorPlan for non-relational sub plan
        NESTED_COMMAND,     // Command for nested processor plan
        TABLE_FUNCTION,     // Table Function
        CORRELATED_REFERENCES,  // SymbolMap
        MAKE_NOT_DEP,       // Boolean
        INLINE_VIEW,        // If the source node represents an inline view
        NO_UNNEST,
        MAKE_IND,
        SOURCE_HINT,
        CHECK_MAT_VIEW,

        // Group node properties
        GROUP_COLS,         // List <Expression>
        ROLLUP,             // Boolean

        // Special constant used in converting plan to process for all nodes
        OUTPUT_COLS,        // List <SingleElementSymbol>

        // Plan Node Cost Estimate Constants
        EST_SET_SIZE,        // Integer represents the estimated set size this node would produce for a sibling node as the independent node in a dependent join scenario
        EST_DEP_CARDINALITY, // Float value that represents the estimated cardinality (amount of rows) produced by this node as the dependent node in a dependent join scenario
        EST_DEP_JOIN_COST,   // Float value that represents the estimated cost of a dependent join (the join strategy for this could be Nested Loop or Merge)
        EST_JOIN_COST,       // Float value that represents the estimated cost of a merge join (the join strategy for this could be Nested Loop or Merge)
        EST_CARDINALITY,     // Float represents the estimated cardinality (amount of rows) produced by this node
        EST_COL_STATS,
        EST_SELECTIVITY,     // Float that represents the selectivity of a criteria node

        // Tuple limit and offset
        MAX_TUPLE_LIMIT,     // Expression that evaluates to the max number of tuples generated
        OFFSET_TUPLE_COUNT,  // Expression that evaluates to the tuple offset of the starting tuple
        IS_IMPLICIT_LIMIT,   // Boolean if the limit is created by the rewriter as part of a subquery optimization
        IS_NON_STRICT,         // Boolean if the unordered limit should not be enforced strictly

        // Common AP Information
        ACCESS_PATTERNS,     // Collection <List <Object element ID> >
        ACCESS_PATTERN_USED, // List <Object element ID>
        REQUIRED_ACCESS_PATTERN_GROUPS,
        APPROXIMATE_OUTPUT_COLUMNS, //int approximate number of output columns

    }
}