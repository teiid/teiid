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

package org.teiid.query.rewriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.types.Transform;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.json.simple.JSONParser;
import org.teiid.language.Like.MatchMode;
import org.teiid.language.SQLConstants;
import org.teiid.language.WindowFrame.BoundMode;
import org.teiid.language.WindowFrame.FrameMode;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.AliasGenerator;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.optimizer.relational.rules.RulePlanSubqueries;
import org.teiid.query.optimizer.relational.rules.RulePlanSubqueries.PlannedResult;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.command.InsertResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageObject.Util;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.FilteredCommand;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.ImmutableCompareCriteria;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.JsonTable;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.TableFunctionReference.ProjectedColumn;
import org.teiid.query.sql.lang.TargetedCommand;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.proc.ExpressionStatement;
import org.teiid.query.sql.proc.IfStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.RaiseStatement;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.proc.WhileStatement;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.symbol.WindowFrame;
import org.teiid.query.sql.symbol.WindowFrame.FrameBound;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor.EvaluationLevel;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.UpdateValidator.UpdateMapping;
import org.teiid.query.validator.ValidationVisitor;
import org.teiid.translator.SourceSystemFunctions;


/**
 * Rewrites commands and command fragments to a form that is better for planning and execution.  There is a current limitation that
 * command objects themselves cannot change type, since the same object is always used.
 */
public class QueryRewriter {

    private static final Constant TRUE_CONSTANT = new Constant(true);
    private static final Constant FALSE_CONSTANT = new Constant(false);

    private static final String WRITE_THROUGH = "write-through"; //$NON-NLS-1$

    private static final Constant ZERO_CONSTANT = new Constant(0, DataTypeManager.DefaultDataClasses.INTEGER);
    public static final CompareCriteria TRUE_CRITERIA = new ImmutableCompareCriteria(new Constant(1, DataTypeManager.DefaultDataClasses.INTEGER), CompareCriteria.EQ, new Constant(1, DataTypeManager.DefaultDataClasses.INTEGER));
    public static final CompareCriteria FALSE_CRITERIA = new ImmutableCompareCriteria(new Constant(1, DataTypeManager.DefaultDataClasses.INTEGER), CompareCriteria.EQ, ZERO_CONSTANT);
    public static final CompareCriteria UNKNOWN_CRITERIA = new ImmutableCompareCriteria(new Constant(null, DataTypeManager.DefaultDataClasses.STRING), CompareCriteria.NE, new Constant(null, DataTypeManager.DefaultDataClasses.STRING));

    private static final Map<String, String> ALIASED_FUNCTIONS = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    private static final Set<String> PARSE_FORMAT_TYPES = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

    static {
        ALIASED_FUNCTIONS.put("lower", SourceSystemFunctions.LCASE); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("upper", SourceSystemFunctions.UCASE); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("cast", SourceSystemFunctions.CONVERT); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("nvl", SourceSystemFunctions.IFNULL); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("||", SourceSystemFunctions.CONCAT); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("chr", SourceSystemFunctions.CHAR); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("substr", SourceSystemFunctions.SUBSTRING); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("st_geomfrombinary", SourceSystemFunctions.ST_GEOMFROMWKB); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put(SQLConstants.Reserved.CURRENT_DATE, SourceSystemFunctions.CURDATE);
        ALIASED_FUNCTIONS.put("character_length", SourceSystemFunctions.LENGTH); //$NON-NLS-1$
        ALIASED_FUNCTIONS.put("char_length", SourceSystemFunctions.LENGTH); //$NON-NLS-1$
        PARSE_FORMAT_TYPES.addAll(    Arrays.asList(DataTypeManager.DefaultDataTypes.TIME,
            DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.BIG_DECIMAL,
            DataTypeManager.DefaultDataTypes.BIG_INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.LONG,
            DataTypeManager.DefaultDataTypes.FLOAT, DataTypeManager.DefaultDataTypes.DOUBLE));
    }

    // Constants used in simplifying mathematical criteria
    private final static Integer INTEGER_ZERO = new Integer(0);
    private final static Double DOUBLE_ZERO = new Double(0);
    private final static Float FLOAT_ZERO = new Float(0);
    private final static Long LONG_ZERO = new Long(0);
    private final static BigInteger BIG_INTEGER_ZERO = new BigInteger("0"); //$NON-NLS-1$
    private final static BigDecimal BIG_DECIMAL_ZERO = new BigDecimal("0"); //$NON-NLS-1$
    private final static Short SHORT_ZERO = new Short((short)0);
    private final static Byte BYTE_ZERO = new Byte((byte)0);
    private boolean rewriteAggs = true;

    private boolean preserveUnknown;

    private QueryMetadataInterface metadata;
    private CommandContext context;

    private boolean rewriteSubcommands;
    private boolean processing;
    private boolean preEvaluation;
    private Evaluator evaluator;
    private Map<ElementSymbol, Expression> variables; //constant propagation

    private QueryRewriter(QueryMetadataInterface metadata,
            CommandContext context) {
        this.metadata = metadata;
        this.context = context;
        this.evaluator = new Evaluator(Collections.emptyMap(), null, context);
    }

    public static Command evaluateAndRewrite(Command command, Evaluator eval, CommandContext context, QueryMetadataInterface metadata) throws TeiidProcessingException, TeiidComponentException {
        QueryRewriter queryRewriter = new QueryRewriter(metadata, context);
        queryRewriter.evaluator = eval;
        queryRewriter.rewriteSubcommands = true;
        queryRewriter.processing = true;
        return queryRewriter.rewriteCommand(command, false);
    }

    public static Criteria evaluateAndRewrite(Criteria criteria, Evaluator eval, CommandContext context, QueryMetadataInterface metadata) throws TeiidProcessingException, TeiidComponentException {
        QueryRewriter queryRewriter = new QueryRewriter(metadata, context);
        queryRewriter.evaluator = eval;
        queryRewriter.rewriteSubcommands = true;
        queryRewriter.preEvaluation = true;
        return queryRewriter.rewriteCriteria(criteria);
    }

    public static Command rewrite(Command command, QueryMetadataInterface metadata, CommandContext context, Map<ElementSymbol, Expression> variableValues) throws TeiidComponentException, TeiidProcessingException{
        QueryRewriter rewriter = new QueryRewriter(metadata, context);
        rewriter.rewriteSubcommands = true;
        rewriter.variables = variableValues;
        return rewriter.rewriteCommand(command, false);
    }

    public static Command rewrite(Command command, QueryMetadataInterface metadata, CommandContext context) throws TeiidComponentException, TeiidProcessingException{
        return rewrite(command, metadata, context, null);
    }

    /**
     * Rewrites the command and all of its subcommands (both embedded and non-embedded)
     *
     * @param command
     * @param removeOrderBy
     * @return
     * @throws QueryValidatorException
     */
    private Command rewriteCommand(Command command, boolean removeOrderBy) throws TeiidComponentException, TeiidProcessingException{
        boolean oldRewriteAggs = rewriteAggs;
        QueryMetadataInterface oldMetadata = metadata;

        TempMetadataStore tempMetadata = command.getTemporaryMetadata();
        if(tempMetadata != null) {
            metadata = new TempMetadataAdapter(metadata, tempMetadata);
        }

        switch(command.getType()) {
            case Command.TYPE_QUERY:
                QueryCommand queryCommand = (QueryCommand)command;
                if (removeOrderBy && queryCommand.getLimit() == null) {
                    queryCommand.setOrderBy(null);
                }
                List<WithQueryCommand> withList = queryCommand.getWith();
                if (withList != null) {
                    queryCommand.setWith(null);
                    List<UnaryFromClause> clauses = getUnaryFromClauses(queryCommand);
                    queryCommand.setWith(withList);
                    outer: for (int i = withList.size() - 1; i >= 0; i--) {
                        WithQueryCommand withQueryCommand = withList.get(i);
                        if (withQueryCommand.getColumns() == null) {
                            List<ElementSymbol> columns = ResolverUtil.resolveElementsInGroup(withQueryCommand.getGroupSymbol(), metadata);
                            withQueryCommand.setColumns(LanguageObject.Util.deepClone(columns, ElementSymbol.class));
                        }
                        Collection<UnaryFromClause> all = new ArrayList<UnaryFromClause>(clauses);
                        List<UnaryFromClause> current = getUnaryFromClauses(withQueryCommand.getCommand());
                        clauses.addAll(current);
                        rewriteSubqueryContainer(withQueryCommand, true);

                        //can't inline with a hint or once it's planned
                        if (withQueryCommand.isNoInline() || withQueryCommand.getCommand().getProcessorPlan() != null || processing) {
                            //TODO: in the processing case we may want to remove unneeded cte declarations, rather than
                            //pushing them down
                            continue;
                        }

                        boolean removeOnly = false;
                        //check for scalar with clauses
                        boolean replaceScalar = replaceScalar(withQueryCommand);
                        if (!replaceScalar) {
                            int referenceCount = 0;
                            for (UnaryFromClause ufc : all) {
                                if (ufc.getGroup().getMetadataID() != withQueryCommand.getGroupSymbol().getMetadataID()) {
                                    continue;
                                }
                                referenceCount++;
                                if (referenceCount > 1) {
                                    continue outer; //referenced in more than 1 location
                                }
                            }
                            if (referenceCount == 0) {
                                removeOnly = true;
                            } else if (withQueryCommand.isRecursive()) {
                                continue; //can't inline if recursive
                            }
                        }
                        withList.remove(i);
                        if (withList.isEmpty()) {
                            queryCommand.setWith(null);
                        }
                        if (removeOnly) {
                            clauses = clauses.subList(0, clauses.size() - current.size());
                            continue;
                        }
                        for (UnaryFromClause clause : all) {
                            //we match on equality as the name can be redefined
                            if (clause.getGroup().getMetadataID() != withQueryCommand.getGroupSymbol().getMetadataID()) {
                                continue;
                            }
                            if (!replaceScalar) {
                                //use the original since we need to keep the references
                                //to nested unaryfromclause instances
                                clause.setExpandedCommand(withQueryCommand.getCommand());
                                break;
                            }
                            clause.setExpandedCommand((Command) withQueryCommand.getCommand().clone());
                        }
                    }
                }
                if(command instanceof Query) {
                    command = rewriteQuery((Query) command);
                }else {
                    command = rewriteSetQuery((SetQuery) command);
                }
                break;
            case Command.TYPE_STORED_PROCEDURE:
                command = rewriteExec((StoredProcedure) command);
                break;
            case Command.TYPE_INSERT:
                command = rewriteInsert((Insert) command);
                break;
            case Command.TYPE_UPDATE:
                command = rewriteUpdate((Update) command);
                break;
            case Command.TYPE_DELETE:
                command = rewriteDelete((Delete) command);
                break;
            case Command.TYPE_UPDATE_PROCEDURE:
                command = rewriteUpdateProcedure((CreateProcedureCommand) command);
                break;
            case Command.TYPE_BATCHED_UPDATE:
                List subCommands = ((BatchedUpdateCommand)command).getUpdateCommands();
                for (int i = 0; i < subCommands.size(); i++) {
                    Command subCommand = (Command)subCommands.get(i);
                    subCommand = rewriteCommand(subCommand, false);
                    subCommands.set(i, subCommand);
                }
                break;
            case Command.TYPE_TRIGGER_ACTION:
                TriggerAction ta = (TriggerAction)command;
                ta.setBlock(rewriteBlock(ta.getBlock()));
                break;
        }

        this.rewriteAggs = oldRewriteAggs;
        this.metadata = oldMetadata;
        return command;
    }

    private boolean replaceScalar(WithQueryCommand withQueryCommand) {
        if (!GroupCollectorVisitor.getGroups(withQueryCommand.getCommand(), false).isEmpty()) {
            return false;
        }
        //if deterministic, just inline
        return !FunctionCollectorVisitor.isNonDeterministic(withQueryCommand.getCommand());
    }

    private List<UnaryFromClause> getUnaryFromClauses(QueryCommand queryCommand) {
        final List<UnaryFromClause> clauses = new ArrayList<UnaryFromClause>();

        LanguageVisitor visitor = new LanguageVisitor() {

            public void visit(UnaryFromClause obj) {
                clauses.add(obj);
            }
        };
        DeepPreOrderNavigator.doVisit(queryCommand, visitor);
        return clauses;
    }

    private Command rewriteUpdateProcedure(CreateProcedureCommand command) throws TeiidComponentException {
        Block block = rewriteBlock(command.getBlock());
        command.setBlock(block);
        return command;
    }

    private Block rewriteBlock(Block block) throws TeiidComponentException {
        List<Statement> statements = block.getStatements();
        List<Statement> newStmts = rewriteStatements(statements);
        block.setStatements(newStmts);
        if (block.getExceptionStatements() != null) {
            block.setExceptionStatements(rewriteStatements(block.getExceptionStatements()));
        }
        return block;
     }

    private List<Statement> rewriteStatements(List<Statement> statements) throws TeiidComponentException {
        Iterator<Statement> stmtIter = statements.iterator();

        List<Statement> newStmts = new ArrayList<Statement>(statements.size());
        // plan each statement in the block
        while(stmtIter.hasNext()) {
            Statement stmnt = stmtIter.next();
            try {
                rewriteStatement(stmnt, newStmts);
            } catch (TeiidProcessingException e) {
                /*
                 * defer the processing of the exception until runtime as there may be an exception handler
                 */
                RaiseStatement raise = new RaiseStatement(new Constant(e));
                newStmts.add(raise);
                break;
            }
        }
        return newStmts;
    }

    private void rewriteStatement(Statement statement, List<Statement> newStmts)
                                 throws TeiidComponentException, TeiidProcessingException{

        // evaluate the HAS Criteria on the procedure and rewrite
        int stmtType = statement.getType();
        switch(stmtType) {
            case Statement.TYPE_IF:
                IfStatement ifStmt = (IfStatement) statement;
                Criteria ifCrit = ifStmt.getCondition();
                Criteria evalCrit = rewriteCriteria(ifCrit);

                ifStmt.setCondition(evalCrit);
                if(evalCrit.equals(TRUE_CRITERIA)) {
                    Block ifblock = rewriteBlock(ifStmt.getIfBlock());
                    if (ifblock.isAtomic()) {
                        newStmts.add(ifblock);
                    } else {
                        newStmts.addAll(ifblock.getStatements());
                    }
                    return;
                } else if(evalCrit.equals(FALSE_CRITERIA) || evalCrit.equals(UNKNOWN_CRITERIA)) {
                    if(ifStmt.hasElseBlock()) {
                        Block elseBlock = rewriteBlock(ifStmt.getElseBlock());
                        if (elseBlock.isAtomic()) {
                            newStmts.add(elseBlock);
                        } else {
                            newStmts.addAll(elseBlock.getStatements());
                        }
                        return;
                    }
                    return;
                } else {
                    Block ifblock = rewriteBlock(ifStmt.getIfBlock());
                    ifStmt.setIfBlock(ifblock);
                    if(ifStmt.hasElseBlock()) {
                        Block elseBlock = rewriteBlock(ifStmt.getElseBlock());
                        ifStmt.setElseBlock(elseBlock);
                    }
                }
                break;
            case Statement.TYPE_ERROR:
            case Statement.TYPE_DECLARE:
            case Statement.TYPE_ASSIGNMENT:
            case Statement.TYPE_RETURN:
                ExpressionStatement exprStmt = (ExpressionStatement) statement;
                // replace variables to references, these references are later
                // replaced in the processor with variable values
                Expression expr = exprStmt.getExpression();
                if (expr != null) {
                    boolean preserveUnknownOld = preserveUnknown;
                    preserveUnknown = true;
                    expr = rewriteExpressionDirect(expr);
                    preserveUnknown = preserveUnknownOld;
                    exprStmt.setExpression(expr);
                }
                break;
            case Statement.TYPE_COMMAND:
                CommandStatement cmdStmt = (CommandStatement) statement;
                rewriteSubqueryContainer(cmdStmt, false);

                if(cmdStmt.getCommand().getType() == Command.TYPE_UPDATE) {
                    Update update = (Update)cmdStmt.getCommand();
                    if (update.getChangeList().isEmpty()) {
                        return;
                    }
                }
                break;
            case Statement.TYPE_LOOP:
                LoopStatement loop = (LoopStatement)statement;

                rewriteSubqueryContainer(loop, false);

                rewriteBlock(loop.getBlock());

                if (loop.getBlock().getStatements().isEmpty()) {
                    return;
                }
                break;
            case Statement.TYPE_WHILE:
                WhileStatement whileStatement = (WhileStatement) statement;
                Criteria crit = whileStatement.getCondition();
                crit = rewriteCriteria(crit);

                whileStatement.setCondition(crit);
                if(crit.equals(FALSE_CRITERIA) || crit.equals(UNKNOWN_CRITERIA)) {
                    return;
                }
                whileStatement.setBlock(rewriteBlock(whileStatement.getBlock()));

                if (whileStatement.getBlock().getStatements().isEmpty()) {
                    return;
                }
                break;
            case Statement.TYPE_COMPOUND:
                statement = rewriteBlock((Block) statement);
                break;
        }
        newStmts.add(statement);
    }

    /**
     * @param removeOrderBy
     * @param container
     * @throws QueryValidatorException
     */
    private void rewriteSubqueryContainer(SubqueryContainer container, boolean removeOrderBy) throws TeiidComponentException, TeiidProcessingException{
        if (rewriteSubcommands && container.getCommand() != null && (container.getCommand().getProcessorPlan() == null || processing)) {
            container.setCommand(rewriteCommand(container.getCommand(), removeOrderBy));
        }
    }

    private Command rewriteQuery(Query query)
             throws TeiidComponentException, TeiidProcessingException{

        // Rewrite from clause
        From from = query.getFrom();
        if(from != null){
            List<FromClause> clauses = new ArrayList<FromClause>(from.getClauses().size());
            Iterator<FromClause> clauseIter = from.getClauses().iterator();
            while(clauseIter.hasNext()) {
                clauses.add( rewriteFromClause(query, clauseIter.next()) );
            }
            from.setClauses(clauses);
        } else {
            query.setOrderBy(null);
        }

        // Rewrite criteria
        Criteria crit = query.getCriteria();
        if(crit != null) {
            boolean preserveUnknownOld = preserveUnknown;
            preserveUnknown = false;
            Criteria clone = null;
            if (processing && query.getGroupBy() == null && query.hasAggregates()) {
                clone = (Criteria) crit.clone();
            }
            crit = rewriteCriteria(crit);
            preserveUnknown = preserveUnknownOld;
            if(crit.equals(TRUE_CRITERIA)) {
                query.setCriteria(null);
            } else if (crit.equals(UNKNOWN_CRITERIA)) {
                query.setCriteria(FALSE_CRITERIA);
            } else {
                query.setCriteria(crit);
            }

            //attempt to workaround a soft spot in planning with
            //aggregates and an always false predicate
            if (clone != null && query.getCriteria() != null && query.getCriteria().equals(FALSE_CRITERIA)) {
                List<Criteria> crits = new ArrayList<Criteria>();
                List<Criteria> parts = Criteria.separateCriteriaByAnd(clone);
                if (parts.size() > 1) {
                    for (Criteria c : parts) {
                        crits.add(rewriteCriteria(c));
                    }
                    query.setCriteria(new CompoundCriteria(parts));
                }
            }
        }

        if (from != null) {
            rewriteSubqueriesAsJoins(query);
        }

        query = rewriteGroupBy(query);

        // Rewrite having
        Criteria having = query.getHaving();
        if(having != null) {
            boolean preserveUnknownOld = preserveUnknown;
            preserveUnknown = false;
            crit = rewriteCriteria(having);
            preserveUnknown = preserveUnknownOld;
            if(crit == TRUE_CRITERIA) {
                query.setHaving(null);
            } else {
                query.setHaving(crit);
            }
        }

        //remove multiple element symbols
        boolean hasMes = false;
        for (Expression ex : query.getSelect().getSymbols()) {
            if (ex instanceof MultipleElementSymbol) {
                hasMes = true;
            }
        }
        if (hasMes) {
            query.getSelect().setSymbols(query.getSelect().getProjectedSymbols());
        }

        boolean preserveUnknownOld = preserveUnknown;
        preserveUnknown = true;
        rewriteExpressions(query.getSelect());

        if (from != null) {
            List<Expression> symbols = query.getSelect().getSymbols();
            RulePlanSubqueries rmc = new RulePlanSubqueries(null, null, null, this.context, this.metadata);
            TreeSet<String> names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            List<GroupSymbol> groups = query.getFrom().getGroups();
            for (GroupSymbol gs : groups) {
                names.add(gs.getName());
            }
            PlannedResult plannedResult = new PlannedResult();
            for (int i = 0; i < symbols.size(); i++) {
                Expression symbol = symbols.get(i);
                plannedResult.reset();
                rmc.findSubquery(SymbolMap.getExpression(symbol), context!=null?context.getOptions().isSubqueryUnnestDefault():false, plannedResult, true);
                if (plannedResult.query == null || plannedResult.query.getProcessorPlan() != null
                        || plannedResult.query.getFrom() == null) {
                    continue;
                }
                determineCorrelatedReferences(groups, plannedResult);
                boolean requiresDistinct = RulePlanSubqueries.requiresDistinctRows(query);
                if (!rmc.planQuery(groups, requiresDistinct, plannedResult)) {
                    continue;
                }
                Query q = convertToJoin(plannedResult, names, query, true);
                symbols.set(i, new AliasSymbol(ExpressionSymbol.getName(symbol), (Expression) q.getProjectedSymbols().get(0).clone()));
            }
        }

        query = (Query)rewriteOrderBy(query);
        preserveUnknown = preserveUnknownOld;

        if (query.getLimit() != null) {
            query.setLimit(rewriteLimitClause(query.getLimit()));
        }

        if (query.getInto() != null) {
            return rewriteSelectInto(query);
        }

        return query;
    }

    private void rewriteSubqueriesAsJoins(Query query)
            throws TeiidComponentException, QueryMetadataException,
            QueryResolverException {
        if (query.getCriteria() == null) {
            return;
        }
        RulePlanSubqueries rmc = new RulePlanSubqueries(null, null, null, this.context, this.metadata);
        List<Criteria> current = Criteria.separateCriteriaByAnd(query.getCriteria());
        query.setCriteria(null);
        List<GroupSymbol> groups = query.getFrom().getGroups();
        TreeSet<String> names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        for (GroupSymbol gs : groups) {
            names.add(gs.getName());
        }
        for (Iterator<Criteria> crits = current.iterator(); crits.hasNext();) {
            PlannedResult plannedResult = rmc.findSubquery(crits.next(), context!=null?context.getOptions().isSubqueryUnnestDefault():false);
            if (plannedResult.not || plannedResult.query == null || plannedResult.query.getProcessorPlan() != null
                    || plannedResult.query.getWith() != null) {
                continue;
            }
            determineCorrelatedReferences(groups, plannedResult);
            boolean requiresDistinct = RulePlanSubqueries.requiresDistinctRows(query);
            if (!rmc.planQuery(groups, requiresDistinct, plannedResult)) {
                continue;
            }
            crits.remove();
            convertToJoin(plannedResult, names, query, false);
            //transform the query into an inner join
        }
        query.setCriteria(Criteria.combineCriteria(query.getCriteria(), Criteria.combineCriteria(current)));
    }

    private Query convertToJoin(PlannedResult plannedResult, Set<String> names, Query query, boolean leftOuter) throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        GroupSymbol viewName = RulePlaceAccess.recontextSymbol(new GroupSymbol("X"), names); //$NON-NLS-1$
        viewName.setName(viewName.getName());
        viewName.setDefinition(null);
        Query q = createInlineViewQuery(viewName, plannedResult.query, metadata, plannedResult.query.getSelect().getProjectedSymbols());

        Iterator<Expression> iter = q.getSelect().getProjectedSymbols().iterator();
        HashMap<Expression, Expression> expressionMap = new HashMap<Expression, Expression>();
        for (Expression symbol : plannedResult.query.getSelect().getProjectedSymbols()) {
            expressionMap.put(SymbolMap.getExpression(symbol), SymbolMap.getExpression(iter.next()));
        }
        for (int i = 0; i < plannedResult.leftExpressions.size(); i++) {
            plannedResult.nonEquiJoinCriteria.add(new CompareCriteria(SymbolMap.getExpression((Expression)plannedResult.leftExpressions.get(i)), CompareCriteria.EQ, (Expression)plannedResult.rightExpressions.get(i)));
        }
        Criteria mappedCriteria = Criteria.combineCriteria(plannedResult.nonEquiJoinCriteria);
        ExpressionMappingVisitor.mapExpressions(mappedCriteria, expressionMap);
        FromClause clause = q.getFrom().getClauses().get(0);
        if (plannedResult.makeInd) {
            clause.setMakeInd(new Option.MakeDep());
        }
        if (leftOuter) {
            FromClause leftClause = query.getFrom().getClauses().get(0);
            if (query.getFrom().getClauses().size() > 1) {
                leftClause = null;
                for (FromClause fc : query.getFrom().getClauses()) {
                    if (leftClause == null) {
                        leftClause = fc;
                        continue;
                    }
                    leftClause = new JoinPredicate(leftClause, fc, JoinType.JOIN_CROSS);
                }
            }
            query.getFrom().getClauses().clear();
            JoinPredicate jp = new JoinPredicate(leftClause, clause, JoinType.JOIN_LEFT_OUTER);
            jp.setJoinCriteria(Criteria.separateCriteriaByAnd(mappedCriteria));
            query.getFrom().getClauses().add(jp);
        } else {
            query.setCriteria(Criteria.combineCriteria(query.getCriteria(), mappedCriteria));
            query.getFrom().addClause(clause);
        }
        query.getTemporaryMetadata().getData().putAll(q.getTemporaryMetadata().getData());
        return q;
    }

    private void determineCorrelatedReferences(List<GroupSymbol> groups,
            PlannedResult plannedResult) {
        if (plannedResult.query.getCorrelatedReferences() == null) {
            //create the correlated refs if they exist
            //there is a little bit of a design problem here that null usually means no refs.
            ArrayList<Reference> correlatedReferences = new ArrayList<Reference>();
            CorrelatedReferenceCollectorVisitor.collectReferences(plannedResult.query, groups, correlatedReferences, metadata);
            if (!correlatedReferences.isEmpty()) {
                SymbolMap map = new SymbolMap();
                for (Reference reference : correlatedReferences) {
                    map.addMapping(reference.getExpression(), reference.getExpression());
                }
                plannedResult.query.setCorrelatedReferences(map);
            }
        }
    }

    private Query rewriteGroupBy(Query query) throws TeiidComponentException, TeiidProcessingException {
        if (query.getGroupBy() == null) {
            rewriteAggs = false;
            return query;
        }
        if (isDistinctWithGroupBy(query)) {
            query.getSelect().setDistinct(false);
        }
        rewriteExpressions(query.getGroupBy());
        List<Expression> expr = query.getGroupBy().getSymbols();
        for (Iterator<Expression> iter = expr.iterator(); iter.hasNext();) {
            if (EvaluatableVisitor.willBecomeConstant(iter.next())) {
                iter.remove();
            }
        }
        if (expr.isEmpty()) {
            query.setGroupBy(null);
        }
        return query;
    }

    public static boolean isDistinctWithGroupBy(Query query) {
        GroupBy groupBy = query.getGroupBy();
        if (groupBy == null) {
            return false;
        }
        HashSet<Expression> selectExpressions = new HashSet<Expression>();
        for (Expression selectExpr : query.getSelect().getProjectedSymbols()) {
            selectExpressions.add(SymbolMap.getExpression(selectExpr));
        }
        for (Expression groupByExpr : groupBy.getSymbols()) {
            if (!selectExpressions.contains(groupByExpr)) {
                return false;
            }
        }
        return true;
    }

    private void rewriteExpressions(LanguageObject obj) throws TeiidComponentException, TeiidProcessingException{
        if (obj == null) {
            return;
        }
        ExpressionMappingVisitor visitor = new ExpressionMappingVisitor(null) {
            /**
             * @see org.teiid.query.sql.visitor.ExpressionMappingVisitor#replaceExpression(org.teiid.query.sql.symbol.Expression)
             */
            @Override
            public Expression replaceExpression(Expression element) {
                try {
                    return rewriteExpressionDirect(element);
                } catch (TeiidException err) {
                     throw new TeiidRuntimeException(err);
                }
            }
        };
        try {
            PostOrderNavigator.doVisit(obj, visitor);
        } catch (TeiidRuntimeException err) {
            if (err.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)err.getCause();
            }
            if (err.getCause() instanceof TeiidProcessingException) {
                throw (TeiidProcessingException)err.getCause();
            }
            throw err;
        }
    }

    /**
     * Rewrite the order by clause.
     * Unrelated order by expressions will cause the creation of nested inline views.
     *
     * @param queryCommand
     */
    public QueryCommand rewriteOrderBy(QueryCommand queryCommand) throws TeiidComponentException, TeiidProcessingException {
        final OrderBy orderBy = queryCommand.getOrderBy();
        if (orderBy == null) {
            return queryCommand;
        }
        Select select = queryCommand.getProjectedQuery().getSelect();
        final List<Expression> projectedSymbols = select.getProjectedSymbols();

        rewriteOrderBy(queryCommand, orderBy, projectedSymbols, this);

        return queryCommand;
    }

    public static void rewriteOrderBy(QueryCommand queryCommand,
            final OrderBy orderBy, final List projectedSymbols, CommandContext context, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException {
        QueryRewriter rewriter = new QueryRewriter(metadata, context);
        rewriteOrderBy(queryCommand, orderBy, projectedSymbols, rewriter);
    }

    private static void rewriteOrderBy(QueryCommand queryCommand,
            final OrderBy orderBy, final List projectedSymbols, QueryRewriter rewriter)
            throws TeiidComponentException, TeiidProcessingException {
        HashSet<Expression> previousExpressions = new HashSet<Expression>();
        for (int i = 0; i < orderBy.getVariableCount(); i++) {
            Expression querySymbol = orderBy.getVariable(i);
            int index = orderBy.getExpressionPosition(i);
            if (index == -1) {
                querySymbol = rewriter.rewriteExpressionDirect(querySymbol);
            } else {
                querySymbol = (Expression)projectedSymbols.get(index);
            }
            Expression expr = SymbolMap.getExpression(querySymbol);
            if (!previousExpressions.add(expr) || (queryCommand instanceof Query && EvaluatableVisitor.willBecomeConstant(expr))) {
                orderBy.removeOrderByItem(i--);
            } else {
                orderBy.getOrderByItems().get(i).setSymbol((Expression)querySymbol.clone());
            }
        }
        if (orderBy.getVariableCount() == 0) {
            queryCommand.setOrderBy(null);
        }
    }

    /**
     * This method will alias each of the select into elements to the corresponding column name in the
     * target table.  This ensures that they will all be uniquely named.
     *
     * @param query
     * @throws QueryValidatorException
     */
    private Command rewriteSelectInto(Query query) throws TeiidProcessingException{
        Into into = query.getInto();
        try {
            List<ElementSymbol> allIntoElements = Util.deepClone(ResolverUtil.resolveElementsInGroup(into.getGroup(), metadata), ElementSymbol.class);
            Insert insert = new Insert(into.getGroup(), allIntoElements, Collections.emptyList());
            insert.setSourceHint(query.getSourceHint());
            query.setSourceHint(null);
            query.setInto(null);
            insert.setQueryExpression(query);
            return rewriteInsert(correctDatatypes(insert));
        } catch (QueryMetadataException e) {
             throw new QueryValidatorException(e);
        } catch (TeiidComponentException e) {
             throw new QueryValidatorException(e);
        }
    }

    private Insert correctDatatypes(Insert insert) {
        boolean needsView = false;
        for (int i = 0; !needsView && i < insert.getVariables().size(); i++) {
            Expression ses = insert.getVariables().get(i);
            if (ses.getType() != insert.getQueryExpression().getProjectedSymbols().get(i).getType()) {
                needsView = true;
            }
        }
        if (needsView) {
            try {
                insert.setQueryExpression(createInlineViewQuery(new GroupSymbol("X"), insert.getQueryExpression(), metadata, insert.getVariables())); //$NON-NLS-1$
            } catch (TeiidException err) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30371, err);
            }
        }
        return insert;
    }

    private void correctProjectedTypes(List actualSymbolTypes, Query query) {

        List symbols = query.getSelect().getProjectedSymbols();

        List newSymbols = SetQuery.getTypedProjectedSymbols(symbols, actualSymbolTypes, this.metadata);

        query.getSelect().setSymbols(newSymbols);
    }

    private SetQuery rewriteSetQuery(SetQuery setQuery)
                 throws TeiidComponentException, TeiidProcessingException{

        if (setQuery.getProjectedTypes() != null) {
            for (QueryCommand command : setQuery.getQueryCommands()) {
                if (!(command instanceof Query)) {
                    continue;
                }
                correctProjectedTypes(setQuery.getProjectedTypes(), (Query)command);
            }
            setQuery.setProjectedTypes(null, null);
        }

        setQuery.setLeftQuery((QueryCommand)rewriteCommand(setQuery.getLeftQuery(), true));
        setQuery.setRightQuery((QueryCommand)rewriteCommand(setQuery.getRightQuery(), true));

        rewriteOrderBy(setQuery);

        if (setQuery.getLimit() != null) {
            setQuery.setLimit(rewriteLimitClause(setQuery.getLimit()));
        }

        return setQuery;
    }

    private FromClause rewriteFromClause(Query parent, FromClause clause)
             throws TeiidComponentException, TeiidProcessingException{
        if(clause instanceof JoinPredicate) {
            return rewriteJoinPredicate(parent, (JoinPredicate) clause);
        } else if (clause instanceof SubqueryFromClause) {
            rewriteSubqueryContainer((SubqueryFromClause)clause, true);
        } else if (clause instanceof TextTable) {
            TextTable tt = (TextTable)clause;
            tt.setFile(rewriteExpressionDirect(tt.getFile()));
        } else if (clause instanceof XMLTable) {
            rewriteExpressions(clause);
        } else if (clause instanceof JsonTable) {
            return rewriteJsonTable((JsonTable)clause);
        } else if (clause instanceof ObjectTable) {
            rewriteExpressions(clause);
        } else if (clause instanceof ArrayTable) {
            ArrayTable at = (ArrayTable)clause;
            at.setArrayValue(rewriteExpressionDirect(at.getArrayValue()));
        }
        return clause;
    }

    /**
     * Rewrite from jsontable to arraytable with jsontoarray
     * @param clause
     * @return
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    private FromClause rewriteJsonTable(JsonTable clause) throws TeiidComponentException, TeiidProcessingException {
        ArrayTable at = new ArrayTable();
        List<Expression> args = new ArrayList<Expression>();
        List<ProjectedColumn> cols = new ArrayList<>();

        args.add(rewriteExpressionDirect(clause.getJson()));
        args.add(new Constant(clause.getRowPath()));
        args.add(new Constant(Boolean.TRUE.equals(clause.getNullLeaf())));

        for (JsonTable.JsonColumn col : clause.getColumns()) {
            cols.add(new ProjectedColumn(col.getName(), col.getType()));

            if (col.isOrdinal()) {
                args.add(new Constant("ordinal")); //$NON-NLS-1$
            } else if (col.getPath() != null) {
                args.add(new Constant(col.getPath()));
            } else {
                //default path, use bracket notation to avoid escaping issues
                StringBuilder path = new StringBuilder("@['"); //$NON-NLS-1$
                try {
                    JSONParser.escape(col.getName(), path);
                } catch (IOException e) {
                    throw new TeiidRuntimeException(e);
                }
                path.append("']"); //$NON-NLS-1$
                args.add(new Constant(path.toString()));
            }
        }

        Function f = new Function(SourceSystemFunctions.JSONTOARRAY, args.toArray(new Expression[args.size()]));

        ResolverVisitor.resolveLanguageObject(f, metadata);
        at.setArrayValue(f);
        at.setCorrelatedReferences(clause.getCorrelatedReferences());
        at.setMakeDep(clause.getMakeDep());
        at.setMakeInd(clause.getMakeInd());
        at.setColumns(cols);
        at.setSingleRow(false);
        at.setName(clause.getName());
        at.getGroupSymbol().setMetadataID(clause.getGroupSymbol().getMetadataID());
        return at;
    }

    private JoinPredicate rewriteJoinPredicate(Query parent, JoinPredicate predicate)
             throws TeiidComponentException, TeiidProcessingException{
        List joinCrits = predicate.getJoinCriteria();
        if(joinCrits != null && joinCrits.size() > 0) {
            //rewrite join crits by rewriting a compound criteria
            Criteria criteria = new CompoundCriteria(new ArrayList(joinCrits));
            criteria = rewriteCriteria(criteria);
            joinCrits.clear();
            if (criteria instanceof CompoundCriteria && ((CompoundCriteria)criteria).getOperator() == CompoundCriteria.AND) {
                joinCrits.addAll(((CompoundCriteria)criteria).getCriteria());
            } else {
                joinCrits.add(criteria);
            }
            predicate.setJoinCriteria(joinCrits);
        }

        if (predicate.getJoinType() == JoinType.JOIN_UNION) {
            predicate.setJoinType(JoinType.JOIN_FULL_OUTER);
            predicate.setJoinCriteria(new ArrayList<Criteria>(Arrays.asList(FALSE_CRITERIA)));
        } else if (predicate.getJoinType() == JoinType.JOIN_RIGHT_OUTER) {
            predicate.setJoinType(JoinType.JOIN_LEFT_OUTER);
            FromClause leftClause = predicate.getLeftClause();
            predicate.setLeftClause(predicate.getRightClause());
            predicate.setRightClause(leftClause);
        }

        predicate.setLeftClause( rewriteFromClause(parent, predicate.getLeftClause()));
        predicate.setRightClause( rewriteFromClause(parent, predicate.getRightClause()));

        return predicate;
    }

    /**
     * Rewrite the criteria by evaluating some trivial cases.
     * @param criteria The criteria to rewrite
     * @param metadata
     * @return The re-written criteria
     */
    public static Criteria rewriteCriteria(Criteria criteria, CommandContext context, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException{
        return new QueryRewriter(metadata, context).rewriteCriteria(criteria);
    }

    /**
     * Rewrite the criteria by evaluating some trivial cases.
     * @param criteria The criteria to rewrite
     * in the procedural language.
     * @return The re-written criteria
     */
    private Criteria rewriteCriteria(Criteria criteria) throws TeiidComponentException, TeiidProcessingException{
        if(criteria instanceof CompoundCriteria) {
            return rewriteCriteria((CompoundCriteria)criteria, true);
        } else if(criteria instanceof NotCriteria) {
            criteria = rewriteCriteria((NotCriteria)criteria);
        } else if(criteria instanceof CompareCriteria) {
            criteria = rewriteCriteria((CompareCriteria)criteria);
        } else if(criteria instanceof SubqueryCompareCriteria) {
            criteria = rewriteCriteria((SubqueryCompareCriteria)criteria);
        } else if(criteria instanceof MatchCriteria) {
            criteria = rewriteCriteria((MatchCriteria)criteria);
        } else if(criteria instanceof SetCriteria) {
            criteria = rewriteCriteria((SetCriteria)criteria);
        } else if(criteria instanceof IsNullCriteria) {
            criteria = rewriteCriteria((IsNullCriteria)criteria);
        } else if(criteria instanceof BetweenCriteria) {
            criteria = rewriteCriteria((BetweenCriteria)criteria);
        } else if (criteria instanceof ExistsCriteria) {
            ExistsCriteria exists = (ExistsCriteria)criteria;
            if (exists.shouldEvaluate() && processing) {
                return getCriteria(evaluator.evaluate(exists, null));
            }
            rewriteSubqueryContainer((SubqueryContainer)criteria, true);
            if (!RelationalNodeUtil.shouldExecute(exists.getCommand(), false, true)) {
                return exists.isNegated()?TRUE_CRITERIA:FALSE_CRITERIA;
            }
            if (exists.getCommand().getProcessorPlan() == null) {
                if (exists.getCommand() instanceof Query) {
                    Query query = (Query)exists.getCommand();
                    if ((query.getLimit() == null || query.getOrderBy() == null) && query.getSelect().getProjectedSymbols().size() > 1) {
                        query.getSelect().clearSymbols();
                        query.getSelect().addSymbol(new ExpressionSymbol("x", new Constant(1))); //$NON-NLS-1$
                    }
                }
                addImplicitLimit(exists, 1);
            }
        } else if (criteria instanceof SubquerySetCriteria) {
            SubquerySetCriteria sub = (SubquerySetCriteria)criteria;
            rewriteWithExplicitArray(sub.getExpression(), sub);
            rewriteSubqueryContainer(sub, true);
            if (!RelationalNodeUtil.shouldExecute(sub.getCommand(), false, true)) {
                return sub.isNegated()?TRUE_CRITERIA:FALSE_CRITERIA;
            }
            if (rewriteLeftExpression(sub)) {
                addImplicitLimit(sub, 1);
            }
        } else if (criteria instanceof DependentSetCriteria) {
            criteria = rewriteDependentSetCriteria((DependentSetCriteria)criteria);
        } else if (criteria instanceof ExpressionCriteria) {
            criteria = rewriteCriteria(new CompareCriteria(((ExpressionCriteria) criteria).getExpression(), CompareCriteria.EQ, new Constant(Boolean.TRUE)));
        }

        return evaluateCriteria(criteria);
    }

    private void rewriteWithExplicitArray(Expression ex, SubqueryContainer<QueryCommand> sub)
            throws QueryMetadataException, QueryResolverException,
            TeiidComponentException {
        if (!(ex instanceof Array) || sub.getCommand() == null || sub.getCommand().getProjectedSymbols().size() == 1) {
            return;
        }
        Query query = QueryRewriter.createInlineViewQuery(new GroupSymbol("x"), sub.getCommand(), metadata, sub.getCommand().getProjectedSymbols()); //$NON-NLS-1$
        List<Expression> exprs = new ArrayList<Expression>();
        for (Expression expr : query.getSelect().getProjectedSymbols()) {
            exprs.add(SymbolMap.getExpression(expr));
        }
        Array array = new Array(exprs);
        query.getSelect().clearSymbols();
        query.getSelect().addSymbol(array);
        ResolverVisitor.resolveComponentType(array);
        sub.setCommand(query);
    }

    private void addImplicitLimit(SubqueryContainer<QueryCommand> container, int rowLimit) {
        if (container.getCommand().getLimit() != null) {
            Limit lim = container.getCommand().getLimit();
            if (lim.getRowLimit() instanceof Constant) {
                Constant c = (Constant)lim.getRowLimit();
                if (!c.isMultiValued() && Integer.valueOf(rowLimit).compareTo((Integer) c.getValue()) <= 0) {
                    lim.setRowLimit(new Constant(rowLimit));
                    if (lim.getRowLimit() == null) {
                        lim.setImplicit(true);
                        container.getCommand().setOrderBy(null);
                    }
                }
            }
            return;
        }
        boolean addLimit = true;
        if (container.getCommand() instanceof Query) {
            Query query = (Query)container.getCommand();
            addLimit = !(query.hasAggregates() && query.getGroupBy() == null);
        }
        if (addLimit) {
            Limit lim = new Limit(null, new Constant(rowLimit));
            lim.setImplicit(true);
            container.getCommand().setLimit(lim);
        }
    }

    private Criteria rewriteDependentSetCriteria(DependentSetCriteria dsc)
            throws TeiidComponentException, TeiidProcessingException{
        if (!processing) {
            if (rewriteLeftExpression(dsc)) {
                return UNKNOWN_CRITERIA;
            }
        }
        return dsc;
    }

    /**
     * Performs simple expression flattening
     *
     * @param criteria
     * @return
     */
    public static Criteria optimizeCriteria(CompoundCriteria criteria, QueryMetadataInterface metadata) {
        try {
            return new QueryRewriter(metadata, null).rewriteCriteria(criteria, false);
        } catch (TeiidException err) {
            //shouldn't happen
            return criteria;
        }
    }

    /** May be simplified if this is an AND and a sub criteria is always
     * false or if this is an OR and a sub criteria is always true
     */
    private Criteria rewriteCriteria(CompoundCriteria criteria, boolean rewrite) throws TeiidComponentException, TeiidProcessingException{
        List<Criteria> crits = criteria.getCriteria();
        int operator = criteria.getOperator();

        // Walk through crits and collect converted ones
        LinkedHashSet<Criteria> newCrits = new LinkedHashSet<Criteria>(crits.size());
        HashMap<Expression, Criteria> exprMap = new HashMap<Expression, Criteria>();
        for (Criteria converted : crits) {
            if (rewrite) {
                converted = rewriteCriteria(converted);
            } else if (converted instanceof CompoundCriteria) {
                converted = rewriteCriteria((CompoundCriteria)converted, false);
            }
            List<Criteria> critList = null;
            if (converted instanceof CompoundCriteria) {
                CompoundCriteria other = (CompoundCriteria)converted;
                if (other.getOperator() == criteria.getOperator()) {
                    critList = other.getCriteria();
                }
            }
            if (critList == null) {
                critList = Arrays.asList(converted);
            }
            for (Criteria criteria2 : critList) {
                converted = criteria2;
                //begin boolean optimizations
                if(TRUE_CRITERIA.equals(converted)) {
                    if(operator == CompoundCriteria.OR) {
                        // this OR must be true as at least one branch is always true
                        return converted;
                    }
                } else if(FALSE_CRITERIA.equals(converted)) {
                    if(operator == CompoundCriteria.AND) {
                        // this AND must be false as at least one branch is always false
                        return converted;
                    }
                } else if (UNKNOWN_CRITERIA.equals(converted)) {
                    if (preserveUnknown) {
                        newCrits.add(converted);
                    } else {
                        if(operator == CompoundCriteria.AND) {
                            return FALSE_CRITERIA;
                        }
                    }
                } else {
                    if (operator == CompoundCriteria.AND) {
                         converted = rewriteAndConjunct(converted, exprMap, newCrits);
                         if (converted != null) {
                             return converted;
                         }
                    } else {
                        //or
                        if (converted instanceof SetCriteria) {
                            SetCriteria sc = (SetCriteria)converted;
                            if (!sc.isNegated() && sc.isAllConstants()) {
                                Criteria crit = exprMap.get(sc.getExpression());
                                if (crit == null) {
                                    exprMap.put(sc.getExpression(), sc);
                                } else if (crit instanceof SetCriteria) {
                                    SetCriteria other = (SetCriteria)crit;
                                    other.getValues().addAll(sc.getValues());
                                    continue;
                                } else {
                                    newCrits.remove(crit);
                                    CompareCriteria cc = (CompareCriteria)crit;
                                    sc.getValues().add(cc.getRightExpression());
                                }
                            }
                        } else if (converted instanceof CompareCriteria) {
                            CompareCriteria cc = (CompareCriteria)converted;
                            if (cc.getOperator() == CompareCriteria.EQ && cc.getRightExpression() instanceof Constant) {
                                Criteria crit = exprMap.get(cc.getLeftExpression());
                                if (crit == null) {
                                    exprMap.put(cc.getLeftExpression(), cc);
                                } else if (crit instanceof SetCriteria) {
                                    SetCriteria other = (SetCriteria)crit;
                                    other.getValues().add(cc.getRightExpression());
                                    continue;
                                } else {
                                    newCrits.remove(crit);
                                    CompareCriteria other = (CompareCriteria)crit;
                                    SetCriteria sc = new SetCriteria(cc.getLeftExpression(), DataTypeManager.isHashable(other.getRightExpression().getType())?new LinkedHashSet<Constant>():new TreeSet<Constant>());
                                    sc.setAllConstants(true);
                                    sc.getValues().add(cc.getRightExpression());
                                    sc.getValues().add(other.getRightExpression());
                                    exprMap.put(sc.getExpression(), sc);
                                    converted = sc;
                                }
                            }
                        }
                        newCrits.add(converted);
                    }
                }
            }
        }

        if(newCrits.size() == 0) {
            if(operator == CompoundCriteria.AND) {
                return TRUE_CRITERIA;
            }
            return FALSE_CRITERIA;
        } else if(newCrits.size() == 1) {
            // Only one sub crit now, so just return it
            return newCrits.iterator().next();
        } else {
            criteria.getCriteria().clear();
            criteria.getCriteria().addAll(newCrits);
            return criteria;
        }
    }

    /**
     * Rewrite the given conjunct
     * @return null if the conjunct was internally handled
     */
    private Criteria rewriteAndConjunct(Criteria converted, Map<Expression, Criteria> exprMap, LinkedHashSet<Criteria> newCrits) {
        if (converted instanceof IsNullCriteria) {
            IsNullCriteria inc = (IsNullCriteria)converted;
            if (!inc.isNegated()) {
                Criteria crit = exprMap.get(inc.getExpression());
                if (crit == null) {
                    exprMap.put(inc.getExpression(), converted);
                } else if (!(crit instanceof IsNullCriteria)) {
                    return FALSE_CRITERIA;
                }
            }
        } else if (converted instanceof SetCriteria) {
            SetCriteria sc = (SetCriteria)converted;
            Criteria crit = exprMap.get(sc.getExpression());
            if (crit instanceof IsNullCriteria) {
                return FALSE_CRITERIA;
            }
            if (!sc.isNegated() && sc.isAllConstants()) {
                if (crit == null) {
                    exprMap.put(sc.getExpression(), converted);
                } else if (crit instanceof SetCriteria) {
                    SetCriteria sc1 = (SetCriteria)crit;
                    newCrits.remove(sc1);
                    sc1.getValues().retainAll(sc.getValues());
                    if (sc1.getValues().isEmpty()) {
                        return FALSE_CRITERIA;
                    }
                    //TODO: single value as compare criteria
                    newCrits.add(sc1);
                    exprMap.put(sc1.getExpression(), sc1);
                    return null;
                } else {
                    CompareCriteria cc = (CompareCriteria)crit;
                    for (Iterator<Constant> exprIter = sc.getValues().iterator(); exprIter.hasNext();) {
                        if (!Evaluator.compare(cc.getOperator(), exprIter.next().getValue(), ((Constant)cc.getRightExpression()).getValue())) {
                            exprIter.remove();
                        }
                    }
                    if (sc.getValues().isEmpty()) {
                        return FALSE_CRITERIA;
                    }
                    if (cc.getOperator() != CompareCriteria.EQ) {
                        newCrits.remove(cc);
                        //TODO: single value as compare criteria
                        exprMap.put(sc.getExpression(), sc);
                    } else {
                        return null;
                    }
                }
            }
        } else if (converted instanceof CompareCriteria) {
            CompareCriteria cc = (CompareCriteria)converted;
            Criteria crit = exprMap.get(cc.getLeftExpression());
            if (crit instanceof IsNullCriteria) {
                return FALSE_CRITERIA;
            }
            if (cc.getRightExpression() instanceof Constant) {
                if (crit == null) {
                    exprMap.put(cc.getLeftExpression(), cc);
                } else if (crit instanceof SetCriteria) {
                    SetCriteria sc = (SetCriteria)crit;
                    boolean modified = false;
                    for (Iterator<Constant> exprIter = sc.getValues().iterator(); exprIter.hasNext();) {
                        if (!Evaluator.compare(cc.getOperator(), exprIter.next().getValue(), ((Constant)cc.getRightExpression()).getValue())) {
                            if (!modified) {
                                modified = true;
                                newCrits.remove(sc);
                            }
                            exprIter.remove();
                        }
                    }
                    //TODO: single value as compare criteria
                    if (sc.getValues().isEmpty()) {
                        return FALSE_CRITERIA;
                    }
                    if (cc.getOperator() == CompareCriteria.EQ) {
                        exprMap.put(cc.getLeftExpression(), cc);
                    } else if (modified) {
                        if (sc.getNumberOfValues() == 1) {
                            CompareCriteria comp = new CompareCriteria(sc.getExpression(), CompareCriteria.EQ, (Expression)sc.getValues().iterator().next());
                            newCrits.add(comp);
                            exprMap.put(sc.getExpression(), comp);
                        } else {
                            newCrits.add(sc);
                            exprMap.put(sc.getExpression(), sc);
                        }
                        return null;
                    }
                } else {
                    CompareCriteria cc1 = (CompareCriteria)crit;
                    if (cc1.getOperator() == CompareCriteria.NE) {
                        exprMap.put(cc.getLeftExpression(), cc);
                    } else if (cc1.getOperator() == CompareCriteria.EQ) {
                        if (!Evaluator.compare(cc.getOperator(), ((Constant)cc1.getRightExpression()).getValue(), ((Constant)cc.getRightExpression()).getValue())) {
                            return FALSE_CRITERIA;
                        }
                        return null;
                    }
                    if (cc.getOperator() == CompareCriteria.EQ) {
                        if (!Evaluator.compare(cc1.getOperator(), ((Constant)cc.getRightExpression()).getValue(), ((Constant)cc1.getRightExpression()).getValue())) {
                            return FALSE_CRITERIA;
                        }
                        exprMap.put(cc.getLeftExpression(), cc);
                        newCrits.remove(cc1);
                    } else if (cc.getOperator() != CompareCriteria.NE && cc.getOperator() != CompareCriteria.EQ
                            && (cc.getOperator() == cc1.getOperator() || Math.abs(cc.getOperator() - cc1.getOperator()) == 2)) {
                        if (Evaluator.compare(cc.getOperator() != cc1.getOperator()?cc1.getOperator():cc.getOperator(), ((Constant)cc1.getRightExpression()).getValue(), ((Constant)cc.getRightExpression()).getValue())) {
                            return null;
                        }
                        exprMap.put(cc.getLeftExpression(), cc);
                        newCrits.remove(cc1);
                    }
                }
            }
        }
        newCrits.add(converted);
        return null;
    }

    private Criteria evaluateCriteria(Criteria crit) throws TeiidComponentException, TeiidProcessingException{
        if(EvaluatableVisitor.isFullyEvaluatable(crit, !processing)) {
            try {
                Boolean eval = evaluator.evaluateTVL(crit, Collections.emptyList());

                return getCriteria(eval);

            } catch(ExpressionEvaluationException e) {
                 throw new QueryValidatorException(QueryPlugin.Event.TEIID30372, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30372, crit));
            }
        }

        return crit;
    }

    private Criteria getCriteria(Boolean eval) {
        if (eval == null) {
            return UNKNOWN_CRITERIA;
        }

        if(Boolean.TRUE.equals(eval)) {
            return TRUE_CRITERIA;
        }

        return FALSE_CRITERIA;
    }

    private Criteria rewriteCriteria(NotCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        Criteria innerCrit = criteria.getCriteria();
        if (innerCrit instanceof CompoundCriteria) {
            //reduce to only negation of predicates, so that the null/unknown handling criteria is applied appropriately
            return rewriteCriteria(Criteria.applyDemorgan(innerCrit));
        }
        if(innerCrit == TRUE_CRITERIA) {
            return FALSE_CRITERIA;
        } else if(innerCrit == FALSE_CRITERIA) {
            return TRUE_CRITERIA;
        } else if (innerCrit == UNKNOWN_CRITERIA) {
            return UNKNOWN_CRITERIA;
        }
        if (innerCrit instanceof Negatable) {
            ((Negatable) innerCrit).negate();
            return rewriteCriteria(innerCrit);
        }
        if (innerCrit instanceof NotCriteria) {
            return rewriteCriteria(((NotCriteria)innerCrit).getCriteria());
        }
        Criteria newInnerCrit = rewriteCriteria(innerCrit);
        if (!newInnerCrit.equals(innerCrit)) {
            criteria.setCriteria(newInnerCrit);
            return rewriteCriteria(criteria);
        }
        return criteria;
    }

    /**
     * Rewrites "a [NOT] BETWEEN b AND c" as "a &gt;= b AND a &lt;= c", or as "a &lt;= b OR a&gt;= c"
     * @param criteria
     * @return
     * @throws QueryValidatorException
     */
    private Criteria rewriteCriteria(BetweenCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        CompareCriteria lowerCriteria = new CompareCriteria(criteria.getExpression(),
                                                            criteria.isNegated() ? CompareCriteria.LT: CompareCriteria.GE,
                                                            criteria.getLowerExpression());
        CompareCriteria upperCriteria = new CompareCriteria(criteria.getExpression(),
                                                            criteria.isNegated() ? CompareCriteria.GT: CompareCriteria.LE,
                                                            criteria.getUpperExpression());
        CompoundCriteria newCriteria = new CompoundCriteria(criteria.isNegated() ? CompoundCriteria.OR : CompoundCriteria.AND,
                                                            lowerCriteria,
                                                            upperCriteria);

        return rewriteCriteria(newCriteria);
    }

    private Criteria rewriteCriteria(CompareCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        if (criteria == TRUE_CRITERIA || criteria == UNKNOWN_CRITERIA || criteria == FALSE_CRITERIA) {
            return criteria;
        }
        Expression leftExpr = rewriteExpressionDirect(criteria.getLeftExpression());
        Expression rightExpr = rewriteExpressionDirect(criteria.getRightExpression());
        criteria.setLeftExpression(leftExpr);
        criteria.setRightExpression(rightExpr);

        if (isNull(leftExpr) || isNull(rightExpr)) {
            return UNKNOWN_CRITERIA;
        }

        if (leftExpr.equals(rightExpr)) {
            switch(criteria.getOperator()) {
                case CompareCriteria.LE:
                case CompareCriteria.GE:
                case CompareCriteria.EQ:
                    if (leftExpr instanceof Constant) {
                        return TRUE_CRITERIA;
                    }
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), true, true);
                default:
                    if (leftExpr instanceof Constant) {
                        return FALSE_CRITERIA;
                    }
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), false, true);
            }
        }

        boolean rightConstant = false;
        if(EvaluatableVisitor.willBecomeConstant(rightExpr)) {
            rightConstant = true;
        } else if (EvaluatableVisitor.willBecomeConstant(leftExpr)) {
            // Swap in this particular case for connectors
            criteria.setLeftExpression(rightExpr);
            criteria.setRightExpression(leftExpr);

            // Check for < or > operator as we have to switch it
            criteria.setOperator(criteria.getReverseOperator());
            rightConstant = true;
        }

        if (rightConstant
                && criteria.getLeftExpression().getType() == DataTypeManager.DefaultDataClasses.BOOLEAN
                && criteria.getRightExpression() instanceof Constant) {
            //simplify to an  comparison
            if (TRUE_CONSTANT.equals(criteria.getRightExpression())) {
                switch (criteria.getOperator()) {
                case CompareCriteria.GE:
                    criteria.setOperator(CompareCriteria.EQ);
                    break;
                case CompareCriteria.GT:
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), false, true);
                case CompareCriteria.LT:
                    criteria.setOperator(CompareCriteria.EQ);
                    criteria.setRightExpression((Expression) FALSE_CONSTANT.clone());
                    break;
                case CompareCriteria.LE:
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), true, true);
                }
            } else if (FALSE_CONSTANT.equals(criteria.getRightExpression())) {
                switch (criteria.getOperator()) {
                case CompareCriteria.GE:
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), true, true);
                case CompareCriteria.GT:
                    criteria.setOperator(CompareCriteria.EQ);
                    criteria.setRightExpression((Expression)TRUE_CONSTANT.clone());
                    break;
                case CompareCriteria.LT:
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), false, true);
                case CompareCriteria.LE:
                    criteria.setOperator(CompareCriteria.EQ);
                    break;
                }
            }
        }

        Function f = null;
        while (rightConstant && f != criteria.getLeftExpression() && criteria.getLeftExpression() instanceof Function) {
            f = (Function)criteria.getLeftExpression();
            Criteria result = simplifyWithInverse(criteria);
            if (!(result instanceof CompareCriteria)) {
                return result;
            }
            criteria = (CompareCriteria)result;
        }

        Criteria modCriteria = simplifyTimestampMerge(criteria);
        if(modCriteria instanceof CompareCriteria) {
            modCriteria = simplifyTimestampMerge2((CompareCriteria)modCriteria);
        }
        return modCriteria;
    }

    public static boolean isNull(Expression expr) {
        return expr instanceof Constant && ((Constant)expr).isNull();
    }

    /*
     * The thing of primary importance here is that the use of the 'ANY' predicate
     * quantifier is replaced with the canonical and equivalent 'SOME'
     */
    private Criteria rewriteCriteria(SubqueryCompareCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        rewriteWithExplicitArray(criteria.getArrayExpression(), criteria);
        if (criteria.getCommand() != null && criteria.getCommand().getProcessorPlan() == null) {
            if ((criteria.getOperator() == CompareCriteria.EQ && criteria.getPredicateQuantifier() != SubqueryCompareCriteria.ALL)
                    || (criteria.getOperator() == CompareCriteria.NE && criteria.getPredicateQuantifier() == SubqueryCompareCriteria.ALL)) {
                SubquerySetCriteria result = new SubquerySetCriteria(criteria.getLeftExpression(), criteria.getCommand());
                result.setNegated(criteria.getOperator() == CompareCriteria.NE);
                return rewriteCriteria(result);
            }
            if (criteria.getPredicateQuantifier() != SubqueryCompareCriteria.ALL && criteria.getOperator() != CompareCriteria.EQ && criteria.getOperator() != CompareCriteria.NE) {
                CompareCriteria cc = new CompareCriteria();
                cc.setLeftExpression(criteria.getLeftExpression());
                boolean useView = true;
                if (criteria.getCommand() instanceof Query) {
                    Query query = (Query)criteria.getCommand();
                    if (!query.hasAggregates() && query.getCriteria() != null && query.getOrderBy() == null) {
                        final boolean[] hasWindowFunctions = new boolean[1];
                        PreOrPostOrderNavigator.doVisit(query.getSelect(), new LanguageVisitor() {
                            public void visit(WindowFunction windowFunction) {
                                hasWindowFunctions[0] = true;
                            };
                        }, PreOrPostOrderNavigator.PRE_ORDER);
                        useView = hasWindowFunctions[0];
                    }
                }
                AggregateSymbol.Type type = Type.MAX;
                if (criteria.getOperator() == CompareCriteria.GT || criteria.getOperator() == CompareCriteria.GE) {
                    type = Type.MIN;
                }
                if (useView) {
                    Query q = createInlineViewQuery(new GroupSymbol("X"), criteria.getCommand(), metadata, criteria.getCommand().getProjectedSymbols()); //$NON-NLS-1$
                    Expression ses = q.getProjectedSymbols().get(0);
                    Expression expr = SymbolMap.getExpression(ses);
                    q.getSelect().clearSymbols();
                    q.getSelect().addSymbol(new AggregateSymbol(type.name(), false, expr));
                    ScalarSubquery ss = new ScalarSubquery(q);
                    ss.setSubqueryHint(criteria.getSubqueryHint());
                    cc.setRightExpression(ss);
                    cc.setOperator(criteria.getOperator());
                    return rewriteCriteria(cc);
                }
                Select select = ((Query)criteria.getCommand()).getSelect();
                Expression ex = select.getProjectedSymbols().get(0);
                ex = SymbolMap.getExpression(ex);
                select.setSymbols(Arrays.asList(new AggregateSymbol(type.name(), false, ex)));
                select.setDistinct(false);
            }
        }

        Expression leftExpr = rewriteExpressionDirect(criteria.getLeftExpression());

        if (isNull(leftExpr) && criteria.getCommand() != null) {
            addImplicitLimit(criteria, 1);
        }

        criteria.setLeftExpression(leftExpr);

        if (criteria.getPredicateQuantifier() == SubqueryCompareCriteria.ANY){
            criteria.setPredicateQuantifier(SubqueryCompareCriteria.SOME);
        }

        rewriteSubqueryContainer(criteria, true);

        if (criteria.getCommand() != null && !RelationalNodeUtil.shouldExecute(criteria.getCommand(), false, true)) {
            //TODO: this is not interpreted the same way in all databases
            //for example H2 treat both cases as false - however the spec and all major vendors support the following:
            if (criteria.getPredicateQuantifier()==SubqueryCompareCriteria.SOME) {
                return FALSE_CRITERIA;
            }
            return TRUE_CRITERIA;
        }

        return criteria;
    }

    private Criteria simplifyWithInverse(CompareCriteria criteria) throws TeiidProcessingException{
        Expression leftExpr = criteria.getLeftExpression();

        Function leftFunction = (Function) leftExpr;
        if(isSimpleMathematicalFunction(leftFunction)) {
            return simplifyMathematicalCriteria(criteria);
        }
        if (FunctionLibrary.isConvert(leftFunction)) {
            return simplifyConvertFunction(criteria);
        }
        return simplifyParseFormatFunction(criteria);
    }

    private boolean isSimpleMathematicalFunction(Function function) {
        String funcName = function.getName();
        if(funcName.equals("+") || funcName.equals("-") || funcName.equals("*") || funcName.equals("/")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Expression[] args = function.getArgs();
            if(args[0] instanceof Constant || args[1] instanceof Constant) {
                return true;
            }
        }

        // fall through - not simple mathematical
        return false;
    }

    /**
     * @param criteria
     * @return CompareCriteria
     */
    private CompareCriteria simplifyMathematicalCriteria(CompareCriteria criteria)
    throws TeiidProcessingException{

        Expression leftExpr = criteria.getLeftExpression();
        Expression rightExpr = criteria.getRightExpression();

        // Identify all the pieces of this criteria
        Function function = (Function) leftExpr;
        String funcName = function.getName();
        Expression[] args = function.getArgs();
        Constant const1 = null;
        Expression expr = null;
        if(args[1] instanceof Constant) {
            const1 = (Constant) args[1];
            expr = args[0];
        } else {
            if(funcName.equals("+") || funcName.equals("*")) { //$NON-NLS-1$ //$NON-NLS-2$
                const1 = (Constant) args[0];
                expr = args[1];
            } else {
                // If we have "5 - x = 10" or "5 / x = 10", abort!
                return criteria;
            }
        }
        int operator = criteria.getOperator();

        // Determine opposite function
        String oppFunc = null;
        switch(funcName.charAt(0)) {
            case '+':   oppFunc = "-";  break; //$NON-NLS-1$
            case '-':   oppFunc = "+";  break; //$NON-NLS-1$
            case '*':   oppFunc = "/";  break; //$NON-NLS-1$
            case '/':   oppFunc = "*";  break; //$NON-NLS-1$
        }

        // Create a function of the two constants and evaluate it
        Expression combinedConst = null;
        FunctionLibrary funcLib = this.metadata.getFunctionLibrary();
        FunctionDescriptor descriptor = funcLib.findFunction(oppFunc, new Class[] { rightExpr.getType(), const1.getType() });
        if (descriptor == null){
            //See defect 9380 - this can be caused by const2 being a null Constant, for example (? + 1) < null
            return criteria;
        }


        if (rightExpr instanceof Constant) {
            Constant const2 = (Constant)rightExpr;
            try {
                Object result = descriptor.invokeFunction(new Object[] { const2.getValue(), const1.getValue() }, null, this.context );
                combinedConst = new Constant(result, descriptor.getReturnType());
            } catch(FunctionExecutionException e) {
                throw new QueryValidatorException(QueryPlugin.Event.TEIID30373, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30373, e.getMessage()));
            } catch (BlockedException e) {
                throw new QueryValidatorException(QueryPlugin.Event.TEIID30373, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30373, e.getMessage()));
            }
        } else {
            Function conversion = new Function(descriptor.getName(), new Expression[] { rightExpr, const1 });
            conversion.setType(leftExpr.getType());
            conversion.setFunctionDescriptor(descriptor);
            combinedConst = conversion;

        }

        // Flip operator if necessary
        if(! (operator == CompareCriteria.EQ || operator == CompareCriteria.NE) &&
             (oppFunc.equals("*") || oppFunc.equals("/")) ) { //$NON-NLS-1$ //$NON-NLS-2$

            Object value = const1.getValue();
            if(value != null) {
                Class type = const1.getType();
                Comparable comparisonObject = null;
                if(type.equals(DataTypeManager.DefaultDataClasses.INTEGER)) {
                    comparisonObject = INTEGER_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
                    comparisonObject = DOUBLE_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.FLOAT)) {
                    comparisonObject = FLOAT_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.LONG)) {
                    comparisonObject = LONG_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_INTEGER)) {
                    comparisonObject = BIG_INTEGER_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
                    comparisonObject = BIG_DECIMAL_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.SHORT)) {
                    comparisonObject = SHORT_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BYTE)) {
                    comparisonObject = BYTE_ZERO;
                } else {
                    // Unknown type
                    return criteria;
                }

                // If value is less than comparison object (which is zero),
                // then need to switch operator.
                if(comparisonObject.compareTo(value) > 0) {
                    switch(operator) {
                        case CompareCriteria.LE:    operator = CompareCriteria.GE;  break;
                        case CompareCriteria.LT:    operator = CompareCriteria.GT;  break;
                        case CompareCriteria.GE:    operator = CompareCriteria.LE;  break;
                        case CompareCriteria.GT:    operator = CompareCriteria.LT;  break;
                    }
                }
            }
        }

        criteria.setLeftExpression(expr);
        criteria.setRightExpression(combinedConst);
        criteria.setOperator(operator);

        // Return new simplified criteria
        return criteria;
    }

    /**
     * This method attempts to rewrite compare criteria of the form
     *
     * <code>convert(typedColumn, string) = '5'</code>
     *
     * into
     *
     * <code>typedColumn = convert('5', typeOfColumn)</code>
     * where 'typeOfColumn' is the type of 'typedColumn'
     *
     * if, for example, the type of the column is integer, than the above
     * can be pre-evaluated to
     *
     * <code>typedColumn = 5 </code>
     *
     * Right expression has already been checked to be a Constant, left expression to be
     * a function.  Function is known to be "convert" or "cast".
     *
     * @param crit CompareCriteria
     * @return same Criteria instance (possibly optimized)
     * @since 4.2
     */
    private Criteria simplifyConvertFunction(CompareCriteria crit) {
        Function leftFunction = (Function) crit.getLeftExpression();
        Expression leftExpr = leftFunction.getArgs()[0];

        if(!(crit.getRightExpression() instanceof Constant)
                //TODO: this can be relaxed for order preserving operations
                || !(crit.getOperator() == CompareCriteria.EQ || crit.getOperator() == CompareCriteria.NE)) {
            return crit;
        }

        Constant rightConstant = (Constant) crit.getRightExpression();

        String leftExprTypeName = DataTypeManager.getDataTypeName(leftExpr.getType());

        Constant result = ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(rightConstant.getType()), leftExprTypeName, rightConstant);
        if (result == null) {
            return getSimpliedCriteria(crit, leftExpr, crit.getOperator() != CompareCriteria.EQ, true);
        }
        Constant other = ResolverUtil.convertConstant(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()), result);
        if (other == null || rightConstant.compareTo(other) != 0) {
            return getSimpliedCriteria(crit, leftExpr, crit.getOperator() != CompareCriteria.EQ, true);
        }

        if (!DataTypeManager.isImplicitConversion(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()))) {
            return crit;
        }

        crit.setRightExpression(result);
        crit.setLeftExpression(leftExpr);

        return crit;
    }


    /**
     * This method attempts to rewrite set criteria of the form
     *
     * <code>convert(typedColumn, string) in  ('5', '6')</code>
     *
     * into
     *
     * <code>typedColumn in (convert('5', typeOfColumn), convert('6', typeOfColumn)) </code>
     * where 'typeOfColumn' is the type of 'typedColumn'
     *
     * if, for example, the type of the column is integer, than the above
     * can be pre-evaluated to
     *
     * <code>typedColumn in (5,6)  </code>
     *
     * Right expression has already been checked to be a Constant, left expression to be
     * a function.  Function is known to be "convert" or "cast".  The scope of this change
     * will be limited to the case where the left expression is attempting to convert to
     * 'string'.
     *
     * @param crit CompareCriteria
     * @return same Criteria instance (possibly optimized)
     * @throws QueryValidatorException
     * @since 4.2
     */
    private Criteria simplifyConvertFunction(SetCriteria crit) throws TeiidComponentException, TeiidProcessingException{
        Function leftFunction = (Function) crit.getExpression();
        Expression leftExpr = leftFunction.getArgs()[0];
        String leftExprTypeName = DataTypeManager.getDataTypeName(leftExpr.getType());

        Iterator i = crit.getValues().iterator();
        Collection newValues = new ArrayList(crit.getNumberOfValues());

        boolean convertedAll = true;
        boolean removedSome = false;
        while (i.hasNext()) {
            Object next = i.next();
            if (!(next instanceof Constant)) {
                convertedAll = false;
                continue;
            }

            Constant rightConstant = (Constant) next;

            Constant result = ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(rightConstant.getType()), leftExprTypeName, rightConstant);
            if (result != null) {
                Constant other = ResolverUtil.convertConstant(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()), result);
                if (other == null || ((Comparable)rightConstant.getValue()).compareTo(other.getValue()) != 0) {
                    result = null;
                }
            }

            if (result == null) {
                removedSome = true;
                i.remove();
            } else if (DataTypeManager.isImplicitConversion(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()))) {
                newValues.add(result);
            } else {
                convertedAll = false;
            }
        }

        if (!convertedAll) {
            if (!removedSome) {
                return crit; //just return as is
            }
        } else {
            crit.setExpression(leftExpr);
            crit.setValues(newValues);
        }
        return rewriteCriteria(crit);
    }

    private Criteria simplifyParseFormatFunction(CompareCriteria crit) {
        //TODO: this can be relaxed for order preserving operations
        if(!(crit.getOperator() == CompareCriteria.EQ || crit.getOperator() == CompareCriteria.NE)) {
            return crit;
        }
        boolean isFormat = false;
        Function leftFunction = (Function) crit.getLeftExpression();
        String funcName = leftFunction.getName();
        String inverseFunction = null;
        if(StringUtil.startsWithIgnoreCase(funcName, "parse")) { //$NON-NLS-1$
            String type = funcName.substring(5);
            if (!PARSE_FORMAT_TYPES.contains(type)) {
                return crit;
            }
            inverseFunction = "format" + type; //$NON-NLS-1$
        } else if(StringUtil.startsWithIgnoreCase(funcName, "format")) { //$NON-NLS-1$
            String type = funcName.substring(6);
            if (!PARSE_FORMAT_TYPES.contains(type)) {
                return crit;
            }
            inverseFunction = "parse" + type; //$NON-NLS-1$
            isFormat = true;
        } else {
            return crit;
        }
        Expression rightExpr = crit.getRightExpression();
        if (!(rightExpr instanceof Constant)) {
            return crit;
        }
        Expression leftExpr = leftFunction.getArgs()[0];
        Expression formatExpr = leftFunction.getArgs()[1];
        if(!(formatExpr instanceof Constant)) {
            return crit;
        }
        String format = (String)((Constant)formatExpr).getValue();
        FunctionLibrary funcLib = this.metadata.getFunctionLibrary();
        FunctionDescriptor descriptor = funcLib.findFunction(inverseFunction, new Class[] { rightExpr.getType(), formatExpr.getType() });
        if(descriptor == null){
            return crit;
        }
        Object value = ((Constant)rightExpr).getValue();
        try {
            Object result = descriptor.invokeFunction(new Object[] {context, ((Constant)rightExpr).getValue(), format}, null, this.context );
            result = leftFunction.getFunctionDescriptor().invokeFunction(new Object[] {context, result, format }, null, this.context );
            if (Constant.COMPARATOR.compare(value, result) != 0) {
                return getSimpliedCriteria(crit, leftExpr, crit.getOperator() != CompareCriteria.EQ, true);
            }
        } catch(FunctionExecutionException e) {
            //Not all numeric formats are invertable, so just return the criteria as it may still be valid
            return crit;
        } catch (BlockedException e) {
            return crit;
        }
        //parseFunctions are all potentially narrowing
        if (!isFormat) {
            return crit;
        }
        //TODO: if format is not lossy, then invert the function
        return crit;
    }

    /**
     * This method applies a similar simplification as the previous method for Case 1829.  This is conceptually
     * the same thing but done using the timestampCreate system function.
     *
     * TIMESTAMPCREATE(rpcolli_physical.RPCOLLI.Table_B.date_field, rpcolli_physical.RPCOLLI.Table_B.time_field)
     *    = {ts'1969-09-20 18:30:45.0'}
     *
     *  -------------
     *
     *   rpcolli_physical.RPCOLLI.Table_B.date_field = {d'1969-09-20'}
     *   AND
     *   rpcolli_physical.RPCOLLI.Table_B.time_field = {t'18:30:45'}
     *
     *
     * @param criteria Compare criteria
     * @return Simplified criteria, if possible
     */
    private Criteria simplifyTimestampMerge2(CompareCriteria criteria) {
        if(criteria.getOperator() != CompareCriteria.EQ) {
            return criteria;
        }

        Expression leftExpr = criteria.getLeftExpression();
        Expression rightExpr = criteria.getRightExpression();

        // Allow for concat and string literal to be on either side
        Function tsCreateFunction = null;
        Constant timestampConstant = null;
        if(leftExpr instanceof Function && rightExpr instanceof Constant) {
            tsCreateFunction = (Function) leftExpr;
            timestampConstant = (Constant) rightExpr;
        } else {
            return criteria;
        }

        // Verify data type of constant and that constant has a value
        if(! timestampConstant.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            return criteria;
        }

        // Verify function is timestampCreate function
        if(! (tsCreateFunction.getName().equalsIgnoreCase("timestampCreate"))) { //$NON-NLS-1$
            return criteria;
        }

        // Get timestamp literal and break into pieces
        Timestamp ts = (Timestamp) timestampConstant.getValue();
        String tsStr = ts.toString();
        Date date = Date.valueOf(tsStr.substring(0, 10));
        Time time = Time.valueOf(tsStr.substring(11, 19));

        // Get timestampCreate args
        Expression[] args = tsCreateFunction.getArgs();

        // Rebuild the function
        CompareCriteria dateCrit = new CompareCriteria(args[0], CompareCriteria.EQ, new Constant(date, DataTypeManager.DefaultDataClasses.DATE));
        CompareCriteria timeCrit = new CompareCriteria(args[1], CompareCriteria.EQ, new Constant(time, DataTypeManager.DefaultDataClasses.TIME));
        CompoundCriteria compCrit = new CompoundCriteria(CompoundCriteria.AND, dateCrit, timeCrit);
        return compCrit;
    }

   /**
    * This method also applies the same simplification for Case 1829.  This is conceptually
    * the same thing but done  using the timestampCreate system function.
    *
    * formatDate(rpcolli_physical.RPCOLLI.Table_B.date_field, 'yyyy-MM-dd')
    *    || formatTime(rpcolli_physical.RPCOLLI.Table_B.time_field, ' HH:mm:ss') = '1969-09-20 18:30:45'
    *
    *  -------------
    *
    *   rpcolli_physical.RPCOLLI.Table_B.date_field = {d'1969-09-20'}
    *   AND
    *   rpcolli_physical.RPCOLLI.Table_B.time_field = {t'18:30:45'}
    *
    *
    * @param criteria Compare criteria
    * @return Simplified criteria, if possible
    */

   private Criteria simplifyTimestampMerge(CompareCriteria criteria) {
       if(criteria.getOperator() != CompareCriteria.EQ) {
           return criteria;
       }

       Expression leftExpr = criteria.getLeftExpression();
       Expression rightExpr = criteria.getRightExpression();

       // Allow for concat and string literal to be on either side
       Function concatFunction = null;
       Constant timestampConstant = null;
       if(leftExpr instanceof Function && rightExpr instanceof Constant) {
           concatFunction = (Function) leftExpr;
           timestampConstant = (Constant) rightExpr;
       } else {
           return criteria;
       }

       // Verify data type of string constant and that constant has a value
       if(! timestampConstant.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
           return criteria;
       }

       // Verify function is concat function
       if(! (concatFunction.getName().equalsIgnoreCase("concat") || concatFunction.getName().equals("||"))) { //$NON-NLS-1$ //$NON-NLS-2$
           return criteria;
       }

       // Verify concat has formatdate and formattime functions
       Expression[] args = concatFunction.getArgs();
       if(! (args[0] instanceof Function && args[1] instanceof Function)) {
           return criteria;
       }
       Function formatDateFunction = (Function) args[0];
       Function formatTimeFunction = (Function) args[1];
       if(! (formatDateFunction.getName().equalsIgnoreCase("formatdate") && formatTimeFunction.getName().equalsIgnoreCase("formattime"))) { //$NON-NLS-1$ //$NON-NLS-2$
           return criteria;
       }

       // Verify format functions have constants
       if(! (formatDateFunction.getArgs()[1] instanceof Constant && formatTimeFunction.getArgs()[1] instanceof Constant)) {
           return criteria;
       }

       // Verify length of combined date/time constants == timestamp constant
       String dateFormat = (String) ((Constant)formatDateFunction.getArgs()[1]).getValue();
       String timeFormat = (String) ((Constant)formatTimeFunction.getArgs()[1]).getValue();
       String timestampValue = (String) timestampConstant.getValue();

       // Passed all the checks, so build the optimized version
       try {
           Timestamp ts = FunctionMethods.parseTimestamp(this.context, timestampValue, dateFormat + timeFormat);
           Constant dateConstant = new Constant(TimestampWithTimezone.createDate(ts));
           CompareCriteria dateCompare = new CompareCriteria(formatDateFunction.getArgs()[0], CompareCriteria.EQ, dateConstant);

           Constant timeConstant = new Constant(TimestampWithTimezone.createTime(ts));
           CompareCriteria timeCompare = new CompareCriteria(formatTimeFunction.getArgs()[0], CompareCriteria.EQ, timeConstant);

           CompoundCriteria compCrit = new CompoundCriteria(CompoundCriteria.AND, dateCompare, timeCompare);
           return compCrit;

       } catch(FunctionExecutionException e) {
           return criteria;
       }
    }

    private Criteria rewriteCriteria(MatchCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        criteria.setLeftExpression( rewriteExpressionDirect(criteria.getLeftExpression()));
        criteria.setRightExpression( rewriteExpressionDirect(criteria.getRightExpression()));

        if (isNull(criteria.getLeftExpression()) || isNull(criteria.getRightExpression())) {
            return UNKNOWN_CRITERIA;
        }

        Expression rightExpr = criteria.getRightExpression();
        if(rightExpr instanceof Constant && rightExpr.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
            Constant constant = (Constant) rightExpr;
            String value = (String) constant.getValue();

            if (criteria.getMode() != MatchMode.REGEX) {
                char escape = criteria.getEscapeChar();

                // Check whether escape char is unnecessary and remove it
                if(escape != MatchCriteria.NULL_ESCAPE_CHAR && value.indexOf(escape) < 0) {
                    criteria.setEscapeChar(MatchCriteria.NULL_ESCAPE_CHAR);
                }

                // if the value of this string constant is '%', then we know the crit will
                // always be true
                if ( value.equals( String.valueOf(MatchCriteria.WILDCARD_CHAR)) ) {
                    return getSimpliedCriteria(criteria, criteria.getLeftExpression(), !criteria.isNegated(), true);
                }

                if (criteria.getMode() == MatchMode.SIMILAR) {
                    //regex is more widely supported
                    criteria.setMode(MatchMode.REGEX);
                    criteria.setRightExpression(new Constant(Evaluator.SIMILAR_TO_REGEX.getPatternString(value, escape)));
                    criteria.setEscapeChar(MatchCriteria.NULL_ESCAPE_CHAR);
                } else if(DataTypeManager.DefaultDataClasses.STRING.equals(criteria.getLeftExpression().getType())
                        && value.indexOf(escape) < 0
                        && value.indexOf(MatchCriteria.MATCH_CHAR) < 0
                        && value.indexOf(MatchCriteria.WILDCARD_CHAR) < 0) {
                    // if both left and right expressions are strings, and the LIKE match characters ('*', '_') are not present
                    //  in the right expression, rewrite the criteria as EQUALs rather than LIKE
                    return rewriteCriteria(new CompareCriteria(criteria.getLeftExpression(), criteria.isNegated()?CompareCriteria.NE:CompareCriteria.EQ, criteria.getRightExpression()));
                }
            }
        }

        return criteria;
    }

    private Criteria getSimpliedCriteria(Criteria crit, Expression a, boolean outcome, boolean nullPossible) {
        if (nullPossible) {
            if (outcome) {
                if (processing) {
                    return crit;
                }
                IsNullCriteria inc = new IsNullCriteria(a);
                inc.setNegated(true);
                return inc;
            }
        } else if (outcome) {
            return TRUE_CRITERIA;
        }
        return FALSE_CRITERIA;
    }

    private boolean rewriteLeftExpression(AbstractSetCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        criteria.setExpression(rewriteExpressionDirect(criteria.getExpression()));

        if (isNull(criteria.getExpression())) {
            return true;
        }

        return false;
    }

    private Criteria rewriteCriteria(SetCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        if (criteria.isAllConstants() && criteria.getValues().size() > 1 && criteria.getExpression() instanceof ElementSymbol) {
            return criteria;
        }

        criteria.setExpression(rewriteExpressionDirect(criteria.getExpression()));

        if (rewriteLeftExpression(criteria) && !criteria.getValues().isEmpty()) {
            return UNKNOWN_CRITERIA;
        }

        Collection vals = criteria.getValues();

        LinkedHashSet newVals = new LinkedHashSet(vals.size());
        Iterator valIter = vals.iterator();
        boolean allConstants = true;
        boolean hasNull = false;
        while(valIter.hasNext()) {
            Expression value = rewriteExpressionDirect( (Expression) valIter.next());
            if (isNull(value)) {
                hasNull = true;
                if (!preserveUnknown) {
                    if (criteria.isNegated()) {
                        return FALSE_CRITERIA;
                    }
                    continue;
                }
            }
            allConstants &= value instanceof Constant;
            newVals.add(value);
        }

        int size = newVals.size();
        if (size == 1) {
            if (preserveUnknown && hasNull) {
                return UNKNOWN_CRITERIA;
            }
            Expression value = (Expression)newVals.iterator().next();
            return rewriteCriteria(new CompareCriteria(criteria.getExpression(), criteria.isNegated()?CompareCriteria.NE:CompareCriteria.EQ, value));
        }

        criteria.setValues(newVals);
        if (allConstants) {
            criteria.setAllConstants(true);
            if (!DataTypeManager.isHashable(criteria.getExpression().getType())) {
                criteria.setValues(new TreeSet(criteria.getValues()));
            }
        }

        if (size == 0) {
            if (hasNull) {
                return UNKNOWN_CRITERIA;
            }
            return criteria.isNegated()?TRUE_CRITERIA:FALSE_CRITERIA;
        }

        if(criteria.getExpression() instanceof Function ) {
            Function leftFunction = (Function)criteria.getExpression();
            if(FunctionLibrary.isConvert(leftFunction)) {
                return simplifyConvertFunction(criteria);
            }
        }

        return criteria;
    }

    private Criteria rewriteCriteria(IsNullCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        criteria.setExpression(rewriteExpressionDirect(criteria.getExpression()));
        return criteria;
    }

    public static Expression rewriteExpression(Expression expression, CommandContext context, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException{
        return rewriteExpression(expression, context, metadata, false);
    }

    public static Expression rewriteExpression(Expression expression, CommandContext context, QueryMetadataInterface metadata, boolean rewriteSubcommands) throws TeiidComponentException, TeiidProcessingException{
        QueryRewriter rewriter = new QueryRewriter(metadata, context);
        rewriter.rewriteSubcommands = rewriteSubcommands;
        return rewriter.rewriteExpressionDirect(expression);
    }

    private Expression rewriteExpressionDirect(Expression expression) throws TeiidComponentException, TeiidProcessingException{
        if (expression instanceof Constant) {
            return expression;
        }
        if (expression instanceof ElementSymbol) {
            ElementSymbol es = (ElementSymbol)expression;
            Class<?> type  = es.getType();
            if (!processing && es.isExternalReference()) {
                if (variables == null) {
                    return new Reference(es);
                }
                Expression value = variables.get(es);

                if (value == null) {
                    String grpName = es.getGroupSymbol().getName();
                    if (grpName.equals(ProcedureReservedWords.CHANGING)) {
                        Assertion.failed("Changing value should not be null"); //$NON-NLS-1$
                    }
                } else if (value instanceof Constant) {
                    if (value.getType() == type) {
                        return value;
                    }
                    try {
                        return new Constant(FunctionMethods.convert(context, ((Constant)value).getValue(), DataTypeManager.getDataTypeName(type)), es.getType());
                    } catch (FunctionExecutionException e) {
                         throw new QueryValidatorException(e);
                    }
                }
                return new Reference(es);
            }
            return expression;
        }
        boolean isBindEligible = true;
        if (expression instanceof AggregateSymbol) {
            expression = rewriteExpression((AggregateSymbol)expression);
        } else if(expression instanceof Function) {
            isBindEligible = !isConstantConvert(expression);
            expression = rewriteFunction((Function) expression);
        } else if (expression instanceof CaseExpression) {
            expression = rewriteCaseExpression((CaseExpression)expression);
        } else if (expression instanceof SearchedCaseExpression) {
            expression = rewriteCaseExpression((SearchedCaseExpression)expression);
        } else if (expression instanceof ScalarSubquery) {
            ScalarSubquery subquery = (ScalarSubquery)expression;
            if (subquery.shouldEvaluate() && processing) {
                return new Constant(evaluator.evaluate(subquery, null), subquery.getType());
            }
            rewriteSubqueryContainer(subquery, true);
            if (!RelationalNodeUtil.shouldExecute(subquery.getCommand(), false, true)) {
                return new Constant(null, subquery.getType());
            }
            if (subquery.getCommand().getProcessorPlan() == null) {
                addImplicitLimit(subquery, 2);
            }
            return expression;
        } else if (expression instanceof ExpressionSymbol) {
            expression = rewriteExpressionDirect(((ExpressionSymbol)expression).getExpression());
        } else if (expression instanceof Criteria) {
            expression = rewriteCriteria((Criteria)expression);
        } else if (expression instanceof XMLSerialize) {
            rewriteExpressions(expression);
            XMLSerialize serialize = (XMLSerialize)expression;
            if (isNull(serialize.getExpression())) {
                return new Constant(null, serialize.getType());
            }
            if (serialize.getDeclaration() == null && serialize.isDocument()) {
                if ((serialize.getVersion() != null && !serialize.getVersion().equals("1.0"))) { //$NON-NLS-1$
                    serialize.setDeclaration(true);
                } else if (serialize.getEncoding() != null) {
                    Charset encoding = Charset.forName(serialize.getEncoding());
                    if (!encoding.equals(Charset.forName("UTF-8")) && !encoding.equals(Charset.forName("UTF-16"))) { //$NON-NLS-1$ //$NON-NLS-2$
                        serialize.setDeclaration(true);
                    }
                }
            }
        } else if (expression instanceof XMLCast) {
            XMLCast cast = (XMLCast)expression;
            if (cast.getType() == DefaultDataClasses.XML) {
                XMLQuery xmlQuery = new XMLQuery();
                xmlQuery.setXquery("$i"); //$NON-NLS-1$
                xmlQuery.setPassing(Arrays.asList(new DerivedColumn("i", cast.getExpression()))); //$NON-NLS-1$
                xmlQuery.compileXqueryExpression();
                return xmlQuery;
            }
        } else if (expression instanceof Reference) {
            if (preEvaluation) {
                Reference ref = (Reference)expression;
                if (ref.isPositional()) {
                    return evaluate(expression, isBindEligible);
                }
            }
        } else {
            if (expression instanceof WindowFunction) {
                WindowFunction wf = (WindowFunction)expression;
                WindowFrame windowFrame = wf.getWindowSpecification().getWindowFrame();
                if (windowFrame != null) {
                    if (Integer.valueOf(0).equals(windowFrame.getStart().getBound())) {
                        windowFrame.setStart(new FrameBound(BoundMode.CURRENT_ROW));
                    }
                    if (windowFrame.getEnd() != null && Integer.valueOf(0).equals(windowFrame.getEnd().getBound())) {
                        windowFrame.setEnd(new FrameBound(BoundMode.CURRENT_ROW));
                    }
                    if (windowFrame.getMode() == FrameMode.RANGE
                            && (windowFrame.getEnd() == null || windowFrame.getEnd().getBoundMode() == BoundMode.CURRENT_ROW)
                            && windowFrame.getStart().getBound() == null && windowFrame.getStart().getBoundMode() == BoundMode.PRECEDING) {
                        //default window frame, just remove
                        wf.getWindowSpecification().setWindowFrame(null);
                    }
                    if (windowFrame.getEnd() == null || windowFrame.getEnd().getBoundMode() == BoundMode.CURRENT_ROW) {
                        //default window frame end, just remove
                        windowFrame.setEnd(null);
                    }
                }
            } else if (expression instanceof Array) {
                Array array = (Array)expression;
                boolean foundAny = false;
                for (Expression ex : array.getExpressions()) {
                    if(!isConstantConvert(ex)) {
                        foundAny = true;
                        break;
                    }
                }
                isBindEligible = foundAny;
            }
            rewriteExpressions(expression);
        }

        if(!processing) {
            if (!EvaluatableVisitor.isFullyEvaluatable(expression, true)) {
                return expression;
            }
        } else if (!(expression instanceof Reference) && !EvaluatableVisitor.isEvaluatable(expression, EvaluationLevel.PROCESSING)) {
            return expression;
        }

        return evaluate(expression, isBindEligible);
    }

    private Constant evaluate(Expression expression, boolean isBindEligible)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        Object value = null;
        if (expression instanceof Criteria) {
            value = evaluator.evaluateTVL((Criteria)expression, Collections.emptyList());
        } else {
            value = evaluator.evaluate(expression, Collections.emptyList());
        }
        if (value instanceof Constant) {
            return (Constant)value; //multi valued substitution
        }
        Constant result = new Constant(value, expression.getType());
        result.setBindEligible(isBindEligible);
        return result;
    }

    private boolean isConstantConvert(Expression ex) {
        if (ex instanceof Constant) {
            return true;
        }
        if (!(ex instanceof Function)) {
            return false;
        }
        Function f = (Function)ex;
        if (!FunctionLibrary.isConvert(f)) {
            return false;
        }
        return isConstantConvert(f.getArg(0));
    }

    private Expression rewriteExpression(AggregateSymbol expression) throws TeiidComponentException, TeiidProcessingException {
        if (expression.isBoolean()) {
            if (expression.getAggregateFunction() == Type.EVERY) {
                expression.setAggregateFunction(Type.MIN);
            } else {
                expression.setAggregateFunction(Type.MAX);
            }
        }
        if ((expression.getAggregateFunction() == Type.MAX || expression.getAggregateFunction() == Type.MIN || expression.getAggregateFunction() == Type.AVG)) {
            if (expression.getAggregateFunction() != Type.AVG && expression.isDistinct()) {
                expression.setDistinct(false);
            }
            if (rewriteAggs && expression.getArg(0) != null && EvaluatableVisitor.willBecomeConstant(expression.getArg(0))) {
                return expression.getArg(0);
            }
        }
        if (expression.isDistinct() && expression.getAggregateFunction() == Type.USER_DEFINED && expression.getFunctionDescriptor().getMethod().getAggregateAttributes().usesDistinctRows()) {
            expression.setDistinct(false);
        }
        Expression[] args = expression.getArgs();
        if (args.length == 1 && expression.getCondition() != null && !expression.respectsNulls()) {
            Expression cond = expression.getCondition();
            Expression ex = expression.getArg(0);
            if (!(cond instanceof Criteria)) {
                cond = new ExpressionCriteria(cond);
            }
            SearchedCaseExpression sce = new SearchedCaseExpression(Arrays.asList(cond), Arrays.asList(ex));
            sce.setType(ex.getType());
            expression.setCondition(null);
            expression.setArgs(new Expression[] {sce});
            args = expression.getArgs();
        }
        for (int i = 0; i < args.length; i++) {
            args[i] = rewriteExpressionDirect(expression.getArg(i));
        }
        return expression;
    }

    private static Map<String, Integer> FUNCTION_MAP = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);

    static {
        FUNCTION_MAP.put(FunctionLibrary.SPACE, 0);
        FUNCTION_MAP.put(FunctionLibrary.NULLIF, 2);
        FUNCTION_MAP.put(FunctionLibrary.COALESCE, 3);
        FUNCTION_MAP.put(FunctionLibrary.CONCAT2, 4);
        FUNCTION_MAP.put(FunctionLibrary.TIMESTAMPADD, 5);
        FUNCTION_MAP.put(FunctionLibrary.PARSEDATE, 6);
        FUNCTION_MAP.put(FunctionLibrary.PARSETIME, 7);
        FUNCTION_MAP.put(FunctionLibrary.FORMATDATE, 8);
        FUNCTION_MAP.put(FunctionLibrary.FORMATTIME, 9);
        FUNCTION_MAP.put(SourceSystemFunctions.TRIM, 10);
        FUNCTION_MAP.put(SourceSystemFunctions.SUBSTRING, 11);
    }

    private Expression rewriteFunction(Function function) throws TeiidComponentException, TeiidProcessingException{
        //rewrite alias functions
        String functionName = function.getName();
        String actualName =ALIASED_FUNCTIONS.get(functionName);
        FunctionLibrary funcLibrary = this.metadata.getFunctionLibrary();

        if (actualName != null) {
            function.setName(actualName);
            Expression[] args = function.getArgs();
            Class<?>[] types = new Class[args.length];
            for(int i=0; i<args.length; i++) {
                types[i] = args[i].getType();
            }
            FunctionDescriptor descriptor = funcLibrary.findFunction(actualName, types);
            function.setFunctionDescriptor(descriptor);
        }

        if(StringUtil.startsWithIgnoreCase(functionName, "parse")) { //$NON-NLS-1$
            String type = functionName.substring(5);
            if (PARSE_FORMAT_TYPES.contains(type) && Number.class.isAssignableFrom(function.getType()) && !type.equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
                Function result = new Function(SourceSystemFunctions.PARSEBIGDECIMAL, function.getArgs());
                FunctionDescriptor descriptor =
                    funcLibrary.findFunction(SourceSystemFunctions.PARSEBIGDECIMAL, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
                result.setFunctionDescriptor(descriptor);
                result.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
                return rewriteFunction(ResolverUtil.getConversion(result, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.getDataTypeName(function.getType()), false, metadata.getFunctionLibrary()));
            } else if ((DataTypeManager.DefaultDataTypes.DATE.equalsIgnoreCase(type)
                    || DataTypeManager.DefaultDataTypes.TIME.equalsIgnoreCase(type)) && function.getArg(1) instanceof Constant) {
                String format = "yyyy-MM-dd"; //$NON-NLS-1$
                int length = 10;
                if (DataTypeManager.DefaultDataTypes.TIME.equalsIgnoreCase(type)) {
                    format = "hh:mm:ss"; //$NON-NLS-1$
                    length = 8;
                }
                Constant c = (Constant) function.getArg(1);
                if (format.equals(c.getValue())) {
                    Expression arg = function.getArg(0);
                    if ((arg instanceof Function)
                            && FunctionLibrary.isConvert((Function)arg)
                            && java.util.Date.class.isAssignableFrom(((Function)arg).getArg(0).getType())) {
                        return rewriteExpressionDirect(ResolverUtil.getConversion(arg, DataTypeManager.DefaultDataTypes.STRING, type, false, metadata.getFunctionLibrary()));
                    }
                }
            }
        } else if(StringUtil.startsWithIgnoreCase(functionName, "format")) { //$NON-NLS-1$
            String type = functionName.substring(6);
            if (PARSE_FORMAT_TYPES.contains(type) && Number.class.isAssignableFrom(function.getArg(0).getType()) && !type.equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
                Function bigDecimalParam = ResolverUtil.getConversion(function.getArg(0), DataTypeManager.getDataTypeName(function.getArg(0).getType()), DataTypeManager.DefaultDataTypes.BIG_DECIMAL, false, metadata.getFunctionLibrary());
                Function result = new Function(SourceSystemFunctions.FORMATBIGDECIMAL, new Expression[] {bigDecimalParam, function.getArg(1)});
                FunctionDescriptor descriptor =
                    funcLibrary.findFunction(SourceSystemFunctions.FORMATBIGDECIMAL, new Class[] { DataTypeManager.DefaultDataClasses.BIG_DECIMAL, DataTypeManager.DefaultDataClasses.STRING });
                result.setFunctionDescriptor(descriptor);
                result.setType(DataTypeManager.DefaultDataClasses.STRING);
                return rewriteFunction(result);
            } else if ((DataTypeManager.DefaultDataTypes.DATE.equalsIgnoreCase(type)
                    || DataTypeManager.DefaultDataTypes.TIME.equalsIgnoreCase(type)) && function.getArg(1) instanceof Constant) {
                String format = "yyyy-MM-dd"; //$NON-NLS-1$
                if (DataTypeManager.DefaultDataTypes.TIME.equalsIgnoreCase(type)) {
                    format = "hh:mm:ss"; //$NON-NLS-1$
                }
                Constant c = (Constant) function.getArg(1);
                if (format.equals(c.getValue())) {
                    return rewriteExpressionDirect(ResolverUtil.getConversion(function.getArg(0), DataTypeManager.getDataTypeName(function.getArg(0).getType()), DataTypeManager.DefaultDataTypes.STRING, false, metadata.getFunctionLibrary()));
                }
            }
        }

        boolean omitNull = false;
        Integer code = FUNCTION_MAP.get(functionName);
        if (code != null) {
            switch (code) {
            case 0: { //space(x) => repeat(' ', x)
                Function result = new Function(SourceSystemFunctions.REPEAT,
                        new Expression[] {new Constant(" "), function.getArg(0)}); //$NON-NLS-1$
                //resolve the function
                FunctionDescriptor descriptor =
                    funcLibrary.findFunction(SourceSystemFunctions.REPEAT, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER});
                result.setFunctionDescriptor(descriptor);
                result.setType(DataTypeManager.DefaultDataClasses.STRING);
                function = result;
                break;
            }
            case 1: {
                // TEIID-4455
                break;
            }
            case 2: {  //rewrite nullif(a, b) => case when (a = b) then null else a
                List when = Arrays.asList(new Criteria[] {new CompareCriteria(function.getArg(0), CompareCriteria.EQ, function.getArg(1))});
                Constant nullConstant = new Constant(null, function.getType());
                List then = Arrays.asList(new Expression[] {nullConstant});
                SearchedCaseExpression caseExpr = new SearchedCaseExpression(when, then);
                caseExpr.setElseExpression(function.getArg(0));
                caseExpr.setType(function.getType());
                return rewriteExpressionDirect(caseExpr);
            }
            case 3: {
                Expression[] args = function.getArgs();
                if (args.length == 2) {
                    Function result = new Function(SourceSystemFunctions.IFNULL,
                            new Expression[] {function.getArg(0), function.getArg(1) });
                    //resolve the function
                    FunctionDescriptor descriptor =
                        funcLibrary.findFunction(SourceSystemFunctions.IFNULL, new Class[] { function.getType(), function.getType()  });
                    result.setFunctionDescriptor(descriptor);
                    result.setType(function.getType());
                    function = result;
                }
                break;
            }
            case 4:
                omitNull = true;
                break;
            case 5: {
                if (function.getType() != DataTypeManager.DefaultDataClasses.TIMESTAMP) {
                    FunctionDescriptor descriptor =
                        funcLibrary.findFunction(SourceSystemFunctions.TIMESTAMPADD, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.TIMESTAMP });
                    function.setFunctionDescriptor(descriptor);
                    Class<?> type = function.getType();
                    function.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
                    function.getArgs()[2] = ResolverUtil.getConversion(function.getArg(2), DataTypeManager.getDataTypeName(type), DataTypeManager.DefaultDataTypes.TIMESTAMP, false, funcLibrary);
                    function = ResolverUtil.getConversion(function, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.getDataTypeName(type), false, funcLibrary);
                }
                break;
            }
            case 6:
            case 7: {
                FunctionDescriptor descriptor =
                    funcLibrary.findFunction(SourceSystemFunctions.PARSETIMESTAMP, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
                function.setName(SourceSystemFunctions.PARSETIMESTAMP);
                function.setFunctionDescriptor(descriptor);
                Class<?> type = function.getType();
                function.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
                function = ResolverUtil.getConversion(function, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.getDataTypeName(type), false, funcLibrary);
                break;
            }
            case 8:
            case 9: {
                FunctionDescriptor descriptor =
                    funcLibrary.findFunction(SourceSystemFunctions.FORMATTIMESTAMP, new Class[] { DataTypeManager.DefaultDataClasses.TIMESTAMP, DataTypeManager.DefaultDataClasses.STRING });
                function.setName(SourceSystemFunctions.FORMATTIMESTAMP);
                function.setFunctionDescriptor(descriptor);
                function.getArgs()[0] = ResolverUtil.getConversion(function.getArg(0), DataTypeManager.getDataTypeName(function.getArg(0).getType()), DataTypeManager.DefaultDataTypes.TIMESTAMP, false, funcLibrary);
                break;
            }
            case 10: {
                if (new Constant(" ").equals(function.getArg(1))) { //$NON-NLS-1$
                    String spec = (String)((Constant)function.getArg(0)).getValue();
                    Expression string = function.getArg(2);
                    if (!SQLConstants.Reserved.TRAILING.equalsIgnoreCase(spec)) {
                        function = new Function(SourceSystemFunctions.LTRIM, new Expression[] {string});
                        FunctionDescriptor descriptor = funcLibrary.findFunction(SourceSystemFunctions.LTRIM, new Class[] { DataTypeManager.DefaultDataClasses.STRING });
                        function.setFunctionDescriptor(descriptor);
                        function.setType(DataTypeManager.DefaultDataClasses.STRING);
                        string = function;
                    }
                    if (!SQLConstants.Reserved.LEADING.equalsIgnoreCase(spec)) {
                        function = new Function(SourceSystemFunctions.RTRIM, new Expression[] {string});
                        FunctionDescriptor descriptor = funcLibrary.findFunction(SourceSystemFunctions.RTRIM, new Class[] { DataTypeManager.DefaultDataClasses.STRING });
                        function.setFunctionDescriptor(descriptor);
                        function.setType(DataTypeManager.DefaultDataClasses.STRING);
                    }
                }
                break;
            }
            case 11: {
                if (function.getArg(1) instanceof Constant) {
                    Constant c = (Constant)function.getArg(1);
                    if (!c.isMultiValued() && !c.isNull()) {
                        int val = (Integer) c.getValue();
                        if (val == 0) {
                            function.getArgs()[1] = new Constant(1);
                        }
                    }
                }
                break;
            }
            }
        }

        Expression[] args = function.getArgs();
        Expression[] newArgs = new Expression[args.length];

        // Rewrite args
        int j = 0;
        for(int i=0; i<args.length; i++) {
            Expression ex = rewriteExpressionDirect(args[i]);
            if (isNull(ex)) {
                if (!function.getFunctionDescriptor().isNullDependent()) {
                    return new Constant(null, function.getType());
                }
                if (omitNull) {
                    continue;
                }
            }
            newArgs[j++] = ex;
        }
        if (omitNull) {
            if (j==0) {
                return new Constant(null, function.getType());
            }
            if (j==1) {
                return newArgs[0];
            }
            if (j!=args.length) {
                newArgs = Arrays.copyOf(newArgs, j);
            }
        }
        function.setArgs(newArgs);

        if( FunctionLibrary.isConvert(function)) {
            Class<?> srcType = newArgs[0].getType();
            Class<?> tgtType = function.getType();

            if(srcType != null && tgtType != null && srcType.equals(tgtType)) {
                return newArgs[0]; //unnecessary conversion
            }

            if (function.isImplicit()) {
                function.setImplicit(false);
            }

            if (!(newArgs[0] instanceof Function) || tgtType == DataTypeManager.DefaultDataClasses.OBJECT) {
                return function;
            }
            Function nested = (Function) newArgs[0];
            if (!FunctionLibrary.isConvert(nested)) {
                return function;
            }
            Class<?> nestedType = nested.getArgs()[0].getType();

            Transform t = DataTypeManager.getTransform(nestedType, nested.getType());
            if (t.isExplicit()) {
                //explicit conversions are required
                return function;
            }
            if (DataTypeManager.getTransform(nestedType, tgtType) == null) {
                //no direct conversion exists
                return function;
            }
            //can't remove a convert that would alter the lexical form
            if (tgtType == DataTypeManager.DefaultDataClasses.STRING &&
                    (nestedType == DataTypeManager.DefaultDataClasses.BOOLEAN
                    || nestedType == DataTypeManager.DefaultDataClasses.DATE
                    || nestedType == DataTypeManager.DefaultDataClasses.TIME
                    || tgtType == DataTypeManager.DefaultDataClasses.BIG_DECIMAL
                    || tgtType == DataTypeManager.DefaultDataClasses.FLOAT
                    || (tgtType == DataTypeManager.DefaultDataClasses.DOUBLE && srcType != DataTypeManager.DefaultDataClasses.FLOAT))) {
                return function;
            }
            //nested implicit transform is not needed
            return rewriteExpressionDirect(ResolverUtil.getConversion(nested.getArgs()[0], DataTypeManager.getDataTypeName(nestedType), DataTypeManager.getDataTypeName(tgtType), false, funcLibrary));
        }

        //convert DECODESTRING function to CASE expression
        if( function.getName().equalsIgnoreCase(FunctionLibrary.DECODESTRING)
                || function.getName().equalsIgnoreCase(FunctionLibrary.DECODEINTEGER)) {
            return convertDecodeFunction(function);
        }

        return function;
    }

    private Expression convertDecodeFunction(Function function){
        Expression exprs[] = function.getArgs();
        String decodeString = (String)((Constant)exprs[1]).getValue();
        String decodeDelimiter = ","; //$NON-NLS-1$
        if(exprs.length == 3){
            decodeDelimiter = (String)((Constant)exprs[2]).getValue();
        }
        List<Criteria> newWhens = new ArrayList<Criteria>();
        List<Constant> newThens = new ArrayList<Constant>();
        Constant elseConst = null;
        StringTokenizer tokenizer = new StringTokenizer(decodeString, decodeDelimiter);
        while (tokenizer.hasMoreTokens()) {
            String resultString;
            String compareString =
                convertString(tokenizer.nextToken().trim());
            if (tokenizer.hasMoreTokens()) {
                resultString = convertString(tokenizer.nextToken().trim());
                Criteria crit;
                if (compareString == null) {
                    crit = new IsNullCriteria((Expression) exprs[0].clone());
                } else {
                    crit = new CompareCriteria((Expression) exprs[0].clone(), CompareCriteria.EQ, new Constant(compareString));
                }
                newWhens.add(crit);
                newThens.add(new Constant(resultString));
            }else {
                elseConst = new Constant(compareString);
            }
        }
        SearchedCaseExpression newCaseExpr = new SearchedCaseExpression(newWhens, newThens);
        if(elseConst != null) {
            newCaseExpr.setElseExpression(elseConst);
        }else {
            newCaseExpr.setElseExpression(exprs[0]);
        }

        newCaseExpr.setType(DefaultDataClasses.STRING);
        if (function.getName().equalsIgnoreCase(FunctionLibrary.DECODEINTEGER)) {
            return ResolverUtil.getConversion(newCaseExpr, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, false, metadata.getFunctionLibrary());
        }
        return newCaseExpr;
    }

    private static String convertString(String string) {
        /*
         * if there are no characters in the compare string we designate that as
         * an indication of null.  ie if the decode string looks like this:
         *
         * "'this', 1,,'null'"
         *
         * Then if the value in the first argument is null then the String 'null' is
         * returned from the function.
         */
        if (string.equals("")) { //$NON-NLS-1$
            return null;
        }

        /*
         * we also allow the use of the keyword null in the decode string.  if it
         * wished to match on the string 'null' then the string must be qualified by
         * ' designators.
         */
         if(string.equalsIgnoreCase("null")){ //$NON-NLS-1$
            return null;
         }

        /*
         * Here we check to see if the String in the decode String submitted
         * was surrounded by String literal characters. In this case we strip
         * these literal characters from the String.
         */
        if ((string.startsWith("\"") && string.endsWith("\"")) //$NON-NLS-1$ //$NON-NLS-2$
            || (string.startsWith("'") && string.endsWith("'"))) { //$NON-NLS-1$ //$NON-NLS-2$
            if (string.length() == 2) {
                /*
                 * This is an indication that the desired string to be compared is
                 * the "" empty string, so we return it as such.
                 */
                string = ""; //$NON-NLS-1$
            } else if (!string.equalsIgnoreCase("'") && !string.equalsIgnoreCase("\"")){ //$NON-NLS-1$ //$NON-NLS-2$
                string = string.substring(1);
                string = string.substring(0, string.length()-1);
            }
        }

        return string;
    }

    private Expression rewriteCaseExpression(CaseExpression expr)
        throws TeiidComponentException, TeiidProcessingException{
        List<CompareCriteria> whens = new ArrayList<CompareCriteria>(expr.getWhenCount());
        for (Expression expression: (List<Expression>)expr.getWhen()) {
            whens.add(new CompareCriteria((Expression)expr.getExpression().clone(), CompareCriteria.EQ, expression));
        }
        SearchedCaseExpression sce = new SearchedCaseExpression(whens, expr.getThen());
        sce.setElseExpression(expr.getElseExpression());
        sce.setType(expr.getType());
        return rewriteCaseExpression(sce);
    }

    private Expression rewriteCaseExpression(SearchedCaseExpression expr)
        throws TeiidComponentException, TeiidProcessingException{
        int whenCount = expr.getWhenCount();
        ArrayList<Criteria> whens = new ArrayList<Criteria>(whenCount);
        ArrayList<Expression> thens = new ArrayList<Expression>(whenCount);
        boolean hasTrue = false;
        for (int i = 0; i < whenCount; i++) {

            // Check the when to see if this CASE can be rewritten due to an always true/false when
            Criteria rewrittenWhen = rewriteCriteria(expr.getWhenCriteria(i));
            if (rewrittenWhen == FALSE_CRITERIA || rewrittenWhen == UNKNOWN_CRITERIA) {
                continue;
            }

            whens.add(rewrittenWhen);
            thens.add(rewriteExpressionDirect(expr.getThenExpression(i)));

            if(rewrittenWhen == TRUE_CRITERIA) {
                if (i == 0) {
                    // WHEN is always true, so just return the THEN
                    return rewriteExpressionDirect(expr.getThenExpression(i));
                }
                hasTrue = true;
                break;
            }
        }

        if (expr.getElseExpression() != null) {
            if (!hasTrue) {
                expr.setElseExpression(rewriteExpressionDirect(expr.getElseExpression()));
            } else {
                expr.setElseExpression(null);
            }
        }

        Expression elseExpr = expr.getElseExpression();
        if(whens.size() == 0) {
            // No WHENs left, so just return the ELSE
            if(elseExpr == null) {
                // No else, no valid whens, just return null constant typed same as CASE
                return new Constant(null, expr.getType());
            }

            // Rewrite the else and return
            return elseExpr;
        }

        expr.setWhen(whens, thens);

        /* optimization for case 5413:
         *   If all of the remaining 'thens' and the 'else' evaluate to the same value,
         *     just return the 'else' expression.
         */

        if ( elseExpr != null ) {
            boolean bAllMatch = true;

            for (int i = 0; i < whenCount; i++) {
                if ( !thens.get( i ).equals(elseExpr) ) {
                    bAllMatch = false;
                    break;
                }
            }

            if ( bAllMatch ) {
                return elseExpr;
            }
        }

        return expr;
    }

    private Command rewriteExec(StoredProcedure storedProcedure) throws TeiidComponentException, TeiidProcessingException{
        //After this method, no longer need to display named parameters
        storedProcedure.setDisplayNamedParameters(false);

        for (SPParameter param : storedProcedure.getInputParameters()) {
            if (!processing || storedProcedure.isPushedInQuery() || storedProcedure.isSupportsExpressionParameters()) {
                param.setExpression(rewriteExpressionDirect(param.getExpression()));
            } else if (!(param.getExpression() instanceof Constant)) {
                boolean isBindEligible = !isConstantConvert(param.getExpression());
                param.setExpression(evaluate(param.getExpression(), isBindEligible));
            }
        }
        return storedProcedure;
    }

    private Command rewriteInsert(Insert insert) throws TeiidComponentException, TeiidProcessingException{
        Command c = rewriteInsertForWriteThrough(insert);
        if (c != null) {
            return c;
        }
        UpdateInfo info = insert.getUpdateInfo();
        if (info != null && info.isInherentInsert()) {
            //TODO: update error messages
            UpdateMapping mapping = info.findInsertUpdateMapping(insert, true);
            if (mapping == null) {
                 throw new QueryValidatorException(QueryPlugin.Event.TEIID30375, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30375, insert.getVariables()));
            }
            Map<ElementSymbol, ElementSymbol> symbolMap = mapping.getUpdatableViewSymbols();
            List<ElementSymbol> mappedSymbols = new ArrayList<ElementSymbol>(insert.getVariables().size());
            for (ElementSymbol symbol : insert.getVariables()) {
                mappedSymbols.add(symbolMap.get(symbol));
            }
            insert.setVariables(mappedSymbols);
            insert.setGroup(mapping.getGroup().clone());
            insert.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(insert.getGroup(), metadata, Command.TYPE_INSERT, true));
            return rewriteInsert(insert);
        }

        if ( insert.getQueryExpression() != null ) {
            insert.setQueryExpression((QueryCommand)rewriteCommand(insert.getQueryExpression(), true));
            return correctDatatypes(insert);
        }
        // Evaluate any function / constant trees in the insert values
        List expressions = insert.getValues();
        List evalExpressions = new ArrayList(expressions.size());
        Iterator expIter = expressions.iterator();
        boolean preserveUnknownOld = preserveUnknown;
        preserveUnknown = true;
        while(expIter.hasNext()) {
            Expression exp = (Expression) expIter.next();
            if (processing && exp instanceof ExpressionSymbol) {
                //expression symbols that were created in the PlanToProcessesConverter
                evalExpressions.add( evaluate(exp, true));
            } else {
                evalExpressions.add( rewriteExpressionDirect( exp ));
            }
        }
        preserveUnknown = preserveUnknownOld;

        insert.setValues(evalExpressions);
        return insert;
    }

    private Command rewriteInsertForWriteThrough(Insert insert)
            throws TeiidComponentException, QueryMetadataException,
            QueryResolverException, TeiidProcessingException {
        if (processing
                || insert.hasTag(WRITE_THROUGH)
                || !metadata.hasMaterialization(insert.getGroup().getMetadataID())
                || !Boolean.valueOf(metadata.getExtensionProperty(insert.getGroup().getMetadataID(), MaterializationMetadataRepository.MATVIEW_WRITE_THROUGH, false))) {
            return null;
        }
        List<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);
        insertElmnts.removeAll(insert.getVariables());
        //TODO: what about the explicit null case
        if (InsertResolver.getAutoIncrementKey(insert.getGroup().getMetadataID(), insertElmnts, metadata) != null) {
            //TODO: this may be possible, but we aren't guaranteed that we'll determine the key from the view insert
            LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, "Write-trough insert is not possible as the primary key will be generated and was not specified"); //$NON-NLS-1$
            return null;
        }

        //create a block to save the insert values, then insert them into both the view and the materialization
        //it would be better to combine this with project into, but there are several paths we have to account for:
        //- upserts
        //- insert rewritten via inherent logic
        //- insert with a single set of values (non-deterministic requires creating a single value set)
        //- insert with a query expression
        //so instead we'll just do this in a single place for now
        Block block = new Block();
        block.setAtomic(true);
        Insert cloneInsert = (Insert)insert.clone();
        Insert values = new Insert();

        GroupSymbol temp = new GroupSymbol("#temp"); //$NON-NLS-1$
        if (context.getGroups().contains(temp.getName())) {
            temp = RulePlaceAccess.recontextSymbol(temp, context.getGroups());
            temp.setDefinition(null);
        }
        values.setGroup(temp);
        if (insert.getQueryExpression() != null) {
            values.setQueryExpression(insert.getQueryExpression());
        } else {
            values.setValues(insert.getValues());
        }
        for (ElementSymbol es : insert.getVariables()) {
            values.addVariable(new ElementSymbol(es.getShortName()));
        }
        block.addStatement(new CommandStatement(values));
        Query q = new Query();
        q.setSelect(new Select(Arrays.asList(new MultipleElementSymbol())));
        q.setFrom(new From(Arrays.asList(new UnaryFromClause(temp))));

        insert.setQueryExpression((QueryCommand) q.clone());
        insert.addTag(WRITE_THROUGH);
        block.addStatement(new CommandStatement(insert));
        ElementSymbol rowCount = new ElementSymbol(ProcedureReservedWords.ROWCOUNT);
        ElementSymbol val = new ElementSymbol("val"); //$NON-NLS-1$
        DeclareStatement ds = new DeclareStatement(val, DataTypeManager.DefaultDataTypes.INTEGER, rowCount);
        block.addStatement(ds);
        Object target = metadata.getMaterialization(insert.getGroup().getMetadataID());
        if (target != null) {
            GroupSymbol newGroup = new GroupSymbol(metadata.getFullName(target));
            newGroup.setMetadataID(target);
            List<ElementSymbol> newVars = new ArrayList<ElementSymbol>();
            for (ElementSymbol es : insert.getVariables()) {
                ElementSymbol newVariable = new ElementSymbol(es.getShortName(), newGroup.clone());
                newVars.add(newVariable);
            }
            cloneInsert.setVariables(newVars);
            cloneInsert.setGroup(newGroup);
            cloneInsert.setUpdateInfo(null);
            cloneInsert.getValues().clear();
            cloneInsert.setQueryExpression(q);
            block.addStatement(new CommandStatement(cloneInsert));
        } else {
            Object key = NewCalculateCostUtil.getKeyUsed(insert.getVariables(), Collections.singleton(insert.getGroup()), metadata, true);
            if (key == null || key != metadata.getPrimaryKey(insert.getGroup().getMetadataID())) {
                //we need a key for this logic to work
                return null;
            }
            Block b = new Block();
            LoopStatement loop = new LoopStatement(b, (Query)q.clone(), "x"); //$NON-NLS-1$
            StoredProcedure sp = new StoredProcedure();
            sp.setProcedureName("SYSAdmin.refreshMatViewRow"); //$NON-NLS-1$
            sp.setParameter(new SPParameter(1, new Constant(metadata.getFullName(insert.getGroup().getMetadataID()))));

            List<Object> ids = metadata.getElementIDsInKey(key);
            int index = 2;
            for (Object id : ids) {
                sp.setParameter(new SPParameter(index++, new ElementSymbol(metadata.getName(id))));
            }
            b.addStatement(new CommandStatement(sp));
            block.addStatement(loop);
        }
        Query result = new Query();
        result.setSelect(new Select(Arrays.asList(val)));
        block.addStatement(new CommandStatement(result));
        CreateProcedureCommand command = new CreateProcedureCommand(block);
        QueryResolver.resolveCommand(command, metadata);
        return rewriteCommand(command, false);
    }

    private Command rewriteForWriteThrough(ProcedureContainer update)
            throws TeiidComponentException, QueryMetadataException,
            QueryResolverException, TeiidProcessingException {
        if (processing
                || update.hasTag(WRITE_THROUGH)
                || !metadata.hasMaterialization(update.getGroup().getMetadataID())
                || !Boolean.valueOf(metadata.getExtensionProperty(update.getGroup().getMetadataID(), MaterializationMetadataRepository.MATVIEW_WRITE_THROUGH, false))) {
            return null;
        }

        //internal
        //update view ... where predicate - mark as write through
        //loop on (select pk from view where predicate) - mark as write through, it's ok that this will use cache
        //  refreshMatView ...
        //end

        //external
        //update view ... where predicate - mark as write through
        //update target ... where predicate - mark as write through

        Block block = new Block();
        block.setAtomic(true);
        ProcedureContainer clone = (ProcedureContainer)update.clone();

        GroupSymbol temp = new GroupSymbol("#temp"); //$NON-NLS-1$
        if (context.getGroups().contains(temp.getName())) {
            temp = RulePlaceAccess.recontextSymbol(temp, context.getGroups());
            temp.setDefinition(null);
        }
        Query q = new Query();
        q.setSelect(new Select(Arrays.asList(new MultipleElementSymbol())));
        q.setFrom(new From(Arrays.asList(new UnaryFromClause(temp))));

        update.addTag(WRITE_THROUGH);
        block.addStatement(new CommandStatement(update));
        ElementSymbol rowCount = new ElementSymbol(ProcedureReservedWords.ROWCOUNT);
        ElementSymbol val = new ElementSymbol("val"); //$NON-NLS-1$
        DeclareStatement ds = new DeclareStatement(val, DataTypeManager.DefaultDataTypes.INTEGER, rowCount);
        block.addStatement(ds);
        final Object gid = update.getGroup().getMetadataID();
        Object target = metadata.getMaterialization(gid);
        if (target != null) {
            final GroupSymbol newGroup = new GroupSymbol(metadata.getFullName(target));
            newGroup.setMetadataID(target);
            ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
                public Expression replaceExpression(Expression element) {
                    if (element instanceof ElementSymbol) {
                        ElementSymbol es = (ElementSymbol)element;
                        if (es.getGroupSymbol().getMetadataID() == gid) {
                            es.setGroupSymbol(newGroup);
                        }
                    }
                    return element;
                }
            };
            if (clone instanceof Update) {
                Update u = (Update)clone;
                for (SetClause sc : u.getChangeList().getClauses()) {
                    sc.setSymbol(new ElementSymbol(sc.getSymbol().getShortName(), newGroup.clone()));
                    PreOrPostOrderNavigator.doVisit(sc, emv, PreOrPostOrderNavigator.POST_ORDER);
                }
                u.setGroup(newGroup);
            } else {
                ((Delete)clone).setGroup(newGroup);
            }
            PreOrPostOrderNavigator.doVisit(((FilteredCommand)clone).getCriteria(), emv, PreOrPostOrderNavigator.POST_ORDER);
            clone.setUpdateInfo(null);
            clone.addTag(WRITE_THROUGH);
            block.addStatement(new CommandStatement(clone));
        } else {
            Object key = metadata.getPrimaryKey(update.getGroup().getMetadataID());
            if (key == null) {
                //we need a key for this logic to work
                return null;
            }
            Block b = new Block();
            List<Object> ids = metadata.getElementIDsInKey(key);
            Select select = new Select();
            for (Object id : ids) {
                select.addSymbol(new ElementSymbol(metadata.getName(id)));
            }
            if (clone instanceof Update) {
                Update u = (Update)clone;
                for (SetClause sc : u.getChangeList().getClauses()) {
                    if (ids.contains(sc.getSymbol().getMetadataID())) {
                        //corner case of an update manipulating the primary key
                        return null;
                    }
                }
            }
            Query keys = new Query();
            keys.setSelect(select);
            keys.setFrom(new From(Arrays.asList(new UnaryFromClause(update.getGroup()))));
            if (((FilteredCommand)clone).getCriteria() != null) {
                keys.setCriteria((Criteria) ((FilteredCommand)clone).getCriteria().clone());
            }
            LoopStatement loop = new LoopStatement(b, keys, "x"); //$NON-NLS-1$
            StoredProcedure sp = new StoredProcedure();
            sp.setProcedureName("SYSAdmin.refreshMatViewRow"); //$NON-NLS-1$
            sp.setParameter(new SPParameter(1, new Constant(metadata.getFullName(update.getGroup().getMetadataID()))));

            int index = 2;
            for (Object id : ids) {
                sp.setParameter(new SPParameter(index++, new ElementSymbol(metadata.getName(id))));
            }
            b.addStatement(new CommandStatement(sp));
            block.addStatement(loop);
        }
        Query result = new Query();
        result.setSelect(new Select(Arrays.asList(val)));
        block.addStatement(new CommandStatement(result));
        CreateProcedureCommand command = new CreateProcedureCommand(block);
        QueryResolver.resolveCommand(command, metadata);
        return rewriteCommand(command, false);
    }

    public static Command rewriteAsUpsertProcedure(Insert insert, QueryMetadataInterface metadata, CommandContext context)
            throws TeiidComponentException, QueryMetadataException,
            QueryValidatorException, QueryResolverException,
            TeiidProcessingException {
        QueryRewriter rewriter = new QueryRewriter(metadata, context);
        Collection<?> keys = metadata.getUniqueKeysInGroup(insert.getGroup().getMetadataID());
        if (keys.isEmpty()) {
            throw new QueryValidatorException(QueryPlugin.Event.TEIID31132, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31132, insert.getGroup()));
        }
        Object key = keys.iterator().next();
        Set<Object> keyCols = new LinkedHashSet<Object>(metadata.getElementIDsInKey(key));
        Insert newInsert = new Insert();
        newInsert.setGroup(insert.getGroup().clone());
        newInsert.setVariables(LanguageObject.Util.deepClone(insert.getVariables(), ElementSymbol.class));
        ArrayList<Expression> values = new ArrayList<Expression>();
        IfStatement ifStatement = new IfStatement();
        Query exists = new Query();
        exists.setSelect(new Select(Arrays.asList(new Constant(1))));
        exists.setFrom(new From(Arrays.asList(new UnaryFromClause(insert.getGroup().clone()))));
        ifStatement.setCondition(new ExistsCriteria(exists));
        Update update = new Update();
        update.setGroup(insert.getGroup().clone());
        SetClauseList setClauses = new SetClauseList();
        update.setChangeList(setClauses);
        List<Criteria> crits = new ArrayList<Criteria>();
        GroupSymbol varGroup = getVarGroup(insert);
        for (ElementSymbol es : insert.getVariables()) {
            ElementSymbol var = new ElementSymbol(es.getShortName(), varGroup.clone());
            values.add(var.clone());
            if (keyCols.contains(es.getMetadataID())) {
                CompareCriteria cc = new CompareCriteria(es.clone(), CompareCriteria.EQ, var.clone());
                crits.add(cc);
            } else {
                setClauses.addClause(new SetClause(es.clone(), var.clone()));
            }
        }
        newInsert.setValues(values);
        update.setCriteria((Criteria) Criteria.combineCriteria(crits).clone());
        exists.setCriteria((Criteria) Criteria.combineCriteria(crits).clone());
        ifStatement.setIfBlock(new Block(new CommandStatement(update)));
        ifStatement.setElseBlock(new Block(new CommandStatement(newInsert)));
        //construct the value query
        QueryCommand query = insert.getQueryExpression();
        if (query == null) {
            Query q = new Query();
            Select s = new Select();
            s.addSymbols(LanguageObject.Util.deepClone(insert.getValues(), Expression.class));
            q.setSelect(s);
            query = q;
        }
        query = createInlineViewQuery(new GroupSymbol("X"), query, metadata, insert.getVariables()); //$NON-NLS-1$
        return rewriter.asLoopProcedure(insert.getGroup(), query, ifStatement, varGroup, Command.TYPE_INSERT);
    }

    private static GroupSymbol getVarGroup(TargetedCommand cmd) {
        if (cmd.getGroup().getShortName().equalsIgnoreCase("X")) { //$NON-NLS-1$
            return new GroupSymbol("X1"); //$NON-NLS-1$
        }
        return new GroupSymbol("X"); //$NON-NLS-1$
    }

    public static Query createInlineViewQuery(GroupSymbol inlineGroup,
                                               Command nested,
                                               QueryMetadataInterface metadata,
                                               List<? extends Expression> actualSymbols) throws QueryMetadataException,
                                                                  QueryResolverException,
                                                                  TeiidComponentException {
        Query query = new Query();
        Select select = new Select();
        query.setSelect(select);
        From from = new From();
        from.addClause(new UnaryFromClause(inlineGroup));
        TempMetadataStore store = new TempMetadataStore();
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, store);
        if (nested instanceof QueryCommand) {
            Query firstProject = ((QueryCommand)nested).getProjectedQuery();
            makeSelectUnique(firstProject.getSelect(), false);
        }
        TempMetadataID gid = store.addTempGroup(inlineGroup.getName(), nested.getProjectedSymbols());
        inlineGroup.setMetadataID(gid);

        List<Class<?>> actualTypes = new ArrayList<Class<?>>(nested.getProjectedSymbols().size());
        for (Expression ses : actualSymbols) {
            actualTypes.add(ses.getType());
        }
        List<Expression> selectSymbols = SetQuery.getTypedProjectedSymbols(ResolverUtil.resolveElementsInGroup(inlineGroup, tma), actualTypes, tma);
        Iterator<? extends Expression> iter = actualSymbols.iterator();
        for (Expression ses : selectSymbols) {
            ses = (Expression)ses.clone();
            Expression actual = iter.next();
            if (!Symbol.getShortName(ses).equals(Symbol.getShortName(actual))) {
                if (ses instanceof AliasSymbol) {
                    ((AliasSymbol)ses).setShortName(Symbol.getShortName(actual));
                } else {
                    ses = new AliasSymbol(Symbol.getShortName(actual), ses);
                }
            }
            select.addSymbol(ses);
        }
        query.setFrom(from);
        QueryResolver.resolveCommand(query, tma);
        query.setOption(nested.getOption()!=null?(Option) nested.getOption().clone():null);
        from.getClauses().clear();
        SubqueryFromClause sqfc = new SubqueryFromClause(inlineGroup.getName());
        sqfc.setCommand(nested);
        sqfc.getGroupSymbol().setMetadataID(inlineGroup.getMetadataID());
        from.addClause(sqfc);
        //copy the metadata onto the new query so that temp metadata adapters will be used in later calls
        query.getTemporaryMetadata().getData().putAll(store.getData());
        return query;
    }

    public static void makeSelectUnique(Select select, boolean expressionSymbolsOnly) {

        select.setSymbols(select.getProjectedSymbols());

        List<Expression> symbols = select.getSymbols();

        HashSet<String> uniqueNames = new HashSet<String>();

        for(int i = 0; i < symbols.size(); i++) {

            Expression symbol = symbols.get(i);

            String baseName = Symbol.getShortName(symbol);
            String name = baseName;

            int exprID = 0;
            while (true) {
                if (uniqueNames.add(name)) {
                    break;
                }
                name = baseName + '_' + (exprID++);
            }

            if (expressionSymbolsOnly && !(symbol instanceof ExpressionSymbol)) {
                continue;
            }

            boolean hasAlias = false;
            // Strip alias if it exists
            if(symbol instanceof AliasSymbol) {
                symbol = ((AliasSymbol)symbol).getSymbol();
                hasAlias = true;
            }

            if (((symbol instanceof ExpressionSymbol) && !hasAlias) || !name.equalsIgnoreCase(baseName)) {
                symbols.set(i, new AliasSymbol(name, symbol));
            }
        }
    }

    private Command rewriteUpdate(Update update) throws TeiidComponentException, TeiidProcessingException{
        if (update.getGroup().getDefinition() != null) {
            removeAlias(update, update.getGroup());
        }
        Command c = rewriteForWriteThrough(update);
        if (c != null) {
            return c;
        }
        UpdateInfo info = update.getUpdateInfo();
        if (info != null && info.isInherentUpdate()) {
            if (!info.getUnionBranches().isEmpty()) {
                List<Command> batchedUpdates = new ArrayList<Command>(info.getUnionBranches().size() + 1);
                for (UpdateInfo branchInfo : info.getUnionBranches()) {
                    batchedUpdates.add(rewriteInherentUpdate((Update)update.clone(), branchInfo));
                }
                batchedUpdates.add(0, rewriteInherentUpdate(update, info));
                return new BatchedUpdateCommand(batchedUpdates, true);
            }
            return rewriteInherentUpdate(update, info);
        }

        boolean preserveUnknownOld = preserveUnknown;
        preserveUnknown = true;
        // Evaluate any function on the right side of set clauses
        for (SetClause entry : update.getChangeList().getClauses()) {
            entry.setValue(rewriteExpressionDirect(entry.getValue()));
        }
        preserveUnknown = preserveUnknownOld;

        // Rewrite criteria
        Criteria crit = update.getCriteria();
        if(crit != null) {
            preserveUnknown = false;
            update.setCriteria(rewriteCriteria(crit));
            preserveUnknown = preserveUnknownOld;
        }

        return update;
    }

    private Command rewriteInherentUpdate(Update update, UpdateInfo info)
            throws QueryValidatorException, QueryMetadataException,
            TeiidComponentException, QueryResolverException,
            TeiidProcessingException {
        UpdateMapping mapping = info.findUpdateMapping(update.getChangeList().getClauseMap().keySet(), false);
        if (mapping == null) {
             throw new QueryValidatorException(QueryPlugin.Event.TEIID30376, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30376, update.getChangeList().getClauseMap().keySet()));
        }
        Map<ElementSymbol, ElementSymbol> symbolMap = mapping.getUpdatableViewSymbols();
        if (info.isSimple()) {
            Collection<ElementSymbol> elements = getAllElementsUsed(update, update.getGroup());

            UpdateMapping fullMapping = info.findUpdateMapping(elements, false);
            if (fullMapping != null) {
                update.setGroup(mapping.getGroup().clone());
                for (SetClause clause : update.getChangeList().getClauses()) {
                    clause.setSymbol(symbolMap.get(clause.getSymbol()));
                }
                //TODO: properly handle correlated references
                DeepPostOrderNavigator.doVisit(update, new ExpressionMappingVisitor(symbolMap, true));
                if (info.getViewDefinition().getCriteria() != null) {
                    update.setCriteria(Criteria.combineCriteria(update.getCriteria(), (Criteria)info.getViewDefinition().getCriteria().clone()));
                }
                //resolve
                update.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(update.getGroup(), metadata, Command.TYPE_UPDATE, true));
                return rewriteUpdate(update);
            }
        }
        Query query = (Query)info.getViewDefinition().clone();
        query.setOrderBy(null);
        SymbolMap expressionMapping = SymbolMap.createSymbolMap(update.getGroup(), query.getProjectedSymbols(), metadata);
        SetClauseList setClauseList = (SetClauseList) update.getChangeList().clone();
        GroupSymbol varGroup = getVarGroup(update);
        ArrayList<Expression> selectSymbols = mapChangeList(setClauseList, symbolMap, varGroup);
        query.setSelect(new Select(selectSymbols));
        ExpressionMappingVisitor emv = new ExpressionMappingVisitor(expressionMapping.asMap(), true);
        PostOrderNavigator.doVisit(query.getSelect(), emv);

        Criteria crit = update.getCriteria();
        if (crit != null) {
            PostOrderNavigator.doVisit(crit, emv);
            query.setCriteria(Criteria.combineCriteria(query.getCriteria(), crit));
        }
        GroupSymbol group = mapping.getGroup();
        String correlationName = mapping.getCorrelatedName().getName();

        return createUpdateProcedure(update, query, group, correlationName, setClauseList, varGroup, null);
    }

    private ArrayList<Expression> mapChangeList(SetClauseList setClauses,
            Map<ElementSymbol, ElementSymbol> symbolMap, GroupSymbol varGroup) {
        ArrayList<Expression> selectSymbols = new ArrayList<Expression>(setClauses.getClauses().size());
        int i = 0;
        for (SetClause clause : setClauses.getClauses()) {
            Expression ex = clause.getValue();
            if (!EvaluatableVisitor.willBecomeConstant(ex)) {
                ex = mapExpression(varGroup, selectSymbols, i, ex);
                clause.setValue(ex);
            }
            if (symbolMap != null) {
                clause.setSymbol(symbolMap.get(clause.getSymbol()));
            }
            i++;
        }
        return selectSymbols;
    }

    private static Expression mapExpression(GroupSymbol varGroup,
            ArrayList<Expression> selectSymbols, int i, Expression ex) {
        String name = "s_" +i; //$NON-NLS-1$
        selectSymbols.add(new AliasSymbol(name, ex));
        ex = new ElementSymbol(name, varGroup.clone());
        return ex;
    }

    private Command createUpdateProcedure(Update update, Query query,
            GroupSymbol group, String correlationName, SetClauseList setClauseList, GroupSymbol varGroup, Criteria constraint)
            throws TeiidComponentException, QueryMetadataException,
            QueryResolverException, TeiidProcessingException {
        Update newUpdate = new Update();
        newUpdate.setConstraint(constraint);
        newUpdate.setChangeList(setClauseList);
        newUpdate.setGroup(group.clone());
        List<Criteria> pkCriteria = createPkCriteria(update.getGroup(), group, correlationName, query, varGroup);
        newUpdate.setCriteria(new CompoundCriteria(pkCriteria));
        return asLoopProcedure(update.getGroup(), query, newUpdate, varGroup, Command.TYPE_UPDATE);
    }

    /**
     * rewrite as loop on (query) as X begin newupdate; rows_updated = rows_updated + 1 end;
     * @param updateType
     */
    private Command asLoopProcedure(GroupSymbol group, QueryCommand query,
            ProcedureContainer newUpdate, GroupSymbol varGroup, int updateType) throws QueryResolverException,
            TeiidComponentException, TeiidProcessingException {
        return asLoopProcedure(group, query, new CommandStatement(newUpdate), varGroup, updateType);
    }

    private Command asLoopProcedure(GroupSymbol group, QueryCommand query,
            Statement s, GroupSymbol varGroup, int updateType) throws QueryResolverException,
            TeiidComponentException, TeiidProcessingException {
        Block b = new Block();
        b.addStatement(s);
        CreateProcedureCommand cupc = new CreateProcedureCommand();
        cupc.setUpdateType(updateType);
        Block parent = new Block();
        parent.setAtomic(true);
        ElementSymbol rowsUpdated = new ElementSymbol(ProcedureReservedWords.VARIABLES+Symbol.SEPARATOR+"ROWS_UPDATED"); //$NON-NLS-1$
        DeclareStatement ds = new DeclareStatement(rowsUpdated, DataTypeManager.DefaultDataTypes.INTEGER, new Constant(0));
        parent.addStatement(ds);
        //create an intermediate temp
        Insert insert = new Insert();
        insert.setGroup(new GroupSymbol("#changes")); //$NON-NLS-1$
        insert.setQueryExpression(query);
        parent.addStatement(new CommandStatement(insert));
        Query q = new Query();
        q.setSelect(new Select());
        q.getSelect().addSymbol(new MultipleElementSymbol());
        q.setFrom(new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("#changes"))))); //$NON-NLS-1$
        LoopStatement ls = new LoopStatement(b, q, varGroup.getName());
        parent.addStatement(ls);
        AssignmentStatement as = new AssignmentStatement();
        rowsUpdated.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        as.setVariable(rowsUpdated);
        as.setExpression(new Function("+", new Expression[] {rowsUpdated, new Constant(1)})); //$NON-NLS-1$
        b.addStatement(as);
        Query returnQuery = new Query();
        returnQuery.setSelect(new Select(Arrays.asList(rowsUpdated.clone())));
        parent.addStatement(new CommandStatement(returnQuery));
        cupc.setBlock(parent);
        cupc.setVirtualGroup(group);
        QueryResolver.resolveCommand(cupc, metadata);
        return rewrite(cupc, metadata, context);
    }

    private List<Criteria> createPkCriteria(GroupSymbol viewGroup, GroupSymbol group, String correlationName, Query query, GroupSymbol varGroup) throws TeiidComponentException, QueryMetadataException, QueryValidatorException {
        Object pk = metadata.getPrimaryKey(group.getMetadataID());
        if (pk == null) {
            Collection uniqueKeysInGroup = metadata.getUniqueKeysInGroup(group.getMetadataID());
            if (uniqueKeysInGroup.isEmpty()) {
                throw new QueryValidatorException(QueryPlugin.Event.TEIID31267, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31267, viewGroup, group));
            }
            pk = uniqueKeysInGroup.iterator().next();
        }
        int i = query.getSelect().getSymbols().size();
        List<Object> ids = metadata.getElementIDsInKey(pk);
        List<Criteria> pkCriteria = new ArrayList<Criteria>(ids.size());
        for (Object object : ids) {
            ElementSymbol es = new ElementSymbol(correlationName + Symbol.SEPARATOR + metadata.getName(object));
            query.getSelect().addSymbol(new AliasSymbol("s_" +i, es)); //$NON-NLS-1$
            es = new ElementSymbol(group.getName() + Symbol.SEPARATOR + metadata.getName(object));
            pkCriteria.add(new CompareCriteria(es, CompareCriteria.EQ, new ElementSymbol("s_" + i, varGroup.clone()))); //$NON-NLS-1$
            i++;
        }
        return pkCriteria;
    }

    private Command rewriteDelete(Delete delete) throws TeiidComponentException, TeiidProcessingException{
        if (delete.getGroup().getDefinition() != null) {
            removeAlias(delete, delete.getGroup());
        }
        Command c = rewriteForWriteThrough(delete);
        if (c != null) {
            return c;
        }
        UpdateInfo info = delete.getUpdateInfo();
        if (info != null && info.isInherentDelete()) {
            if (!info.getUnionBranches().isEmpty()) {
                List<Command> batchedUpdates = new ArrayList<Command>(info.getUnionBranches().size() + 1);
                for (UpdateInfo branchInfo : info.getUnionBranches()) {
                    batchedUpdates.add(rewriteInherentDelete((Delete)delete.clone(), branchInfo));
                }
                batchedUpdates.add(0, rewriteInherentDelete(delete, info));
                return new BatchedUpdateCommand(batchedUpdates, true);
            }
            return rewriteInherentDelete(delete, info);
        }
        // Rewrite criteria
        Criteria crit = delete.getCriteria();
        if(crit != null) {
            boolean preserveUnknownOld = preserveUnknown;
            preserveUnknown = false;
            delete.setCriteria(rewriteCriteria(crit));
            preserveUnknown = preserveUnknownOld;
        }

        return delete;
    }

    /**
     * For backwards compatibility we strip the alias from delete/update
     * @param command
     * @param group
     */
    private void removeAlias(ProcedureContainer command, GroupSymbol group) {
        AliasGenerator ag = new AliasGenerator(true);
        ag.setCorrelationGroups(Arrays.asList(group.getDefinition()));
        command.acceptVisitor(ag);
        final GroupSymbol clone = group.clone();
        DeepPostOrderNavigator.doVisit(command, new LanguageVisitor() {
            public void visit(GroupSymbol obj) {
                if (obj.equals(clone) && obj.getMetadataID() == group.getMetadataID()) {
                    obj.setName(obj.getDefinition());
                    obj.setDefinition(null);
                }
            }
        });
    }

    private Command rewriteInherentDelete(Delete delete, UpdateInfo info)
            throws QueryMetadataException, TeiidComponentException,
            QueryResolverException, TeiidProcessingException {
        UpdateMapping mapping = info.getDeleteTarget();
        if (info.isSimple()) {
            Collection<ElementSymbol> elements = getAllElementsUsed(delete, delete.getGroup());
            UpdateMapping fullMapping = info.findUpdateMapping(elements, false);
            if (fullMapping != null) {
                delete.setGroup(mapping.getGroup().clone());
                //TODO: properly handle correlated references
                DeepPostOrderNavigator.doVisit(delete, new ExpressionMappingVisitor(mapping.getUpdatableViewSymbols(), true));
                delete.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(delete.getGroup(), metadata, Command.TYPE_DELETE, true));
                if (info.getViewDefinition().getCriteria() != null) {
                    delete.setCriteria(Criteria.combineCriteria(delete.getCriteria(), (Criteria)info.getViewDefinition().getCriteria().clone()));
                }
                return rewriteDelete(delete);
            }
        }

        Query query = (Query)info.getViewDefinition().clone();
        query.setOrderBy(null);
        SymbolMap expressionMapping = SymbolMap.createSymbolMap(delete.getGroup(), query.getProjectedSymbols(), metadata);

        query.setSelect(new Select());
        ExpressionMappingVisitor emv = new ExpressionMappingVisitor(expressionMapping.asMap(), true);

        Criteria crit = delete.getCriteria();
        if (crit != null) {
            PostOrderNavigator.doVisit(crit, emv);
            query.setCriteria(Criteria.combineCriteria(query.getCriteria(), crit));
        }
        GroupSymbol group = mapping.getGroup();
        String correlationName = mapping.getCorrelatedName().getName();
        return createDeleteProcedure(delete, query, group, correlationName);
    }

    private Collection<ElementSymbol> getAllElementsUsed(Command cmd, GroupSymbol group) {
        Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(cmd, false, true);
        for (Iterator<ElementSymbol> iter = elements.iterator(); iter.hasNext();) {
            ElementSymbol es = iter.next();
            if (!EquivalenceUtil.areEqual(group, es.getGroupSymbol())) {
                iter.remove();
            }
        }
        return elements;
    }

    public static Command createDeleteProcedure(Delete delete, QueryMetadataInterface metadata, CommandContext context) throws QueryResolverException, QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        QueryRewriter rewriter = new QueryRewriter(metadata, context);
        Criteria crit = delete.getCriteria();
        Query query = new Query(new Select(), new From(Arrays.asList(new UnaryFromClause(delete.getGroup()))), crit, null, null);
        return rewriter.createDeleteProcedure(delete, query, delete.getGroup(), delete.getGroup().getName());
    }

    public static Command createUpdateProcedure(Update update, QueryMetadataInterface metadata, CommandContext context) throws QueryResolverException, QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        QueryRewriter rewriter = new QueryRewriter(metadata, context);
        Criteria crit = update.getCriteria();
        if (crit != null) {
            crit = (Criteria) crit.clone();
        }
        SetClauseList setClauseList = (SetClauseList) update.getChangeList().clone();
        GroupSymbol varGroup = getVarGroup(update);
        ArrayList<Expression> selectSymbols = rewriter.mapChangeList(setClauseList, null, varGroup);
        Criteria constraint = null;
        if (update.getConstraint() != null) {
            constraint = update.getConstraint();
            Map<ElementSymbol, Expression> map = null;
            Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(update.getConstraint(), true);
            Set<ElementSymbol> existing = setClauseList.getClauseMap().keySet();
            for (ElementSymbol es : elems) {
                if (existing.contains(es)) {
                    continue;
                }
                if (map == null) {
                    map = new HashMap<ElementSymbol, Expression>();
                }
                map.put(es, mapExpression(varGroup, selectSymbols, selectSymbols.size(), es));
            }
            if (map != null) {
                constraint = (Criteria)constraint.clone();
                ExpressionMappingVisitor.mapExpressions(constraint, map);
            }
        }
        Query query = new Query(new Select(selectSymbols), new From(Arrays.asList(new UnaryFromClause(update.getGroup()))), crit, null, null);
        return rewriter.createUpdateProcedure(update, query, update.getGroup(), update.getGroup().getName(), setClauseList, varGroup, constraint);
    }

    private Command createDeleteProcedure(Delete delete, Query query,
            GroupSymbol group, String correlationName)
            throws TeiidComponentException, QueryMetadataException,
            QueryResolverException, TeiidProcessingException {
        Delete newUpdate = new Delete();
        newUpdate.setGroup(group.clone());
        GroupSymbol varGroup = getVarGroup(delete);
        List<Criteria> pkCriteria = createPkCriteria(delete.getGroup(), group, correlationName, query, varGroup);
        newUpdate.setCriteria(new CompoundCriteria(pkCriteria));
        return asLoopProcedure(delete.getGroup(), query, newUpdate, varGroup, Command.TYPE_DELETE);
    }

    private Limit rewriteLimitClause(Limit limit) throws TeiidComponentException, TeiidProcessingException{
        if (limit.getOffset() != null) {
            if (!processing) {
                limit.setOffset(rewriteExpressionDirect(limit.getOffset()));
            } else {
                Constant c = evaluate(limit.getOffset(), false);
                limit.setOffset(c);
                ValidationVisitor.LIMIT_CONSTRAINT.validate(c.getValue());
            }
            if (ZERO_CONSTANT.equals(limit.getOffset())) {
                limit.setOffset(null);
            }
        }
        if (limit.getRowLimit() != null) {
            if (!processing) {
                limit.setRowLimit(rewriteExpressionDirect(limit.getRowLimit()));
            } else {
                Constant c = evaluate(limit.getRowLimit(), false);
                limit.setRowLimit(c);
                ValidationVisitor.LIMIT_CONSTRAINT.validate(c.getValue());
            }
        }
        return limit;
    }
}