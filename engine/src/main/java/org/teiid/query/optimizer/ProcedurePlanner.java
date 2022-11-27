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

package org.teiid.query.optimizer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.util.Assertion;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.proc.*;
import org.teiid.query.processor.proc.CreateCursorResultSetInstruction.Mode;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.proc.*;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * <p> This prepares an {@link org.teiid.query.processor.proc.ProcedurePlan ProcedurePlan} from
 * a CreateUpdateProcedureCommand {@link org.teiid.query.sql.proc.CreateProcedureCommand CreateUpdateProcedureCommand}.
 *
 */
public final class ProcedurePlanner implements CommandPlanner {

    /**
     * <p>Produce a ProcessorPlan for the CreateUpdateProcedureCommand on the current node
     * of the CommandTreeNode, the procedure plan construction involves using the child
     * processor plans.
     * @param metadata source of metadata
     * @return ProcessorPlan This processorPlan is a <code>ProcedurePlan</code>
     * @throws QueryPlannerException indicating a problem in planning
     * @throws QueryMetadataException indicating an exception in accessing the metadata
     * @throws TeiidComponentException indicating an unexpected exception
     */
    public ProcessorPlan optimize(Command procCommand, IDGenerator idGenerator, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        boolean debug = analysisRecord.recordDebug();
        if(debug) {
            analysisRecord.println("\n####################################################"); //$NON-NLS-1$
            analysisRecord.println("PROCEDURE COMMAND: " + procCommand); //$NON-NLS-1$
        }

        CreateProcedureCommand cupc = Assertion.isInstanceOf(procCommand, CreateProcedureCommand.class, "Wrong command type"); //$NON-NLS-1$

        if(debug) {
            analysisRecord.println("OPTIMIZING SUB-COMMANDS: "); //$NON-NLS-1$
        }

        for (Command command : CommandCollectorVisitor.getCommands(procCommand)) {
            if (!(command instanceof DynamicCommand)) {
                command.setProcessorPlan(QueryOptimizer.optimizePlan(command, metadata, idGenerator, capFinder, analysisRecord, context));
            }
        }

        Block block = cupc.getBlock();

        Program programBlock = planBlock(cupc, block, metadata, debug, idGenerator, capFinder, analysisRecord, context);

        if(debug) {
            analysisRecord.println("\n####################################################"); //$NON-NLS-1$
        }

        // create plan from program and initialized environment
        ProcedurePlan plan = new ProcedurePlan(programBlock);
        plan.setMetadata(metadata);
        plan.setOutputElements(cupc.getProjectedSymbols());

        if(debug) {
            analysisRecord.println("####################################################"); //$NON-NLS-1$
            analysisRecord.println("PROCEDURE PLAN :"+plan); //$NON-NLS-1$
            analysisRecord.println("####################################################"); //$NON-NLS-1$
        }

        return plan;
    }

    /**
     * <p> Plan a {@link Block} object, recursively plan each statement in the given block and
     * add the resulting {@link ProgramInstruction} a new {@link Program} for the block.
     * @param block The <code>Block</code> to be planned
     * @param metadata Metadata used during planning
     * @param debug Boolean determining if procedure plan needs to be printed for debug purposes
     * @param analysisRecord
     * @return A Program resulting in the block planning
     * @throws QueryPlannerException if invalid statement is encountered in the block
     * @throws QueryMetadataException if there is an error accessing metadata
     * @throws TeiidComponentException if unexpected error occurs
     */
    private Program planBlock(CreateProcedureCommand parentProcCommand, Block block, QueryMetadataInterface metadata, boolean debug, IDGenerator idGenerator, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        // Generate program and add instructions
        // this program represents the block on the procedure
        // instruction in the program would correspond to statements in the block
        Program programBlock = new Program(block.isAtomic());
        programBlock.setLabel(block.getLabel());

        // plan each statement in the block
        planStatements(parentProcCommand, block.getStatements(), metadata, debug, idGenerator,
                capFinder, analysisRecord, context, programBlock);

        if (block.getExceptionGroup() != null) {
            programBlock.setExceptionGroup(block.getExceptionGroup());
            if (block.getExceptionStatements() != null) {
                Program exceptionBlock = new Program(false);
                planStatements(parentProcCommand, block.getExceptionStatements(), metadata, debug, idGenerator,
                        capFinder, analysisRecord, context, exceptionBlock);
                programBlock.setExceptionProgram(exceptionBlock);
            }
        }

        return programBlock;
    }

    private void planStatements(CreateProcedureCommand parentProcCommand,
            List<Statement> stmts, QueryMetadataInterface metadata, boolean debug,
            IDGenerator idGenerator, CapabilitiesFinder capFinder,
            AnalysisRecord analysisRecord, CommandContext context,
            Program programBlock) throws QueryPlannerException,
            QueryMetadataException, TeiidComponentException {
        for (Statement statement : stmts) {
            Object instruction = planStatement(parentProcCommand, statement, metadata, debug, idGenerator, capFinder, analysisRecord, context);
            if(instruction instanceof ProgramInstruction){
                programBlock.addInstruction((ProgramInstruction)instruction);
            }else{
                //an array of ProgramInstruction
                ProgramInstruction[] insts = (ProgramInstruction[])instruction;
                for(int i=0; i<insts.length; i++){
                    programBlock.addInstruction(insts[i]);
                }
            }
        }
    }

    /**
     * <p> Plan a {@link Statement} object, depending on the type of the statement construct the appropriate
     * {@link ProgramInstruction} return it to added to a {@link Program}. If the statement references a
     * <code>Command</code>, it looks up the child CommandTreeNodes to get appropriate child's ProcessrPlan
     * and uses it for constructing the necessary instruction.
     * @param statement The statement to be planned
     * @param metadata Metadata used during planning
     * @param debug Boolean determining if procedure plan needs to be printed for debug purposes
     * @param analysisRecord
     * @return An array containing index of the next child to be accessed and the ProgramInstruction resulting
     * in the statement planning
     * @throws QueryPlannerException if invalid statement is encountered
     * @throws QueryMetadataException if there is an error accessing metadata
     * @throws TeiidComponentException if unexpected error occurs
     */
    private Object planStatement(CreateProcedureCommand parentProcCommand, Statement statement, QueryMetadataInterface metadata, boolean debug, IDGenerator idGenerator, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        int stmtType = statement.getType();
        // object array containing updated child index and the process instruction
        //Object array[] = new Object[2];
        // program instr resulting in planning this statement
        Object instruction = null;
        switch(stmtType) {
            case Statement.TYPE_ASSIGNMENT:
            case Statement.TYPE_DECLARE:
            case Statement.TYPE_RETURN:
            {
                AssignmentStatement assignStmt = (AssignmentStatement)statement;

                if (stmtType == Statement.TYPE_RETURN && assignStmt.getVariable() == null) {
                    return new ReturnInstruction();
                }
                AssignmentInstruction assignInstr = new AssignmentInstruction();
                instruction = assignInstr;

                assignInstr.setVariable(assignStmt.getVariable());

                Expression asigExpr = assignStmt.getExpression();
                assignInstr.setExpression(asigExpr);
                if(debug) {
                    analysisRecord.println("\tASSIGNMENT\n" + statement); //$NON-NLS-1$
                }
                if (stmtType == Statement.TYPE_RETURN) {
                    ReturnInstruction ri = new ReturnInstruction();
                    instruction = new ProgramInstruction[] {assignInstr, ri};
                }
                break;
            }
            case Statement.TYPE_ERROR:
            {
                ErrorInstruction error = new ErrorInstruction();
                instruction = error;
                RaiseStatement res = (RaiseStatement)statement;

                Expression asigExpr = res.getExpression();
                error.setExpression(asigExpr);
                error.setWarning(res.isWarning());
                if(debug) {
                    analysisRecord.println("\tRAISE STATEMENT:\n" + statement); //$NON-NLS-1$
                }
                break;
            }
            case Statement.TYPE_COMMAND:
            {
                CommandStatement cmdStmt = (CommandStatement) statement;
                Command command = cmdStmt.getCommand();
                ProcessorPlan commandPlan = cmdStmt.getCommand().getProcessorPlan();

                if (command.getType() == Command.TYPE_DYNAMIC){
                    instruction = new ExecDynamicSqlInstruction(parentProcCommand,((DynamicCommand)command), metadata, idGenerator, capFinder, ((DynamicCommand)command).getIntoGroup() == null && cmdStmt.isReturnable() && parentProcCommand.returnsResultSet());
                }else{
                    CreateCursorResultSetInstruction cursor = new CreateCursorResultSetInstruction(null, commandPlan, getMode(parentProcCommand, cmdStmt, command));
                    if (cursor.getMode() == Mode.HOLD) {
                        for (GroupSymbol gs : GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, false)) {
                            if (gs.isTempTable() && metadata.getModelID(gs.getMetadataID()) == TempMetadataAdapter.TEMP_MODEL) {
                                cursor.setUsesLocalTemp(true);
                                break;
                            }
                        }

                    }
                    instruction = cursor;
                    //handle stored procedure calls
                    if (command.getType() == Command.TYPE_STORED_PROCEDURE) {
                        StoredProcedure sp = (StoredProcedure)command;
                        if (sp.isCallableStatement()) {
                            Map<ElementSymbol, ElementSymbol> assignments = new LinkedHashMap<ElementSymbol, ElementSymbol>();
                            for (SPParameter param : sp.getParameters()) {
                                if (param.getParameterType() == SPParameter.RESULT_SET
                                        || param.getParameterType() == SPParameter.IN) {
                                    continue;
                                }
                                Expression expr = param.getExpression();
                                if (expr instanceof Reference) {
                                    expr = ((Reference)expr).getExpression();
                                }
                                ElementSymbol symbol = null;
                                if (expr instanceof ElementSymbol) {
                                    symbol = (ElementSymbol)expr;
                                }
                                assignments.put(param.getParameterSymbol(), symbol);
                            }
                            ((CreateCursorResultSetInstruction)instruction).setProcAssignments(assignments);
                        }
                    }
                }

                if(debug) {
                    analysisRecord.println("\tCOMMAND STATEMENT:\n " + statement); //$NON-NLS-1$
                    analysisRecord.println("\t\tSTATEMENT COMMAND PROCESS PLAN:\n " + commandPlan); //$NON-NLS-1$
                }
                break;
            }
            case Statement.TYPE_IF:
            {
                IfStatement ifStmt = (IfStatement)statement;
                Program ifProgram = planBlock(parentProcCommand, ifStmt.getIfBlock(), metadata, debug, idGenerator, capFinder, analysisRecord, context);
                Program elseProgram = null;
                if(ifStmt.hasElseBlock()) {
                    elseProgram = planBlock(parentProcCommand, ifStmt.getElseBlock(), metadata, debug, idGenerator, capFinder, analysisRecord, context);
                }
                instruction = new IfInstruction(ifStmt.getCondition(), ifProgram, elseProgram);
                if(debug) {
                    analysisRecord.println("\tIF STATEMENT:\n" + statement); //$NON-NLS-1$
                }
                break;
            }
            case Statement.TYPE_BREAK:
            case Statement.TYPE_CONTINUE:
            case Statement.TYPE_LEAVE:
            {
                BranchingStatement bs = (BranchingStatement)statement;
                if(debug) {
                    analysisRecord.println("\t" + statement); //$NON-NLS-1$
                }
                instruction = new BranchingInstruction(bs);
                break;
            }
            case Statement.TYPE_LOOP:
            {
                LoopStatement loopStmt = (LoopStatement)statement;
                if(debug) {
                    analysisRecord.println("\tLOOP STATEMENT:\n" + statement); //$NON-NLS-1$
                }
                String rsName = loopStmt.getCursorName();

                ProcessorPlan commandPlan = loopStmt.getCommand().getProcessorPlan();

                Program loopProgram = planBlock(parentProcCommand, loopStmt.getBlock(), metadata, debug, idGenerator, capFinder, analysisRecord, context);
                instruction = new LoopInstruction(loopProgram, rsName, commandPlan, loopStmt.getLabel());
                break;
            }
            case Statement.TYPE_WHILE:
            {
                WhileStatement whileStmt = (WhileStatement)statement;
                Program whileProgram = planBlock(parentProcCommand, whileStmt.getBlock(), metadata, debug, idGenerator, capFinder, analysisRecord, context);
                if(debug) {
                    analysisRecord.println("\tWHILE STATEMENT:\n" + statement); //$NON-NLS-1$
                }
                instruction = new WhileInstruction(whileProgram, whileStmt.getCondition(), whileStmt.getLabel());
                break;
            }
            case Statement.TYPE_COMPOUND:
            {
                Block block = (Block)statement;
                instruction = new BlockInstruction(planBlock(parentProcCommand, block, metadata, debug, idGenerator, capFinder, analysisRecord, context));
                break;
            }
            default:
                 throw new AssertionError("Error while planning update procedure, unknown statement type encountered: " + statement); //$NON-NLS-1$
        }
        return instruction;
    }

    private Mode getMode(CreateProcedureCommand parentProcCommand,
            CommandStatement cmdStmt, Command command) {
        if (!command.returnsResultSet()&&!(command instanceof StoredProcedure)) {
            return Mode.UPDATE;
        }
        if (parentProcCommand.returnsResultSet()&&cmdStmt.isReturnable()&&cmdStmt.getCommand().returnsResultSet()) {
            return Mode.HOLD;
        }
        return Mode.NOHOLD;
    }

} // END CLASS
