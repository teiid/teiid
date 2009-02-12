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

package com.metamatrix.query.optimizer.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.mapping.xml.MappingNode;
import com.metamatrix.query.mapping.xml.MappingNodeLogger;
import com.metamatrix.query.mapping.xml.MappingRecursiveElement;
import com.metamatrix.query.mapping.xml.MappingSourceNode;
import com.metamatrix.query.mapping.xml.MappingVisitor;
import com.metamatrix.query.mapping.xml.Navigator;
import com.metamatrix.query.mapping.xml.ResultSetInfo;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.CommandPlanner;
import com.metamatrix.query.optimizer.CommandTreeNode;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.xml.Program;
import com.metamatrix.query.processor.xml.XMLPlan;
import com.metamatrix.query.processor.xml.XMLProcessorEnvironment;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SelectSymbol;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.LogConstants;

/**
 * <p> This prepares an {@link com.metamatrix.query.processor.xml.XMLPlan XMLPlan} from
 * a Mapping Document structure of {@link com.metamatrix.query.mapping.xml.MappingNode MappingNodes}.
 * </p>
 */
public final class XMLPlanner implements CommandPlanner{

	/**
	 * Default constructor.  Since this object is stateless the constructor
	 * has nothing to do.
	 */
	public XMLPlanner() {}

    /**
	 * @see com.metamatrix.query.optimizer.CommandPlanner#generateCanonical
	 */
	public void generateCanonical(CommandTreeNode rootNode, QueryMetadataInterface metadata, AnalysisRecord analysisRecord, CommandContext context)
	throws QueryPlannerException, MetaMatrixComponentException {
		//Nothing to do here
	}

	/**
	 * @see com.metamatrix.query.optimizer.CommandPlanner#optimize
	 */
	public ProcessorPlan optimize(CommandTreeNode node,IDGenerator idGenerator,QueryMetadataInterface metadata,CapabilitiesFinder capFinder,AnalysisRecord analysisRecord,CommandContext context)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        XMLPlannerEnvironment env = new XMLPlannerEnvironment(metadata);

        // Check for XML results form property in parent CommandTreeNode (if any)
        if (node.getParent() != null && node.getParent().getCommandType() == CommandTreeNode.TYPE_XQUERY_COMMAND){
            String xmlFormResults = (String)node.getParent().getProperty(XMLPlannerEnvironment.XML_FORM_RESULTS_PROPERTY);
            if (xmlFormResults != null){
                env.xmlFormResults = xmlFormResults;
            }
        }

		return XMLPlanner.preparePlan(node.getCommand(), metadata, analysisRecord, env, idGenerator, capFinder, context);
	}

    /**
     * This method takes in a Command object of the user's query and returns a XML plan
     * as a XMLNode object.
     * @param command The Command object for which query plan is to be returned
     * @param metadata The metadata needed for planning
     * @param planEnv XMLPlanner.XMLPlannerEnvironment object which holds various data
     * @return The XML plan returned as an XMLPlan object
     * @throws QueryPlannerException
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    static XMLPlan preparePlan(Command command, QueryMetadataInterface metadata, AnalysisRecord analysisRecord, XMLPlannerEnvironment planEnv, IDGenerator idGenerator, CapabilitiesFinder capFinder, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        Query xmlQuery = (Query) command;        
        boolean debug = analysisRecord.recordDebug();
        if(debug) {
            analysisRecord.println("============================================================================"); //$NON-NLS-1$
            analysisRecord.println("XML COMMAND: " + xmlQuery); //$NON-NLS-1$
        }

        // lookup mapping node for the user command
        Collection groups = GroupCollectorVisitor.getGroups(xmlQuery, true);
        GroupSymbol group = (GroupSymbol) groups.iterator().next();

        MappingDocument doc = (MappingDocument)metadata.getMappingNode(group.getMetadataID());
        doc = (MappingDocument)doc.clone();
        
        // make a copy of the document
        planEnv.mappingDoc = doc;
		planEnv.documentGroup = group;
        planEnv.xmlCommand = (Query) command;
        planEnv.analysisRecord = analysisRecord;
        planEnv.capFinder = capFinder;
        planEnv.idGenerator = idGenerator;
        planEnv.context = context;

        LogManager.logTrace(LogConstants.CTX_XML_PLANNER, new Object[]{"Mapping document tree", new MappingNodeLogger(planEnv.mappingDoc)}); //$NON-NLS-1$
        if(debug) {
            debugDocumentInfo("Start", planEnv); //$NON-NLS-1$
        }
                
		prePlan(planEnv, debug);

        // Generate program to create document
        Program programPlan = XMLPlanToProcessVisitor.planProgram(planEnv.mappingDoc, planEnv);
        
        // create plan from program and initialized environment
        XMLProcessorEnvironment env = planEnv.createProcessorEnvironment(programPlan);    
        env.setChildPlans(getChildPlans(doc));
        XMLPlan plan = new XMLPlan(env);

        if(debug) {
            analysisRecord.println(""); //$NON-NLS-1$
            analysisRecord.println(plan.toString());
            analysisRecord.println("============================================================================"); //$NON-NLS-1$
        }

        return plan;
	}

    private static Collection getChildPlans(MappingDocument doc) throws QueryMetadataException, MetaMatrixComponentException {
    	final List childPlans = new ArrayList();
    	MappingVisitor real = new MappingVisitor(){
    		public void visit(MappingSourceNode element) {
    			childPlans.add(element.getResultSetInfo().getPlan());
    		}
    	};
        try {
            MappingVisitor visitor = new Navigator(true, real);
            doc.acceptVisitor(visitor);
        } catch (MetaMatrixRuntimeException e) {
            if (e.getCause() instanceof QueryMetadataException) {
                throw (QueryMetadataException)e.getCause();
            }
            else if (e.getCause() instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException)e.getCause();
            }
            throw e;
        }
		return childPlans;
	}
    
    private static void debugDocumentInfo(String msgTag, XMLPlannerEnvironment planEnv) {
        planEnv.analysisRecord.println("\n"+msgTag+":============================================================================"); //$NON-NLS-1$ //$NON-NLS-2$
        planEnv.analysisRecord.println("MAPPING DOCUMENT:\n" + MappingNode.toStringNodeTree(planEnv.mappingDoc)); //$NON-NLS-1$
    }

     
    /**
     * Pre planning - steps that occur before planning of the XML Query plan.
     * @param root root of mapping document
     * @param rsCrits Map of String mapping class result set name to Criteria object
     * @param rsOrderBy Map of String mapping class result set name to OrderBy clause
     * @param planEnv an instance of XMLPlannerEnvironment for passing around information
     * @param metadata QueryMetadataInterface instance
     * @throws QueryPlannerException for any logical exception detected during planning
     * @throws QueryMetadataException if metadata encounters exception
     * @throws MetaMatrixComponentException unexpected exception
     */
	private static void prePlan(XMLPlannerEnvironment planEnv, boolean debug)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        
        //extract source nodes
        planEnv.mappingDoc = SourceNodeGenaratorVisitor.extractSourceNodes(planEnv.mappingDoc);
        
        if (debug) {
            debugDocumentInfo("After Source Node Generation", planEnv); //$NON-NLS-1$
        }
        
        //raise input set criteria if possible
        SourceNodePlannerVisitor.raiseInputSet(planEnv.mappingDoc, planEnv);
        
        //Place the orderbys 
        placeOrderBys(planEnv.xmlCommand.getOrderBy(), planEnv);
        
        //prepare any user-specified criteria in command
        Criteria crit = planEnv.xmlCommand.getCriteria();
        CriteriaPlanner.placeUserCriteria(crit, planEnv);

        //Plan the various relational result sets along with criteria and order by
        XMLQueryPlanner.prePlanQueries(planEnv.mappingDoc, planEnv);
        
        if (debug) {
            debugDocumentInfo("After Pre Plan Queries", planEnv); //$NON-NLS-1$
        }
        
		preMarkExcluded(planEnv.xmlCommand, planEnv.mappingDoc);

        if (debug) {
            debugDocumentInfo("After Mark Exclude", planEnv); //$NON-NLS-1$
        }
        
        //Remove excluded document subtrees
        removeExcluded(planEnv.mappingDoc);
        
        if (debug) {
            debugDocumentInfo("After Exclude", planEnv); //$NON-NLS-1$
        }
        
        // Autostage queries. try to auto-stage the planned queries
        // removal of this step should not affect overall processing
        XMLStagaingQueryPlanner.stageQueries(planEnv.mappingDoc, planEnv);
        
        //JoinSourceNodes.joinSourceNodes(planEnv.mappingDoc, planEnv);
        
        //Plan the various relational result sets
        XMLQueryPlanner.optimizeQueries(planEnv.mappingDoc, planEnv);
        
		//Handle nillable nodes
        planEnv.mappingDoc = HandleNillableVisitor.execute(planEnv.mappingDoc);
        
        //Resolve all the "elements" aginst the result sets
        NameInSourceResolverVisitor.resolveElements(planEnv.mappingDoc, planEnv);
        
        //Validate and resolve the criteria specified on the mapping nodes.
        ValidateMappedCriteriaVisitor.validateAndCollectCriteriaElements(planEnv.mappingDoc, planEnv);
	}
    
    static void removeExcluded(MappingNode node) {
        for (Iterator i = node.getChildren().iterator(); i.hasNext();) {
            MappingNode child = (MappingNode)i.next();
            if (!(node instanceof MappingRecursiveElement) && child.isExcluded()) {
                i.remove();
            } else {
                removeExcluded(child);
            }
        }
    }

    /**
     * Is selection is not "select *", then mark all those un used nodes as excluded. The way
     * we do this mark the nodes selected and their parents as selected, then we sweep to mark
     * everybody who is not marked "included" as excluded.
     * @param xmlCommand
     */
    static MappingDocument preMarkExcluded(Query xmlCommand, MappingDocument doc) {
        Select select = xmlCommand.getSelect();
        SelectSymbol firstSymbol = select.getSymbol(0);

        // 0. mark the nodes to be excluded
        if(firstSymbol instanceof AllSymbol) {
            return doc;
        }
        
        // Get all the valid nodes to be marked as included.
        Collection validElements = ElementCollectorVisitor.getElements(select, true);
        HashSet elements = new HashSet(validElements.size());
        for (final Iterator i = validElements.iterator(); i.hasNext();) {
            final ElementSymbol element = (ElementSymbol)i.next();
            elements.add(element.getCanonicalName());
        }
        
        // keep the nodes given mark the rest of the nodes to be elementated
        return MarkExcludeVisitor.markExcludedNodes(doc, elements);
     }    
        
   
	/**
	 * Get the result sets of all orderby's elements
	 * @param orderBy The fully resolved order by clause
	 * @param planEnv XMLPlannerEnvironment
	 * @param metadata QueryMetadataInterface
	 * @return Map The hash map of result sets and corresponding order by clauses
	 * @throws QueryPlannerException
	 * @throws QueryMetadataException
	 * @throws MetaMatrixComponentException
	 */
	private static void placeOrderBys(OrderBy orderBy, XMLPlannerEnvironment planEnv)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        //prepare fully resolved Order By elements
        
        if (orderBy == null) {
            return;
        }
        
        List elements = orderBy.getVariables();
		List types = orderBy.getTypes();

		for (int i = 0; i< elements.size(); i++) {
			ElementSymbol elemSymbol = (ElementSymbol) elements.get(i);
			
            String nodeName = planEnv.getGlobalMetadata().getFullName(elemSymbol.getMetadataID()).toUpperCase();
            MappingNode elementNode = MappingNode.findNode(planEnv.mappingDoc, nodeName); 

            // make sure that the name in source is defined for this node, so that it can be used
            // in the orderby. static nodes do not qualify for ordering.
            if (elementNode.getNameInSource() == null){
                Object[] params = new Object[] {elementNode, orderBy};
                String msg = QueryExecPlugin.Util.getString("XMLPlanner.The_XML_document_element_{0}_is_not_mapped_to_data_and_cannot_be_used_in_the_ORDER_BY_clause__{1}_1", params); //$NON-NLS-1$
                throw new QueryPlannerException(msg);
            }
            
            MappingSourceNode sourceNode = elementNode.getSourceNode();
            ResultSetInfo rs = sourceNode.getResultSetInfo();
            OrderBy by = rs.getOrderBy();
            if (by == null) {
                by = new OrderBy();
            }
            ElementSymbol mappedSymbol = (ElementSymbol)sourceNode.getSymbolMap().get(new ElementSymbol(elementNode.getNameInSource()));
            by.addVariable((ElementSymbol)mappedSymbol.clone(), ((Boolean)types.get(i)).booleanValue());
            rs.setOrderBy(by);
		}
	}
        
}