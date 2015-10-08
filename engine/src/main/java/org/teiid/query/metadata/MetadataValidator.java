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
package org.teiid.query.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.report.ReportItem;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

public class MetadataValidator {
	
	private Map<String, Datatype> typeMap;
	private QueryParser parser;
	
	interface MetadataRule {
		void execute(VDBMetaData vdb, MetadataStore vdbStore, ValidatorReport report, MetadataValidator metadataValidator);
	}	
	
	public MetadataValidator(Map<String, Datatype> typeMap, QueryParser parser) {
		this.typeMap = typeMap;
		this.parser = parser;
	}
	
	public MetadataValidator() {
		this.typeMap = SystemMetadata.getInstance().getRuntimeTypeMap();
		this.parser = QueryParser.getQueryParser();
	}

	public ValidatorReport validate(VDBMetaData vdb, MetadataStore store) {
		ValidatorReport report = new ValidatorReport();
		if (store != null && !store.getSchemaList().isEmpty()) {
			new SourceModelArtifacts().execute(vdb, store, report, this);
			new CrossSchemaResolver().execute(vdb, store, report, this);
			new ResolveQueryPlans().execute(vdb, store, report, this);
			new MinimalMetadata().execute(vdb, store, report, this);
		}
		return report;
	}
	
	// At minimum the model must have table/view, procedure or function 
	static class MinimalMetadata implements MetadataRule {
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report, MetadataValidator metadataValidator) {
			for (Schema schema:store.getSchemaList()) {
				if (vdb.getImportedModels().contains(schema.getName())) {
					continue;
				}
				ModelMetaData model = vdb.getModel(schema.getName());
				
				if (schema.getTables().isEmpty() 
						&& schema.getProcedures().isEmpty()
						&& schema.getFunctions().isEmpty()) {
					metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31070, model.getName()));
				}
				
				for (Table t:schema.getTables().values()) {
					if (t.getColumns() == null || t.getColumns().size() == 0) {
						metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31071, t.getFullName()));
					}
					Set<String> names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
					validateConstraintNames(metadataValidator, report, model, t.getAllKeys(), names);
					validateConstraintNames(metadataValidator, report, model, t.getFunctionBasedIndexes(), names);
				}
				
				// procedure validation is handled in parsing routines.
				
				if (!schema.getFunctions().isEmpty()) {
			    	ActivityReport<ReportItem> funcReport = new ActivityReport<ReportItem>("Translator metadata load " + model.getName()); //$NON-NLS-1$
					FunctionMetadataValidator.validateFunctionMethods(schema.getFunctions().values(),report);
					if(report.hasItems()) {
						metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31073, funcReport));
					}						
				}
			}
		}

		private void validateConstraintNames(MetadataValidator metadataValidator, ValidatorReport report, ModelMetaData model, Collection<KeyRecord> keys, Set<String> names) {
			for (KeyRecord record : keys) {
				if (record.getName() == null) {
					continue;
				}
				if (!names.add(record.getName())) {
					metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31152, record.getFullName()));
				}
			}
		}
	}
	
	// do not allow foreign tables, source functions in view model and vice versa 
	static class SourceModelArtifacts implements MetadataRule {
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report, MetadataValidator metadataValidator) {
			for (Schema schema:store.getSchemaList()) {
				if (vdb.getImportedModels().contains(schema.getName())) {
					continue;
				}
				ModelMetaData model = vdb.getModel(schema.getName());
				for (Table t:schema.getTables().values()) {
					if (t.isPhysical() && !model.isSource()) {
						metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31075, t.getFullName(), model.getName()));
					}
				}
				
				Set<String> names = new HashSet<String>();
				for (Procedure p:schema.getProcedures().values()) {
					boolean hasReturn = false;
					names.clear();
					for (int i = 0; i < p.getParameters().size(); i++) {
						ProcedureParameter param = p.getParameters().get(i);
						if (param.isVarArg() && param != p.getParameters().get(p.getParameters().size() -1)) {
							//check that the rest of the parameters are optional
							//this accommodates variadic multi-source procedures
							//effective this and the resolving logic ensure that you can used named parameters for everything,
							//or call the vararg procedure as normal
							for (int j = i+1; j < p.getParameters().size(); j++) {
								ProcedureParameter param1 = p.getParameters().get(j);
								if ((param1.getType() == Type.In || param1.getType() == Type.InOut) 
										&& (param1.isVarArg() || (param1.getNullType() != NullType.Nullable && param1.getDefaultValue() == null))) {
									metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31112, p.getFullName()));
								}
							}
						}
						if (param.getType() == ProcedureParameter.Type.ReturnValue) {
							if (hasReturn) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31107, p.getFullName()));
							}
							hasReturn = true;
						} else if (p.isFunction() && param.getType() != ProcedureParameter.Type.In) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31165, p.getFullName(), param.getFullName()));
						}
						if (!names.add(param.getName())) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31106, p.getFullName(), param.getFullName()));
						}
					}
					if (!p.isVirtual() && !model.isSource()) {
						metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31077, p.getFullName(), model.getName()));
					}
					if (p.isFunction()) {
						if (!hasReturn) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31166, p.getFullName()));
						}
						if (p.isVirtual() && p.getQueryPlan() == null) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31167, p.getFullName()));
						}
					}
					
				}
				
				for (FunctionMethod func:schema.getFunctions().values()) {
					for (FunctionParameter param : func.getInputParameters()) {
						if (param.isVarArg() && param != func.getInputParameters().get(func.getInputParameterCount() -1)) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31112, func.getFullName()));
						}
					}
					if (func.getPushdown().equals(FunctionMethod.PushDown.MUST_PUSHDOWN) && !model.isSource()) {
						metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31078, func.getFullName(), model.getName()));
					}
				}
			}
		}
	}	
	
	// Resolves metadata query plans to make sure they are accurate
	static class ResolveQueryPlans implements MetadataRule {
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report, MetadataValidator metadataValidator) {
			QueryMetadataInterface metadata = vdb.getAttachment(QueryMetadataInterface.class);
	    	metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
			for (Schema schema:store.getSchemaList()) {
				if (vdb.getImportedModels().contains(schema.getName())) {
					continue;
				}
				ModelMetaData model = vdb.getModel(schema.getName());
				MetadataFactory mf = new MetadataFactory(vdb.getName(), vdb.getVersion(), metadataValidator.typeMap, model) {
					@Override
					protected void setUUID(AbstractMetadataRecord record) {
						if (count >= 0) {
							count = Integer.MIN_VALUE;
						}
						super.setUUID(record);
					}
				};
				mf.setBuiltinDataTypes(store.getDatatypes());
				for (AbstractMetadataRecord record : schema.getResolvingOrder()) {
					if (record instanceof Table) {
						Table t = (Table)record;
						// no need to verify the transformation of the xml mapping document, 
						// as this is very specific and designer already validates it.
						if (t.getTableType() == Table.Type.Document
								|| t.getTableType() == Table.Type.XmlMappingClass
								|| t.getTableType() == Table.Type.XmlStagingTable) {
							continue;
						}
						if (t.isVirtual() && t.getTableType() != Table.Type.TemporaryTable) {
							if (t.getSelectTransformation() == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31079, t.getFullName(), model.getName()));
							}
							else {
								metadataValidator.validate(vdb, model, t, report, metadata, mf);
							}
						}						
					} else if (record instanceof Procedure) {
						Procedure p = (Procedure)record;
						if (p.isVirtual()) {
							if (p.getQueryPlan() == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31081, p.getFullName(), model.getName()));
							}
							else {
								metadataValidator.validate(vdb, model, p, report, metadata, mf);
							}
						}						
					}
				}
			}
		}
	}	

	public void log(ValidatorReport report, ModelMetaData model, String msg) {
		log(report, model, Severity.ERROR, msg);
	}
	
	public void log(ValidatorReport report, ModelMetaData model, Severity severity, String msg) {
		model.addRuntimeMessage(severity, msg);
		int messageLevel = MessageLevel.WARNING;
		if (severity == Severity.ERROR) {
			report.handleValidationError(msg);
		} else {
			messageLevel = MessageLevel.INFO;
			report.handleValidationWarning(msg);
		}
		LogManager.log(messageLevel, LogConstants.CTX_QUERY_RESOLVER, msg);
	}
	
    private void validate(VDBMetaData vdb, ModelMetaData model, AbstractMetadataRecord record, ValidatorReport report, QueryMetadataInterface metadata, MetadataFactory mf) {
    	ValidatorReport resolverReport = null;
    	try {
    		if (record instanceof Procedure) {
    			Procedure p = (Procedure)record;
    			Command command = parser.parseProcedure(p.getQueryPlan(), false);
    			QueryResolver.resolveCommand(command, new GroupSymbol(p.getFullName()), Command.TYPE_STORED_PROCEDURE, metadata, false);
    			resolverReport =  Validator.validate(command, metadata);
    			determineDependencies(p, command);
    		} else if (record instanceof Table) {
    			Table t = (Table)record;
    			
    			GroupSymbol symbol = new GroupSymbol(t.getFullName());
    			ResolverUtil.resolveGroup(symbol, metadata);    			
    			String selectTransformation = t.getSelectTransformation();
				if (t.isVirtual()) {
    				QueryCommand command = (QueryCommand)QueryParser.getQueryParser().parseCommand(selectTransformation);
    				QueryResolver.resolveCommand(command, metadata);
    				resolverReport =  Validator.validate(command, metadata);
    				if (!resolverReport.hasItems() && (t.getColumns() == null || t.getColumns().isEmpty())) {
    					List<Expression> symbols = command.getProjectedSymbols();
    					for (Expression column:symbols) {
    						try {
								addColumn(Symbol.getShortName(column), column.getType(), t, mf);
							} catch (TranslatorException e) {
								log(report, model, e.getMessage());
							}
    					}
					}
    				if (t.getColumns() != null && !t.getColumns().isEmpty()) { 
	    				determineDependencies(t, command);
	    				if (t.getInsertPlan() != null && t.isInsertPlanEnabled()) {
		    				validateUpdatePlan(model, report, metadata, t, t.getInsertPlan(), Command.TYPE_INSERT);
	    				}
	    				if (t.getUpdatePlan() != null && t.isUpdatePlanEnabled()) {
	    					validateUpdatePlan(model, report, metadata, t, t.getUpdatePlan(), Command.TYPE_UPDATE);
	    				}
	    				if (t.getDeletePlan() != null && t.isDeletePlanEnabled()) {
	    					validateUpdatePlan(model, report, metadata, t, t.getDeletePlan(), Command.TYPE_DELETE);
	    				}
    				}
    			}
    			
    			boolean addCacheHint = false;
    			if (t.isMaterialized() && t.getMaterializedTable() == null) {
	    			List<KeyRecord> fbis = t.getFunctionBasedIndexes();
	    			List<GroupSymbol> groups = Arrays.asList(symbol);
					if (fbis != null && !fbis.isEmpty()) {
						for (KeyRecord fbi : fbis) {
	    					for (int j = 0; j < fbi.getColumns().size(); j++) {
	    						Column c = fbi.getColumns().get(j);
	    						if (c.getParent() != fbi) {
	    							continue;
	    						}
	    						String exprString = c.getNameInSource();
	    						try {
		    						Expression ex = QueryParser.getQueryParser().parseExpression(exprString);
									ResolverVisitor.resolveLanguageObject(ex, groups, metadata);
									if (!ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ex).isEmpty()) {
										log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31114, exprString, fbi.getFullName()));
									} 
									EvaluatableVisitor ev = new EvaluatableVisitor();
									PreOrPostOrderNavigator.doVisit(ex, ev, PreOrPostOrderNavigator.PRE_ORDER);
									if (ev.getDeterminismLevel().compareTo(Determinism.VDB_DETERMINISTIC) < 0) {
										log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31115, exprString, fbi.getFullName()));
									}
	    						} catch (QueryResolverException e) {
	    							log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31116, exprString, fbi.getFullName(), e.getMessage()));
	    						}
							}
						}
					}
    			}
    			else {
    				addCacheHint = true;
    			}
    			
    			// this seems to parse, resolve and validate.
    			QueryNode node = QueryResolver.resolveView(symbol, new QueryNode(selectTransformation), SQLConstants.Reserved.SELECT, metadata, true);
    			CacheHint cacheHint = node.getCommand().getCacheHint();
				Long ttl = -1L;
				if (cacheHint != null && cacheHint.getTtl() != null && addCacheHint && t.getProperty(MaterializationMetadataRepository.MATVIEW_TTL, false) == null) {
					ttl = cacheHint.getTtl();
					t.setProperty(MaterializationMetadataRepository.MATVIEW_TTL, String.valueOf(ttl));
				}
    		}
			processReport(model, record, report, resolverReport);
		} catch (TeiidException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getMessage()));
		}
    }

	public static void determineDependencies(AbstractMetadataRecord p, Command command) {
		Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, true);
		LinkedHashSet<AbstractMetadataRecord> values = new LinkedHashSet<AbstractMetadataRecord>();
		for (GroupSymbol group : groups) {
			Object mid = group.getMetadataID();
			if (mid instanceof TempMetadataAdapter) {
				mid = ((TempMetadataID)mid).getOriginalMetadataID();
			}
			if (mid instanceof AbstractMetadataRecord) {
				values.add((AbstractMetadataRecord)mid);
			}
		}
		Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(command, true, true);
		for (ElementSymbol elem : elems) {
			Object mid = elem.getMetadataID();
			if (mid instanceof TempMetadataAdapter) {
				mid = ((TempMetadataID)mid).getOriginalMetadataID();
			}
			if (mid instanceof AbstractMetadataRecord) {
				values.add((AbstractMetadataRecord)mid);
			}
		}
		p.setIncomingObjects(new ArrayList<AbstractMetadataRecord>(values));
	}

	private void validateUpdatePlan(ModelMetaData model,
			ValidatorReport report,
			QueryMetadataInterface metadata, 
			Table t, String plan, int type) throws QueryParserException, QueryResolverException,
			TeiidComponentException {
		Command command = QueryParser.getQueryParser().parseProcedure(plan, true);
		
		QueryResolver.resolveCommand(command, new GroupSymbol(t.getFullName()), type, metadata, false);
		
		//determineDependencies(t, command); -- these should be tracked against triggers
		ValidatorReport resolverReport = Validator.validate(command, metadata);
		processReport(model, t, report, resolverReport);
	}

	private void processReport(ModelMetaData model,
			AbstractMetadataRecord record, ValidatorReport report,
			ValidatorReport resolverReport) {
		if(resolverReport != null && resolverReport.hasItems()) {
			for (ValidatorFailure v:resolverReport.getItems()) {
				log(report, model, v.getStatus() == ValidatorFailure.Status.ERROR?Severity.ERROR:Severity.WARNING, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), v.getMessage()));
			}
		}
	}

	private Column addColumn(String name, Class<?> type, Table table, MetadataFactory mf) throws TranslatorException {
		if (type == null) {
			throw new TranslatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31086, name, table.getFullName()));
		}
		Column column = mf.addColumn(name, DataTypeManager.getDataTypeName(type), table);
		column.setUpdatable(table.supportsUpdate());
		return column;		
	}
	
	// this class resolves the artifacts that are dependent upon objects from other schemas
	// materialization sources, fk and data types (coming soon..)
	// ensures that even if cached metadata is used that we resolve to a single instance
	static class CrossSchemaResolver implements MetadataRule {
		
		private boolean keyMatches(List<String> names, KeyRecord record) {
			if (names.size() != record.getColumns().size()) {
				return false;
			}
			Set<String> keyNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
			for (Column c: record.getColumns()) {
				keyNames.add(c.getName());
			}
			for (int i = 0; i < names.size(); i++) {
				if (!keyNames.contains(names.get(i))) {
					return false;
				}
			}
			return true;
		}

		
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report, MetadataValidator metadataValidator) {
			for (Schema schema:store.getSchemaList()) {
				if (vdb.getImportedModels().contains(schema.getName())) {
					continue;
				}
				ModelMetaData model = vdb.getModel(schema.getName());

				for (Table t:schema.getTables().values()) {
					if (t.isVirtual()) {
						if (t.isMaterialized() && t.getMaterializedTable() != null) {
							String matTableName = t.getMaterializedTable().getFullName();
							int index = matTableName.indexOf(Table.NAME_DELIM_CHAR);
							if (index == -1) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31088, matTableName, t.getFullName()));
							}
							else {
								String schemaName = matTableName.substring(0, index);
								Schema matSchema = store.getSchema(schemaName);
								if (matSchema == null) {
									metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31089, schemaName, matTableName, t.getFullName()));
								}
								else {
									Table matTable = matSchema.getTable(matTableName.substring(index+1));
									if (matTable == null) {
										metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31090, matTableName.substring(index+1), schemaName, t.getFullName()));
									}
									else {
										t.setMaterializedTable(matTable);
									}
								}
							}
						}
					}
					
					for (KeyRecord record : t.getAllKeys()) {
						if (record.getColumns() == null || record.getColumns().isEmpty()) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31149, t.getFullName(), record.getName()));
						}
					}
						
					List<ForeignKey> fks = t.getForeignKeys();
					if (fks == null || fks.isEmpty()) {
						continue;
					}
					
					for (ForeignKey fk:fks) {
						String referenceTableName = fk.getReferenceTableName();
						
						Table referenceTable = null;
						if (fk.getReferenceKey() == null) {
							if (referenceTableName == null){
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31091, t.getFullName()));
								continue;
							}
							//TODO there is an ambiguity here because we don't properly track the name parts
							//so we have to first check for a table name that may contain .
							referenceTable = schema.getTable(referenceTableName);
						} else {
							referenceTableName = fk.getReferenceKey().getParent().getFullName();
						}
						
						String referenceSchemaName = schema.getName();
						int index = referenceTableName.indexOf(Table.NAME_DELIM_CHAR);
						if (referenceTable == null) {
							if (index != -1) {
								referenceSchemaName = referenceTableName.substring(0, index);
								Schema referenceSchema = store.getSchema(referenceSchemaName);
								if (referenceSchema == null) {
									metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31093, referenceSchemaName, t.getFullName()));
									continue;
								}
								referenceTable = referenceSchema.getTable(referenceTableName.substring(index+1));
							}
							if (referenceTable == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31092, t.getFullName(), referenceTableName.substring(index+1), referenceSchemaName));
								continue;
							}
						}

						KeyRecord uniqueKey = null;
						List<String> referenceColumns = fk.getReferenceColumns();
						if (fk.getReferenceKey() != null) {
							//index metadata logic sets the key prior to having the column names
							List<Column> cols = fk.getReferenceKey().getColumns();
							referenceColumns = new ArrayList<String>();
							for (Column col : cols) {
								referenceColumns.add(col.getName());
							}
						}
						if (referenceColumns == null || referenceColumns.isEmpty()) {
							if (referenceTable.getPrimaryKey() == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31094, t.getFullName(), referenceTableName.substring(index+1), referenceSchemaName));
							}
							else {
								uniqueKey = referenceTable.getPrimaryKey();										
							}
						} else {
							for (KeyRecord record : referenceTable.getUniqueKeys()) {
								if (keyMatches(referenceColumns, record)) {
									uniqueKey = record;
									break;
								}
							}
							if (uniqueKey == null && referenceTable.getPrimaryKey() != null && keyMatches(referenceColumns, referenceTable.getPrimaryKey())) {
								uniqueKey = referenceTable.getPrimaryKey();
							}
						}
						if (uniqueKey == null) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31095, t.getFullName(), referenceTableName.substring(index+1), referenceSchemaName, referenceColumns));
						}
						else {
							fk.setReferenceKey(uniqueKey);
						}
					}
				}						
			}
			
		}
	}
	

}
