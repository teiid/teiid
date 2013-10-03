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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.*;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.report.ReportItem;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

public class MetadataValidator {
	
	private Map<String, Datatype> typeMap;
	
	interface MetadataRule {
		void execute(VDBMetaData vdb, MetadataStore vdbStore, ValidatorReport report, MetadataValidator metadataValidator);
	}	
	
	public MetadataValidator(Map<String, Datatype> typeMap) {
		this.typeMap = typeMap;
	}
	
	public MetadataValidator() {
		this.typeMap = SystemMetadata.getInstance().getRuntimeTypeMap();
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
					for (ProcedureParameter param : p.getParameters()) {
						if (param.isVarArg() && param != p.getParameters().get(p.getParameters().size() -1)) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31112, p.getFullName()));
						}
						if (param.getType() == ProcedureParameter.Type.ReturnValue) {
							if (hasReturn) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31107, p.getFullName()));
							}
							hasReturn = true;
						}
						if (!names.add(param.getName())) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31106, p.getFullName(), param.getFullName()));
						}
					}
					if (!p.isVirtual() && !model.isSource()) {
						metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31077, p.getFullName(), model.getName()));
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
						if (t.isVirtual()) {
							if (t.getSelectTransformation() == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31079, t.getFullName(), model.getName()));
							}
							else {
								metadataValidator.validate(vdb, model, t, report, metadata, mf);
							}
						}						
					} else if (record instanceof Procedure) {
						Procedure p = (Procedure)record;
						if (p.isVirtual() && !p.isFunction()) {
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
    			Command command = QueryParser.getQueryParser().parseProcedure(p.getQueryPlan(), false);
    			QueryResolver.resolveCommand(command, new GroupSymbol(p.getFullName()), Command.TYPE_STORED_PROCEDURE, metadata, false);
    			resolverReport =  Validator.validate(command, metadata);
    		} else if (record instanceof Table) {
    			Table t = (Table)record;
    			
    			GroupSymbol symbol = new GroupSymbol(t.getFullName());
    			ResolverUtil.resolveGroup(symbol, metadata);
    			if (t.isVirtual() && (t.getColumns() == null || t.getColumns().isEmpty())) {
    				QueryCommand command = (QueryCommand)QueryParser.getQueryParser().parseCommand(t.getSelectTransformation());
    				QueryResolver.resolveCommand(command, metadata);
    				resolverReport =  Validator.validate(command, metadata);
    				if(!resolverReport.hasItems()) {
    					List<Expression> symbols = command.getProjectedSymbols();
    					for (Expression column:symbols) {
    						try {
								addColumn(Symbol.getShortName(column), column.getType(), t, mf);
							} catch (TranslatorException e) {
								log(report, model, e.getMessage());
							}
    					}
    				}
    			}
    			
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
    			
    			// this seems to parse, resolve and validate.
    			QueryResolver.resolveView(symbol, new QueryNode(t.getSelectTransformation()), SQLConstants.Reserved.SELECT, metadata);
    		}
			if(resolverReport != null && resolverReport.hasItems()) {
				for (ValidatorFailure v:resolverReport.getItems()) {
					log(report, model, v.getStatus() == ValidatorFailure.Status.ERROR?Severity.ERROR:Severity.WARNING, v.getMessage());
				}
			}
		} catch (TeiidException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getFullMessage()));
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
						if (t.isMaterialized() && t.getMaterializedTable() != null && t.getMaterializedTable().getParent() == null) {
							String matTableName = t.getMaterializedTable().getName();
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
						
					List<ForeignKey> fks = t.getForeignKeys();
					if (fks == null || fks.isEmpty()) {
						continue;
					}
					
					for (ForeignKey fk:fks) {
						if (fk.getPrimaryKey() != null) {
							continue;
						}
						
						String referenceTableName = fk.getReferenceTableName();
						if (referenceTableName == null){
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31091, t.getFullName()));
							continue;
						}
						
						Table referenceTable = null;
						String referenceSchemaName = schema.getName(); 
						int index = referenceTableName.indexOf(Table.NAME_DELIM_CHAR);
						if (index == -1) {
							referenceTable = schema.getTable(referenceTableName);
						}
						else {
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

						KeyRecord uniqueKey = null;
						if (fk.getReferenceColumns() == null || fk.getReferenceColumns().isEmpty()) {
							if (referenceTable.getPrimaryKey() == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31094, t.getFullName(), referenceTableName.substring(index+1), referenceSchemaName));
							}
							else {
								uniqueKey = referenceTable.getPrimaryKey();										
							}
							
						} else {
							for (KeyRecord record : referenceTable.getUniqueKeys()) {
								if (keyMatches(fk.getReferenceColumns(), record)) {
									uniqueKey = record;
									break;
								}
							}
							if (uniqueKey == null && referenceTable.getPrimaryKey() != null && keyMatches(fk.getReferenceColumns(), referenceTable.getPrimaryKey())) {
								uniqueKey = referenceTable.getPrimaryKey();
							}
						}
						if (uniqueKey == null) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31095, t.getFullName(), referenceTableName.substring(index+1), referenceSchemaName, fk.getReferenceColumns()));
						}
						else {
							fk.setPrimaryKey(uniqueKey);
							fk.setUniqueKeyID(uniqueKey.getUUID());
						}
					}
				}						
			}
			
		}
	}
	

}
