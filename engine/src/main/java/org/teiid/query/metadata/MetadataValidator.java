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
package org.teiid.query.metadata;

import static org.teiid.query.metadata.MaterializationMetadataRepository.*;

import java.util.*;

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
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.MaterializationMetadataRepository.Scope;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.report.ReportItem;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.validator.AbstractValidationVisitor;
import org.teiid.query.validator.ValidationVisitor;
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
			new MatViewPropertiesValidator().execute(vdb, store, report, this);
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
					FunctionMetadataValidator.validateFunctionMethods(schema.getFunctions().values(),report, store.getDatatypes());
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
						if (t.getTableType() == Table.Type.TemporaryTable) {
						    continue;
						}
						if (t.isVirtual()) {
							if (t.getSelectTransformation() == null) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31079, t.getFullName(), model.getName()));
							}
							else {
								metadataValidator.validate(vdb, model, t, report, metadata, mf);
							}
						} else {
						    for (Trigger tr : t.getTriggers().values()) {
		                        int commandType = Command.TYPE_INSERT;
		                        if (tr.getEvent() == TriggerEvent.DELETE) {
		                            commandType = Command.TYPE_DELETE;
		                        } else if (tr.getEvent() == TriggerEvent.UPDATE) {
		                            commandType = Command.TYPE_UPDATE;
		                        }
		                        try {
		                            metadataValidator.validateUpdatePlan(model, report, metadata, t, tr.getPlan(), commandType);
    						    } catch (TeiidException e) {
    						        metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getMessage()));
    					        }
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
	
	static class MatViewPropertiesValidator implements MetadataRule {

        @Override
        public void execute(final VDBMetaData vdb, MetadataStore store, ValidatorReport report, MetadataValidator metadataValidator) {

            for (Schema schema : store.getSchemaList()) {
                if (vdb.getImportedModels().contains(schema.getName())) {
                    continue;
                }
                ModelMetaData model = vdb.getModel(schema.getName());
                
                for (final Table t:schema.getTables().values()) {
                    if (t.isVirtual() && t.isMaterialized() && t.getMaterializedTable() != null) {
                    	Table matTable = t.getMaterializedTable();
                        Table stageTable = t.getMaterializedStageTable();
                        
                        // set the original names of the VDB with view, incase VDB is imported
                        // into another VDB
                        t.setProperty(MATVIEW_OWNER_VDB_NAME, vdb.getName());
                        t.setProperty(MATVIEW_OWNER_VDB_VERSION, vdb.getVersion());
                        
                        String beforeScript = t.getProperty(MATVIEW_BEFORE_LOAD_SCRIPT, false);
                        String afterScript = t.getProperty(MATVIEW_AFTER_LOAD_SCRIPT, false);
    					if (beforeScript == null || afterScript == null) {
    						metadataValidator.log(report, model, Severity.WARNING, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31155, t.getFullName()));
    					}

    					String matViewLoadNumberColumn = t.getProperty(MATVIEW_LOADNUMBER_COLUMN, false);
                        verifyTableColumns(model, report, metadataValidator, t, matTable, matViewLoadNumberColumn);
                        if(stageTable != null) {
                            verifyTableColumns(model, report, metadataValidator, t, stageTable, matViewLoadNumberColumn);
                        }
                        if (matViewLoadNumberColumn != null) {
                        	Column column = matTable.getColumnByName(matViewLoadNumberColumn);
                        	if (column == null) {
                        		metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31218, t.getFullName(), matTable.getFullName(), matViewLoadNumberColumn));
                        		continue;
                        	} else if (!column.getRuntimeType().equalsIgnoreCase(DataTypeManager.DefaultDataTypes.LONG)) {
								metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31215, t.getFullName(), matTable.getFullName(), matViewLoadNumberColumn, column.getRuntimeType()));
								continue;
                        	}
                        }

                        String status = t.getProperty(MATVIEW_STATUS_TABLE, false);
                        String loadScript = t.getProperty(MATVIEW_LOAD_SCRIPT, false);
                        if (status == null) {
                        	metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31154, t.getFullName()));
                        	continue; 
                        }
                        
                        if (matViewLoadNumberColumn == null && stageTable == null && loadScript == null) {
                        	metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31216, t.getFullName()));
                        	continue; 
                        }
                        
                        String scope = t.getProperty(MATVIEW_SHARE_SCOPE, false);
                        if (scope != null && !scope.equalsIgnoreCase(Scope.IMPORTED.name())
                                && !scope.equalsIgnoreCase(Scope.FULL.name())) {
                            metadataValidator.log(report, model, Severity.WARNING,QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31253, t.getFullName(), scope));
                            t.setProperty(MATVIEW_SHARE_SCOPE, Scope.IMPORTED.name());
                        }
                        
                        String stalenessString = t.getProperty(MaterializationMetadataRepository.MATVIEW_MAX_STALENESS_PCT, false);
                        if (stalenessString != null) {
                            final HashSet<Table> ids = new HashSet<Table>();
                            listPhysicalTables(t.getIncomingObjects(), new TableFilter() {
                                @Override
                                public void accept(Table physicalTable) {
                                    ids.add(physicalTable);
                                }
                            });
                            for (Table physicalTable : ids) {
                                addLazyMatViewTrigger(vdb, t, physicalTable, Table.TriggerEvent.INSERT);
                                addLazyMatViewTrigger(vdb, t, physicalTable, Table.TriggerEvent.UPDATE);
                                addLazyMatViewTrigger(vdb, t, physicalTable, Table.TriggerEvent.DELETE);
                            }
                        }
                        
						Table statusTable = findTableByName(store, status);
						if (statusTable == null) {
							metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31197, t.getFullName(), status));
							continue;
                        }
						
                		Map<String, Class<?>> statusTypeMap = new TreeMap<String, Class<?>>(String.CASE_INSENSITIVE_ORDER);
            			statusTypeMap.put("VDBNAME", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("VDBVERSION", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("SCHEMANAME", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("NAME", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("TARGETSCHEMANAME", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("TARGETNAME", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("VALID", DataTypeManager.DefaultDataClasses.BOOLEAN); //$NON-NLS-1$
            			statusTypeMap.put("LOADSTATE", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("CARDINALITY", DataTypeManager.DefaultDataClasses.LONG); //$NON-NLS-1$
            			statusTypeMap.put("UPDATED", DataTypeManager.DefaultDataClasses.TIMESTAMP); //$NON-NLS-1$
            			statusTypeMap.put("LOADNUMBER", DataTypeManager.DefaultDataClasses.LONG); //$NON-NLS-1$
            			statusTypeMap.put("NODENAME", DataTypeManager.DefaultDataClasses.STRING); //$NON-NLS-1$
            			statusTypeMap.put("STALECOUNT", DataTypeManager.DefaultDataClasses.LONG); //$NON-NLS-1$
                                                
                        List<Column> statusColumns = statusTable.getColumns();
                        for(int i = 0 ; i < statusColumns.size() ; i ++) {
                            String name = statusColumns.get(i).getName();
                            Class<?> expectedType = statusTypeMap.remove(name);
                            if (expectedType == null) {
                            	continue; //unknown column
                            }
                            Class<?> type = statusColumns.get(i).getJavaType();
                            
                            if(type != expectedType) {
                                metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31195, t.getName(), statusTable.getFullName(), name, type, expectedType));
                            }
                        }
                        
                        if (!statusTypeMap.isEmpty()) {
                            metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31196, t.getName(), statusTable.getFullName(), statusTypeMap.keySet()));
                        }
                        
                        // validate the load scripts
                        String manage = t.getProperty(ALLOW_MATVIEW_MANAGEMENT, false); 
    					if (Boolean.valueOf(manage)) {
                            loadScriptsValidation(vdb, report, metadataValidator, model, t, t.getProperty(ON_VDB_START_SCRIPT, false), "ON_VDB_START_SCRIPT");//$NON-NLS-1$
                            loadScriptsValidation(vdb, report, metadataValidator, model, t, t.getProperty(ON_VDB_DROP_SCRIPT, false), "ON_VDB_DROP_SCRIPT");//$NON-NLS-1$
    					}
                        loadScriptsValidation(vdb, report, metadataValidator, model, t, t.getProperty(MATVIEW_BEFORE_LOAD_SCRIPT, false), "MATVIEW_BEFORE_LOAD_SCRIPT");//$NON-NLS-1$
                        loadScriptsValidation(vdb, report, metadataValidator, model, t, t.getProperty(MATVIEW_LOAD_SCRIPT, false), "MATVIEW_LOAD_SCRIPT");//$NON-NLS-1$
                        loadScriptsValidation(vdb, report, metadataValidator, model, t, t.getProperty(MATVIEW_AFTER_LOAD_SCRIPT, false), "MATVIEW_AFTER_LOAD_SCRIPT");//$NON-NLS-1$
                    } else if (t.isVirtual() && t.isMaterialized() && t.getMaterializedTable() == null) {
                    	// internal materialization
                    	String manage = t.getProperty(ALLOW_MATVIEW_MANAGEMENT, false);
                    	if (!Boolean.valueOf(manage)) {
                    		continue;
                    	}
                    	
                    	String updatable = t.getProperty(MATVIEW_UPDATABLE, false);
                    	if (updatable == null || !Boolean.parseBoolean(updatable)) {
                    		metadataValidator.log(report, model, Severity.WARNING, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31217, t.getFullName()));
                    	}
                    }
                } 
            }
        }
        
        interface TableFilter {
            void accept(Table table);
        }
        
        private void listPhysicalTables(Collection<AbstractMetadataRecord> records, TableFilter tableFilter) {
            for (AbstractMetadataRecord record : records) {
                if (record instanceof Table) {
                    Table table = (Table)record;
                    if (table.isPhysical()) {
                        tableFilter.accept(table);
                    } else {
                        listPhysicalTables(table.getIncomingObjects(), tableFilter);
                    }
                } else if (record instanceof Procedure) {
                    Procedure proc = (Procedure)record;
                    if (proc.isVirtual()) {
                        listPhysicalTables(proc.getIncomingObjects(), tableFilter);
                    }
                }
            }
        }

        private void addLazyMatViewTrigger(VDBMetaData vdb, Table t, Table st, Table.TriggerEvent event) {
            String name = "ON_"+st.getName()+"_"+event.name()+"_FOR_"+t.getName()+"_FOR_LAZY_SNAPSHOT";
            String plan = "FOR EACH ROW\n"
                    + "BEGIN ATOMIC\n"
                    + "EXECUTE SYSADMIN.updateStaleCount(schemaName=>'"+t.getParent().getName()+"', viewName=>'"+t.getName()+"');\n"
                    + "END\n";
            Trigger trigger = new Trigger();
            trigger.setName(name);
            trigger.setEvent(event);
            trigger.setPlan(plan);
            trigger.setAfter(true);
            trigger.setProperty(DDLStringVisitor.GENERATED, "true");
            st.getTriggers().put(name, trigger);
            LogManager.logDetail(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31256, st.getName(), t.getName()));
        }
        
        private void loadScriptsValidation(VDBMetaData vdb, ValidatorReport report, MetadataValidator metadataValidator, ModelMetaData model, Table matView, String script, String option) {
        	if(script == null) {
        		return;
        	}
            QueryMetadataInterface metadata = vdb.getAttachment(QueryMetadataInterface.class);
            QueryParser queryParser = QueryParser.getQueryParser();
            try {
                Command command = queryParser.parseCommand(script);
                if (command instanceof CreateProcedureCommand) {
                	((CreateProcedureCommand)command).setResultSetColumns(Collections.EMPTY_LIST);
                }
                QueryResolver.resolveCommand(command, metadata);        
                AbstractValidationVisitor visitor = new ValidationVisitor();
                ValidatorReport subReport = Validator.validate(command, metadata, visitor);
                metadataValidator.processReport(model, matView, report, subReport);
            } catch (QueryParserException | QueryResolverException | TeiidComponentException e) {
                metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31198, matView.getFullName(), option, script, e));
            } 
        }

		private void verifyTableColumns(ModelMetaData model, ValidatorReport report,
				MetadataValidator metadataValidator, Table view, Table matView, String ignoreColumnOnMatView) {
            List<Column> columns = view.getColumns();
	        for(int i = 0 ; i < columns.size() ; i ++) {
	            Column column = columns.get(i);
	            Column matViewColumn = matView.getColumnByName(column.getName());
	            if (matViewColumn == null) {
	            	metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31193, column.getName(), matView.getFullName(), view.getFullName()));
	            } else if(!column.getDatatypeUUID().equals(matViewColumn.getDatatypeUUID())){
	                metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31194, matViewColumn.getName(), matView.getFullName(), column.getName(), view.getFullName()));
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
    				QueryCommand command = (QueryCommand)parser.parseCommand(selectTransformation);
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
		    						Expression ex = parser.parseExpression(exprString);
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
    			
    			if (addCacheHint && t.isMaterialized()) {
        			// this seems to parse, resolve and validate.
        			QueryNode node = QueryResolver.resolveView(symbol, new QueryNode(selectTransformation), SQLConstants.Reserved.SELECT, metadata, true);
        			CacheHint cacheHint = node.getCommand().getCacheHint();
    				Long ttl = -1L;
    				if (cacheHint != null) {
        				if (cacheHint.getTtl() != null && t.getProperty(MaterializationMetadataRepository.MATVIEW_TTL, false) == null) {
        					ttl = cacheHint.getTtl();
        					t.setProperty(MaterializationMetadataRepository.MATVIEW_TTL, String.valueOf(ttl));
        				}
        				if (cacheHint.getUpdatable() != null && t.getProperty(MaterializationMetadataRepository.MATVIEW_UPDATABLE, false) == null) {
        					t.setProperty(MaterializationMetadataRepository.MATVIEW_UPDATABLE, String.valueOf(cacheHint.getUpdatable()));
        				}
        				if (cacheHint.getPrefersMemory() != null && t.getProperty(MaterializationMetadataRepository.MATVIEW_PREFER_MEMORY, false) == null) {
        					t.setProperty(MaterializationMetadataRepository.MATVIEW_PREFER_MEMORY, String.valueOf(cacheHint.getPrefersMemory()));
        				}
        				if (cacheHint.getScope() != null && t.getProperty(MaterializationMetadataRepository.MATVIEW_SHARE_SCOPE, false) == null) {
        				    log(report, model, Severity.WARNING, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31252, t.getName(), cacheHint.getScope().name()));				    
        					t.setProperty(MaterializationMetadataRepository.MATVIEW_SHARE_SCOPE, MaterializationMetadataRepository.Scope.IMPORTED.name());
        				}
    				}
    			}
    		}
			processReport(model, record, report, resolverReport);
		} catch (TeiidException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getMessage()));
		}
    }

	public static void determineDependencies(AbstractMetadataRecord p, Command command) {
		LinkedHashSet<AbstractMetadataRecord> values = new LinkedHashSet<AbstractMetadataRecord>();
		collectDependencies(command, values);
		p.setIncomingObjects(new ArrayList<AbstractMetadataRecord>(values));
		if (p instanceof Table) {
		    Table t = (Table)p;
		    for (int i = 0; i < t.getColumns().size(); i++) {
		        LinkedHashSet<AbstractMetadataRecord> columnValues = new LinkedHashSet<AbstractMetadataRecord>();
		        Column c = t.getColumns().get(i);
		        c.setIncomingObjects(columnValues);
                determineDependencies(command, c, i, columnValues);
		    }
		}
	}

    private static void collectDependencies(org.teiid.query.sql.LanguageObject lo,
            LinkedHashSet<AbstractMetadataRecord> values) {
        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(lo, true);
        for (GroupSymbol group : groups) {
            Object mid = group.getMetadataID();
            if (mid instanceof TempMetadataAdapter) {
                mid = ((TempMetadataID)mid).getOriginalMetadataID();
            }
            if (mid instanceof AbstractMetadataRecord) {
                values.add((AbstractMetadataRecord)mid);
            }
        }
        Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(lo, true, true);
		for (ElementSymbol elem : elems) {
			Object mid = elem.getMetadataID();
			if (mid instanceof TempMetadataAdapter) {
				mid = ((TempMetadataID)mid).getOriginalMetadataID();
			}
			if (mid instanceof AbstractMetadataRecord) {
				values.add((AbstractMetadataRecord)mid);
			}
		}
    }

    private static void determineDependencies(Command command, Column c, int index, LinkedHashSet<AbstractMetadataRecord> columnValues) {
        if (command instanceof Query) {
            Expression ex = command.getProjectedSymbols().get(index);
            collectDependencies(ex, columnValues);
        } else if (command instanceof SetQuery) {
            determineDependencies(((SetQuery)command).getLeftQuery(), c, index, columnValues);
            determineDependencies(((SetQuery)command).getRightQuery(), c, index, columnValues);
        }
    }
	
	private static Table findTableByName(MetadataStore store, String name) {
	    
        Table table = null;
        
        int index = name.indexOf(Table.NAME_DELIM_CHAR);
        if(index == -1) {
            for(Schema schema : store.getSchemaList()) {
                table = schema.getTable(name);
                if(table != null) {
                    break;
                }
            }
        } else {
            String schemaName = name.substring(0, index);
            Schema schema = store.getSchema(schemaName);
            if(schema != null) {
                table = schema.getTable(name.substring(index+1));
            }
        }

        return table;
    }

	private void validateUpdatePlan(ModelMetaData model,
			ValidatorReport report,
			QueryMetadataInterface metadata, 
			Table t, String plan, int type) throws QueryParserException, QueryResolverException,
			TeiidComponentException {
		Command command = parser.parseProcedure(plan, true);
		
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
							
							String stageTable = t.getProperty(MATVIEW_STAGE_TABLE, false);
							if(stageTable != null){
							    Table materializedStageTable = findTableByName(store, stageTable);
							    if(materializedStageTable == null) {
							        metadataValidator.log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31192, t.getFullName(), MATVIEW_STAGE_TABLE, stageTable));
							    } else {
							        t.setMaterializedStageTable(materializedStageTable);
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
