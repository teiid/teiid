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

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.report.ReportItem;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

public class MetadataValidator {
	
	interface MetadataRule {
		void execute(VDBMetaData vdb, MetadataStore vdbStore, ValidatorReport report);
	}	
	
	public static ValidatorReport validate(VDBMetaData vdb, MetadataStore store) {
		ValidatorReport report = new ValidatorReport();
		if (store != null && !store.getSchemaList().isEmpty()) {
			new SourceModelArtifacts().execute(vdb, store, report);
			new ResolveQueryPlans().execute(vdb, store, report);
			new CrossSchemaResolver().execute(vdb, store, report);
			new MinimalMetadata().execute(vdb, store, report);
		}
		return report;
	}
	
	// At minimum the model must have table/view, procedure or function 
	static class MinimalMetadata implements MetadataRule {
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report) {
			for (Schema schema:store.getSchemas().values()) {
				ModelMetaData model = vdb.getModel(schema.getName());
				
				if (schema.getTables().isEmpty() 
						&& schema.getProcedures().isEmpty()
						&& schema.getFunctions().isEmpty()) {
					log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31070, model.getName()));
				}
				
				for (Table t:schema.getTables().values()) {
					if (t.getColumns() == null || t.getColumns().size() == 0) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31071, t.getName()));
					}					
				}
				
				// procedure validation is handled in parsing routines.
				
				if (!schema.getFunctions().isEmpty()) {
			    	ActivityReport<ReportItem> funcReport = new ActivityReport<ReportItem>("Translator metadata load " + model.getName()); //$NON-NLS-1$
					FunctionMetadataValidator.validateFunctionMethods(schema.getFunctions().values(),report);
					if(report.hasItems()) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31073, funcReport));
					}						
				}
			}
		}
	}
	
	// do not allow foreign tables, source functions in view model and vice versa 
	static class SourceModelArtifacts implements MetadataRule {
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report) {
			for (Schema schema:store.getSchemas().values()) {
				ModelMetaData model = vdb.getModel(schema.getName());
				for (Table t:schema.getTables().values()) {
					if (t.isVirtual() && model.isSource()) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31074, t.getName(), model.getName()));
					}
					
					if (t.isPhysical() && !model.isSource()) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31075, t.getName(), model.getName()));
					}
				}
				
				for (Procedure p:schema.getProcedures().values()) {
					if (p.isVirtual() && model.isSource()) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31076, p.getName(), model.getName()));
					}

					if (!p.isVirtual() && !model.isSource()) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31077, p.getName(), model.getName()));
					}
				}
				
				for (FunctionMethod func:schema.getFunctions().values()) {
					if (func.getPushdown().equals(FunctionMethod.PushDown.MUST_PUSHDOWN) && !model.isSource()) {
						log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31078, func.getName(), model.getName()));
					}
				}
			}
		}
	}	
	
	// Resolves metadata query plans to make sure they are accurate
	static class ResolveQueryPlans implements MetadataRule {
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report) {
			for (Schema schema:store.getSchemas().values()) {
				ModelMetaData model = vdb.getModel(schema.getName());

				for (Table t:schema.getTables().values()) {
					if (t.isVirtual()) {
						if (t.getSelectTransformation() == null) {
							log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31079, t.getName(), model.getName()));
						}
						validate(vdb, model, t, store, report);						
					}						
				}
				
				for (Procedure p:schema.getProcedures().values()) {
					if (p.isVirtual() && !p.isFunction()) {
						if (p.getQueryPlan() == null) {
							log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31081, p.getName(), model.getName()));
						}
						validate(vdb, model, p, store, report);
					}
				}					
			}
		}
	}	
	
	private static void log(ValidatorReport report, ModelMetaData model, String msg) {
		model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), msg);
		LogManager.logInfo(LogConstants.CTX_QUERY_RESOLVER, msg);
		report.handleValidationError(msg);
	}
	
    private static void validate(VDBMetaData vdb, ModelMetaData model, AbstractMetadataRecord record, MetadataStore store, ValidatorReport report) {
    	QueryMetadataInterface metadata = vdb.getAttachment(QueryMetadataInterface.class);
    	ValidatorReport resolverReport = null;
    	try {
    		if (record instanceof Procedure) {
    			Procedure p = (Procedure)record;
    			Command command = QueryParser.getQueryParser().parseCommand(p.getQueryPlan());
    			QueryResolver.resolveCommand(command, new GroupSymbol(p.getFullName()), Command.TYPE_STORED_PROCEDURE, metadata);
    			resolverReport =  Validator.validate(command, metadata);
    		}
    		if (record instanceof Table) {
    			Table t = (Table)record;
    			
    			if (t.isVirtual() && (t.getColumns() == null || t.getColumns().isEmpty())) {
    				QueryCommand command = (QueryCommand)QueryParser.getQueryParser().parseCommand(t.getSelectTransformation());
    				QueryResolver.resolveCommand(command, metadata);
    				resolverReport =  Validator.validate(command, metadata);
    				if(!resolverReport.hasItems()) {
    					List<Expression> symbols = command.getProjectedSymbols();
    					for (Expression column:symbols) {
    						try {
								addColumn(Symbol.getShortName(column), getDataType(store.getDatatypes(), column.getType()), t);
							} catch (TranslatorException e) {
								log(report, model, e.getMessage());
							}
    					}
    				}
    			}
    			
    			Query command = (Query)QueryParser.getQueryParser().parseCommand("SELECT * FROM "+t.getFullName()); //$NON-NLS-1$
    			QueryResolver.resolveCommand(command, metadata);
    			GroupSymbol resolvedGroup = command.getFrom().getGroups().get(0);

    			// this seems to parse, resolve and validate.
    			QueryResolver.resolveView(resolvedGroup, new QueryNode(t.getSelectTransformation()), SQLConstants.Reserved.SELECT, metadata);
    		}
			if(resolverReport != null && resolverReport.hasItems()) {
				for (ValidatorFailure v:resolverReport.getItems()) {
					if (v.getStatus() == ValidatorFailure.Status.ERROR) {
						log(report, model, v.getMessage());
					}
				}
			}
		} catch (QueryParserException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getFullMessage()));
		} catch (QueryResolverException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getFullMessage()));
		} catch (TeiidComponentException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getFullMessage()));
		} catch (QueryValidatorException e) {
			log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31080, record.getFullName(), e.getFullMessage()));
		}
    }

	private static Datatype getDataType(Collection<Datatype> dataTypes, Class clazz) {
		for (Datatype type:dataTypes) {
			if (type.getJavaClassName().equals(clazz.getName())) {
				return type;
			}
		}
		return null;
	}
	
	private static Column addColumn(String name, Datatype type, Table table) throws TranslatorException {
		Column column = new Column();
		column.setName(name);
		if (table.getColumns() != null) {
			for (Column c:table.getColumns()) {
				if (c.getName().equalsIgnoreCase(name)) {
					throw new TranslatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31087, name, table.getFullName()));
				}
			}
		}
		table.addColumn(column);
		column.setPosition(table.getColumns().size()); //1 based indexing
		if (type == null) {
			throw new TranslatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31086, name, table.getFullName()));
		}
		column.setDatatype(type);
		column.setDatatypeUUID(type.getUUID());
		column.setLength(type.getLength());
		column.setPrecision(type.getPrecisionLength());
		column.setRadix(type.getRadix());
		column.setRuntimeType(type.getRuntimeTypeName());
		column.setCaseSensitive(type.isCaseSensitive());
		column.setAutoIncremented(type.isAutoIncrement());
		column.setSigned(type.isSigned());		
		column.setUUID("mmuuid:" +UUID.randomUUID()); //$NON-NLS-1$
		return column;		
	}
	
	// this class resolves the artifacts that are dependent upon objects from other schemas
	// materialization sources, fk and data types (coming soon..)
	static class CrossSchemaResolver implements MetadataRule {
		
		private boolean keyMatches(List<String> names, KeyRecord record) {
			if (names.size() != record.getColumns().size()) {
				return false;
			}
			for (int i = 0; i < names.size(); i++) {
				if (!names.get(i).equals(record.getColumns().get(i).getName())) {
					return false;
				}
			}
			return true;
		}
		
		@Override
		public void execute(VDBMetaData vdb, MetadataStore store, ValidatorReport report) {
			for (Schema schema:store.getSchemas().values()) {
				ModelMetaData model = vdb.getModel(schema.getName());

				for (Table t:schema.getTables().values()) {
					if (t.isVirtual()) {
						if (t.isMaterialized() && t.getMaterializedTable() != null) {
							String matTableName = t.getMaterializedTable().getName();
							int index = matTableName.indexOf(Table.NAME_DELIM_CHAR);
							if (index == -1) {
								log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31088, matTableName, t.getName()));
							}
							else {
								String schemaName = matTableName.substring(0, index);
								Schema matSchema = store.getSchema(schemaName);
								if (matSchema == null) {
									log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31089, schemaName, matTableName, t.getName()));
								}
								else {
									Table matTable = matSchema.getTable(matTableName.substring(index+1));
									if (matTable == null) {
										log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31090, matTableName.substring(index+1), schemaName, t.getName()));
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
						if (referenceTableName == null && fk.getPrimaryKey() == null){
							log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31091, t.getName()));
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
								log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31093, referenceSchemaName, t.getName()));
								continue;
							}
							referenceTable = referenceSchema.getTable(referenceTableName.substring(index+1));
						}
						
						if (referenceTable == null) {
							log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31092, t.getName(), referenceTableName.substring(index+1), referenceSchemaName));
							continue;
						}

						KeyRecord uniqueKey = null;
						if (fk.getReferenceColumns() == null || fk.getReferenceColumns().isEmpty()) {
							if (referenceTable.getPrimaryKey() == null) {
								log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31094, t.getName(), referenceTableName.substring(index+1), referenceSchemaName));
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
							log(report, model, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31095, t.getName(), referenceTableName.substring(index+1), referenceSchemaName, fk.getReferenceColumns()));
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
