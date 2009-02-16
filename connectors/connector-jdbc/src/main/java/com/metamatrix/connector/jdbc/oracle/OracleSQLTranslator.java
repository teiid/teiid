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

/*
 */
package com.metamatrix.connector.jdbc.oracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IFrom;
import com.metamatrix.connector.language.IFromItem;
import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.language.IInlineView;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILimit;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.IOrderBy;
import com.metamatrix.connector.language.IOrderByItem;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.language.ISelect;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.connector.language.ISetQuery;
import com.metamatrix.connector.language.ICompareCriteria.Operator;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.visitor.util.CollectorVisitor;
import com.metamatrix.connector.visitor.util.SQLStringVisitor;

/**
 */
public class OracleSQLTranslator extends BasicSQLTranslator {

    protected final static String ROWNUM = "ROWNUM"; //$NON-NLS-1$
    protected final static String ROWNUM_ALIAS = "MM_ROWNUM"; //$NON-NLS-1$
    protected final static String INLINE_VIEW_ALIAS = "MM_VIEW_FOR_LIMIT"; //$NON-NLS-1$
    protected final static String ROOT_EXPRESSION_NAME = "expr"; //$NON-NLS-1$

    

    private Map functionModifiers;
    private Properties connectorProperties;
    private ILanguageFactory languageFactory;

    public void initialize(ConnectorEnvironment env,
                           RuntimeMetadata metadata) throws ConnectorException {
        
        super.initialize(env, metadata);
        ConnectorEnvironment connEnv = getConnectorEnvironment();
        this.connectorProperties = connEnv.getProperties();
        this.languageFactory = connEnv.getLanguageFactory();
        initializeFunctionModifiers();  

    }

    /** 
     * @see com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator#modifyCommand(com.metamatrix.connector.language.ICommand, com.metamatrix.connector.api.ExecutionContext)
     * @since 5.0
     */
    public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
        command = super.modifyCommand(command, context);
        Collection subCommands = CollectorVisitor.collectObjects(IInlineView.class, command);
        for (Iterator i = subCommands.iterator(); i.hasNext();) {
            IInlineView inlineView = (IInlineView)i.next();
            inlineView.setQuery((IQueryCommand)modifyCommand(inlineView.getQuery(), context));
        }
        
        if (!(command instanceof IQueryCommand)) {
            return command;
        }
                
        return modifySingleCommand((IQueryCommand)command, context);
    }
    
    protected IQueryCommand modifySingleCommand(IQueryCommand command, ExecutionContext context) {
        if (command instanceof ISetQuery) {
            ISetQuery union = (ISetQuery)command;
            union.setLeftQuery(modifySingleCommand(union.getLeftQuery(), context));
            union.setRightQuery(modifySingleCommand(union.getRightQuery(), context));
            return union;
        } 
        
        if (command.getLimit() == null) {
            return command;
        }
        
        ILimit limit = command.getLimit();
        command.setLimit(null);
        
        List<ICriteria> lstCriteria = new ArrayList<ICriteria>();
        
        if (limit.getRowOffset() > 0) {
            IGroup group = languageFactory.createGroup(INLINE_VIEW_ALIAS, null, null);
            IElement eleRowNum = languageFactory.createElement(ROWNUM_ALIAS, group, null, TypeFacility.RUNTIME_TYPES.INTEGER);
            ILiteral litOffset = languageFactory.createLiteral(new Integer( limit.getRowOffset() ), TypeFacility.RUNTIME_TYPES.INTEGER);
            ICriteria criteria = languageFactory.createCompareCriteria(Operator.GT, eleRowNum, litOffset);
            lstCriteria.add( criteria );
        }

        IGroup group = languageFactory.createGroup(INLINE_VIEW_ALIAS, null, null);
        IElement eleRowNum = languageFactory.createElement(ROWNUM_ALIAS, group, null, TypeFacility.RUNTIME_TYPES.INTEGER);
        ILiteral litLimit = languageFactory.createLiteral(new Integer( limit.getRowOffset() + limit.getRowLimit() ),TypeFacility.RUNTIME_TYPES.INTEGER);
        ICriteria criteria = languageFactory.createCompareCriteria(Operator.LE, eleRowNum, litLimit);
        lstCriteria.add( criteria );
                
        if ( lstCriteria.size() == 1 ) {
            criteria = lstCriteria.get( 0 );
        } else {
            criteria = languageFactory.createCompoundCriteria(com.metamatrix.connector.language.ICompoundCriteria.Operator.AND, lstCriteria );
        }
        
        IQuery intermediate = createLimitQuery(command, null, true);
        
        IQuery result = createLimitQuery(intermediate, criteria, false);
        
        eleRowNum = languageFactory.createElement(OracleSQLConversionVisitor.ROWNUM, null, null, TypeFacility.RUNTIME_TYPES.INTEGER);
        ISelectSymbol newSelectSymbol = languageFactory.createSelectSymbol(ROWNUM_ALIAS, eleRowNum);
        newSelectSymbol.setAlias(true);
        intermediate.getSelect().getSelectSymbols().add(newSelectSymbol);
        
        return result;
    }

    /** 
     * @param query
     * @param criteria
     * @return
     * @since 5.0
     */
    private IQuery createLimitQuery(IQueryCommand query,
                                    ICriteria criteria, boolean alias) {
        IInlineView view = languageFactory.createInlineView(query, INLINE_VIEW_ALIAS);
        
        IFrom from = languageFactory.createFrom(Arrays.asList(new IFromItem[] {view}));
        
        LinkedHashMap<String, Class<?>> names = new LinkedHashMap<String, Class<?>>();
        
        List symbols = query.getProjectedQuery().getSelect().getSelectSymbols();
        IOrderBy orderBy = query.getOrderBy();
        HashMap<String, IOrderByItem> orderByNames = null;
        if (orderBy != null) {
        	orderByNames = new HashMap<String, IOrderByItem>();
        	for (IOrderByItem item : (List<IOrderByItem>)orderBy.getItems()) {
        		if (item.getName() != null) {
        			orderByNames.put(item.getName().toLowerCase(), item);
        		}
        	}
        }
        
        for (int i = 0; i < symbols.size(); i++) {
            ISelectSymbol symbol = (ISelectSymbol)symbols.get(i);

            String originalName = null;
                        
            if (symbol.getExpression() instanceof IElement || symbol.hasAlias()) {
            	originalName = SQLStringVisitor.getElementShortName(symbol.getOutputName()).toLowerCase();
            } else if (alias) {
            	originalName = ROOT_EXPRESSION_NAME; 
            }
            
            String name = originalName;
            
            int j = 0;
            while (names.containsKey(name)) {
                name = originalName + j++;
            }
            
            if (alias) {
                symbol.setAlias(true);
                symbol.setOutputName(name);
                if (orderByNames != null) {
                    IOrderByItem item = orderByNames.get(originalName);
                    if (item != null) {
                    	item.setName(name);
                    }
                }
            }
            names.put(name, symbol.getExpression().getType());
        }        
        List<ISelectSymbol> lstSelect = new ArrayList<ISelectSymbol>();

        for (Map.Entry<String, Class<?>> entry : names.entrySet()) {
            IGroup group = languageFactory.createGroup(INLINE_VIEW_ALIAS, null, null);
            IElement expression = languageFactory.createElement(entry.getKey(), group, null, entry.getValue());
            ISelectSymbol newSymbol = languageFactory.createSelectSymbol(entry.getKey(), expression);
            lstSelect.add(newSymbol);
        }
        
        ISelect select = languageFactory.createSelect(false, lstSelect);
        
        IQuery result = languageFactory.createQuery(select, from, criteria, null, null, null);
        return result;
    }
    
    /** 
     * @param modifier
     * @since 4.2
     */
    private void initializeFunctionModifiers() {
        functionModifiers = new HashMap();
        functionModifiers.putAll(super.getFunctionModifiers());
        functionModifiers.put("char", new AliasModifier("chr")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("lcase", new AliasModifier("lower")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ucase", new AliasModifier("upper")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ifnull", new AliasModifier("nvl")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("log", new AliasModifier("ln")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ceiling", new AliasModifier("ceil")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("log10", new Log10FunctionModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("convert", new OracleConvertModifier(languageFactory, getRuntimeMetadata(), getConnectorEnvironment().getLogger())); //$NON-NLS-1$
        functionModifiers.put("cast", new OracleConvertModifier(languageFactory, getRuntimeMetadata(), getConnectorEnvironment().getLogger())); //$NON-NLS-1$
        functionModifiers.put("hour", new HourFunctionModifier(languageFactory));//$NON-NLS-1$
        functionModifiers.put("month", new ExtractFunctionModifier("MONTH"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("year", new ExtractFunctionModifier("YEAR"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("day", new ExtractFunctionModifier("DAY"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("minute", new DayWeekQuarterFunctionModifier(languageFactory, "MI"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("second", new DayWeekQuarterFunctionModifier(languageFactory, "SS"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("monthname", new MonthOrDayNameFunctionModifier(languageFactory, "Month"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayname", new MonthOrDayNameFunctionModifier(languageFactory, "Day"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("week", new DayWeekQuarterFunctionModifier(languageFactory, "WW"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("quarter", new DayWeekQuarterFunctionModifier(languageFactory, "Q"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayofweek", new DayWeekQuarterFunctionModifier(languageFactory, "D"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayofmonth", new DayWeekQuarterFunctionModifier(languageFactory, "DD"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayofyear", new DayWeekQuarterFunctionModifier(languageFactory, "DDD"));//$NON-NLS-1$ //$NON-NLS-2$      
        functionModifiers.put("formatdate", new FormatFunctionModifier(languageFactory));//$NON-NLS-1$ 
        functionModifiers.put("formattime", new FormatFunctionModifier(languageFactory));//$NON-NLS-1$ 
        functionModifiers.put("formattimestamp", new FormatFunctionModifier(languageFactory));//$NON-NLS-1$ 
        functionModifiers.put("parsedate", new ParseFunctionModifier(languageFactory, java.sql.Date.class));//$NON-NLS-1$ 
        functionModifiers.put("parsetime", new ParseFunctionModifier(languageFactory, java.sql.Time.class));//$NON-NLS-1$ 
        functionModifiers.put("parsetimestamp", new ParseFunctionModifier(languageFactory, java.sql.Timestamp.class));//$NON-NLS-1$ 
        functionModifiers.put("locate", new LocateFunctionModifier(languageFactory));//$NON-NLS-1$
        functionModifiers.put("substring", new AliasModifier("substr"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("left", new LeftOrRightFunctionModifier(languageFactory, "left"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("right", new LeftOrRightFunctionModifier(languageFactory, "right"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("concat", new ConcatFunctionModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("||", new ConcatFunctionModifier(languageFactory)); //$NON-NLS-1$
    }    
    

    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getTranslationVisitor()
     */
    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new OracleSQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(functionModifiers);
        visitor.setProperties(connectorProperties);
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    }    
 
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getFunctionModifiers()
     */
    public Map getFunctionModifiers() {
        return functionModifiers;
    }

}
