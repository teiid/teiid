package org.teiid.translator.jdbc.modeshape;

import static org.teiid.language.SQLConstants.Reserved.BY;
import static org.teiid.language.SQLConstants.Reserved.ORDER;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.OrderBy;
import org.teiid.language.Select;
import org.teiid.language.SortSpecification;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Column;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class ModeShapeSQLVisitor extends SQLConversionVisitor  {
	
	private Map<String, Column> columnMap = new HashMap<String, Column>();
	private Map<String, Column> aliasMap = new HashMap<String, Column>();

	public ModeShapeSQLVisitor(JDBCExecutionFactory ef) {
		super(ef);

	}
	
	public void visit(Select query) {

		// if the query has an order by, then
		// need to cache the columns so that the 
		// order by column name can be replaced by its
		// correlating select column that has the nameInSource
		if (query.getOrderBy() == null) {
			super.visit(query);
			return;
		}
		
		List<DerivedColumn> selectSymbols = query.getDerivedColumns();
		Iterator<DerivedColumn> symbolIter = selectSymbols.iterator();
		while (symbolIter.hasNext()) {
			DerivedColumn symbol = symbolIter.next();
			Expression expression = symbol.getExpression();

			if (symbol.getAlias() != null) {
				
			}
			// cache the columns so that order by 
			if (expression instanceof ColumnReference) {
				ColumnReference colRef = (ColumnReference) expression;
				if (colRef.getMetadataObject() != null) {
					Column element = colRef.getMetadataObject();
					if (symbol.getAlias() != null) {
						aliasMap.put(symbol.getAlias(), element);
					}
					columnMap.put(element.getName(), element);
				}
			}
		}
		
		super.visit(query);
	}  

    public void visit(OrderBy obj) {
         buffer.append(ORDER)
        .append(Tokens.SPACE)
        .append(BY)
        .append(Tokens.SPACE);
        
    	List<SortSpecification> specs = obj.getSortSpecifications();
    	for (SortSpecification spec : specs) {
    		String specName = spec.getExpression().toString();
    		Column col = null;
    		
    		col = aliasMap.get(specName);
    		if (col == null) {
    			col = columnMap.get(specName);
    		}
    		if (col != null) {
    			buffer.append(ModeShapeUtil.createJCRName(col.getNameInSource()))
    			.append(" ")
    			.append(spec.getOrdering().toString());
    			
    		} else {
    			buffer.append(obj.getSortSpecifications());
    		}
    	}
         
    }

}
