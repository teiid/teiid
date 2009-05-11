package org.teiid.connector.language;

import java.util.List;

public interface IInsertExpressionValueSource extends IInsertValueSource, ILanguageObject {
	
	List<IExpression> getValues();

}
