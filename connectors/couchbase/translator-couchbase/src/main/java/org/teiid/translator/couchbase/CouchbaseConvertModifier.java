package org.teiid.translator.couchbase;


import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.translator.jdbc.FunctionModifier;

/**
 * https://developer.couchbase.com/documentation/server/current/n1ql/n1ql-language-reference/typefun.html
 * @author kylin
 *
 */
public class CouchbaseConvertModifier extends FunctionModifier{
    
    @Override
    public List<?> translate(Function function) {
        Expression param = function.getParameters().get(0);
        int targetCode = getCode(function.getType());
        int sourceCode = getCode(param.getType());
        if(targetCode == BYTE || targetCode == SHORT || targetCode == INTEGER || targetCode == LONG || targetCode == FLOAT || targetCode == DOUBLE ) {
            return Arrays.asList("TONUMBER" + Tokens.LPAREN, param, Tokens.RPAREN);//$NON-NLS-1$ 
        }
        return Arrays.asList("TOOBJECT" + Tokens.LPAREN, param, Tokens.RPAREN);//$NON-NLS-1$ 
    }

}
