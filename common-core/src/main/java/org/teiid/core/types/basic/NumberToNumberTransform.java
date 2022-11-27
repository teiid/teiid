package org.teiid.core.types.basic;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;

public abstract class NumberToNumberTransform extends Transform {

    private Class<?> sourceType;
    private Comparable<?> min;
    private Comparable<?> max;

    public NumberToNumberTransform(Number min, Number max, Class<?> sourceType) {
        this.sourceType = sourceType;
        if (sourceType == Short.class) {
            this.min = min.shortValue();
            this.max = max.shortValue();
        } else if (sourceType == Integer.class) {
            this.min = min.intValue();
            this.max = max.intValue();
        } else if (sourceType == Long.class) {
            this.min = min.longValue();
            this.max = max.longValue();
        } else if (sourceType == Float.class) {
            this.min = min.floatValue();
            this.max = max.floatValue();
        } else if (sourceType == Double.class) {
            this.min = min.doubleValue();
            this.max = max.doubleValue();
        } else if (sourceType == BigInteger.class) {
            if (min instanceof Double || min instanceof Float) {
                this.min = BigDecimal.valueOf(min.doubleValue()).toBigInteger();
                this.max = BigDecimal.valueOf(max.doubleValue()).toBigInteger();
            } else {
                this.min = BigInteger.valueOf(min.longValue());
                this.max = BigInteger.valueOf(max.longValue());
            }
        } else if (sourceType == BigDecimal.class) {
            if (min instanceof Double || min instanceof Float) {
                this.min = BigDecimal.valueOf(min.doubleValue());
                this.max = BigDecimal.valueOf(max.doubleValue());
            } else {
                this.min = BigDecimal.valueOf(min.longValue());
                this.max = BigDecimal.valueOf(max.longValue());
            }
        } else if (sourceType == Byte.class) {
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public Class<?> getSourceType() {
        return sourceType;
    }

    protected void checkValueRange(Object value)
            throws TransformationException {
        if (((Comparable)value).compareTo(min) < 0 || ((Comparable)value).compareTo(max) > 0) {
              throw new TransformationException(CorePlugin.Event.TEIID10058, CorePlugin.Util.gs(CorePlugin.Event.TEIID10058, value, getSourceType().getSimpleName(), getTargetType().getSimpleName()));
        }
    }

}
