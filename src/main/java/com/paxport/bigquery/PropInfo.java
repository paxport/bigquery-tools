package com.paxport.bigquery;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;

import javax.annotation.Nullable;

/**
 * Work out which Class a property is from return type
 * Deals with Optional to unwrap target type
 */
public class PropInfo {

    private final Class propertyClass;
    private final boolean optional;

    private PropInfo(Class<?> propertyClass, boolean optional){
        this.propertyClass = propertyClass;
        this.optional = optional;
    }

    public Class getPropertyClass() {
        return propertyClass;
    }

    public boolean isOptional() {
        return optional;
    }

    public static PropInfo create(Method readMethod) {
        Type type = readMethod.getGenericReturnType();
        boolean optional = readMethod.getAnnotation(Nullable.class) != null;
        if ( type instanceof ParameterizedType ) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            if ( parameterizedType.getRawType().equals(Optional.class) ) {
                optional = true;
                type = parameterizedType.getActualTypeArguments()[0];
            }
            else {
                type = parameterizedType.getRawType();
            }
        }
        if ( type instanceof Class ){
            return new PropInfo((Class)type,optional);
        }
        else {
            throw new RuntimeException("Failed to determine property type info for type: " + type.getTypeName());
        }
    }

    public static PropInfo create(PropertyDescriptor descriptor) {
        return create(descriptor.getReadMethod());
    }

}
