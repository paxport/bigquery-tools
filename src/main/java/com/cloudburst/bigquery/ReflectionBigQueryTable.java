package com.cloudburst.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Will use property descriptors to create table definition and convert items into rows
 * will assume all properties are required unless defined as an Optional<E> or defaultFieldMode
 * is overridden
 * @param <E> the type of item that will map to a row
 */
public abstract class ReflectionBigQueryTable<E> extends BigQueryTable {

    private Class<E> targetClass;
    protected Set<String> excludedProperties = defaultExcludedProperties();
    protected Map<String,PropertyDescriptor> propertyDescriptorMap = new Hashtable<>();
    protected Map<String,TableFieldSchema> definedFields = customFields();

    protected ReflectionBigQueryTable(Class<E> cls, TableIdentifier identifier) {
        super(identifier);
        targetClass = cls;
    }

    /**
     * Override this and define your custom fields
     */
    protected Map<String,TableFieldSchema> customFields() {
        return new Hashtable<>();
    }

    protected Set<String> defaultExcludedProperties() {
        Set set = new HashSet<>();
        set.add("class");
        return set;
    }

    @Override
    protected List<TableFieldSchema> createTableFields() {
        List<TableFieldSchema> fields = new ArrayList<>();
        // use reflection to build fields
        List<PropertyDescriptor> propertyDescriptors = propertyDescriptors(targetClass);
        for (PropertyDescriptor descriptor : propertyDescriptors) {
            propertyDescriptorMap.put(descriptor.getName(),descriptor);
            if ( !excludedProperties.contains(descriptor.getName())){
                TableFieldSchema field = fieldFromPropertyDescriptor(descriptor);
                if ( field != null ){
                    fields.add(field);
                }
            }
        }
        // now add in the pre-defined fields
        fields.addAll(definedFields.values());
        return fields;
    }

    /**
     * Need to grab properties for any getProp or isProp including interfaces
     * Don't bother with getClass()
     * @param targetClass
     * @return
     */
    protected List<PropertyDescriptor> propertyDescriptors(Class<E> targetClass){
        try {
            List<PropertyDescriptor> result = new ArrayList<>();
            for (Method method : targetClass.getMethods()) {
                String methodName = method.getName();
                String propName = null;
                if ( methodName.startsWith("get") ) {
                    propName = methodName.substring(3);
                }
                else if (methodName.startsWith("is") ) {
                    propName = methodName.substring(2);
                }
                if ( propName != null && !"Class".equals(propName)) {
                    propName = Introspector.decapitalize(propName);
                    PropertyDescriptor descriptor = new PropertyDescriptor(propName,method,null);
                    result.add (descriptor);
                }
            }
            return result;
        } catch (IntrospectionException e) {
            throw new RuntimeException("Failed to determine property descriptors", e);
        }
    }

    protected TableFieldSchema fieldFromPropertyDescriptor(PropertyDescriptor prop) {
        if ( definedFields.containsKey(prop.getName()) ) {
            // defined elsewhere
            return null;
        }
        PropInfo propInfo = PropInfo.create(prop);
        FieldType fieldType = FieldType.fromClass(propInfo.getPropertyClass());
        FieldMode mode = propInfo.isOptional() ? FieldMode.NULLABLE : defaultFieldMode();
        return field(prop.getName(),fieldType,mode);
    }

    // required by default but easy to make nullable
    protected FieldMode defaultFieldMode() {
        return FieldMode.REQUIRED;
    }

    public TableDataInsertAllResponse insertItem(E item) throws IOException {
        return insertRow(toRow(item));
    }

    public TableDataInsertAllResponse insertItems(Collection<E> items) throws IOException {
        List<Map<String,Object>> rows = items.stream()
                .map(item -> toRow(item))
                .collect(Collectors.toList());
        return insertRows(rows);
    }

    protected Map<String,Object> toRow (E item) {
        Map<String,Object> row = new HashMap<>();
        for (TableFieldSchema field : tableFields()) {
            PropertyDescriptor descriptor = propertyDescriptorMap.get(field.getName());
            if ( descriptor != null && !excludedProperties.contains(field.getName())) {
                Object value = valueForProperty(descriptor,item);
                if ( value != null ){
                    row.put(descriptor.getName(),value);
                }
            }
        }
        return row;
    }

    protected Object valueForProperty(PropertyDescriptor descriptor, E item) {
        try {
            Object value = descriptor.getReadMethod().invoke(item);
            if ( value instanceof Optional ) {
                Optional optional = (Optional) value;
                value = optional.orElse(null);
            }
            return convertValueIntoColumnData(value, descriptor.getName());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object convertValueIntoColumnData(Object value, String propName) {
        if ( value instanceof ZonedDateTime ) {
            ZonedDateTime zdt = (ZonedDateTime) value;
            return zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        else {
            return value;
        }
    }
}
