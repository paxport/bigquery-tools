package com.paxport.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Will use property descriptors to create table definition and convert items into rows
 * will assume all properties are required unless defined as an Optional<E> or defaultFieldMode
 * is overridden
 * @param <E> the type of item that will map to a row
 */
public abstract class ReflectionBigQueryTable<E> extends BigQueryTable {

    private final static Logger logger = LoggerFactory.getLogger(ReflectionBigQueryTable.class);

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
        List<TableFieldSchema> fields = createFieldsForClass(targetClass);
        // now add in the pre-defined fields
        fields.addAll(definedFields.values());
        return fields;
    }

    private List<TableFieldSchema> createFieldsForClass(Class cls) {
        if(logger.isDebugEnabled()){
            logger.debug("Creating Fields for class " + cls.getName() );
        }
        List<TableFieldSchema> fields = new ArrayList<>();
        // use reflection to build fields
        List<PropertyDescriptor> propertyDescriptors = propertyDescriptors(cls);
        for (PropertyDescriptor descriptor : propertyDescriptors) {
            propertyDescriptorMap.put(descriptor.getName(),descriptor);
            if ( !excludedProperties.contains(descriptor.getName())){
                TableFieldSchema field = fieldFromPropertyDescriptor(descriptor);
                if ( field != null ){
                    if(logger.isDebugEnabled()){
                        logger.debug("Adding field " + field.getName() + " of type " + field.getType() );
                    }
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    /**
     * Need to grab properties for any getProp or isProp including interfaces
     * Don't bother with getClass()
     * @param targetClass
     * @return
     */
    protected List<PropertyDescriptor> propertyDescriptors(Class<E> targetClass){

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
                    try{
                        PropertyDescriptor descriptor = new PropertyDescriptor(propName,method,null);
                        logger.debug("Property Descriptor " + descriptor);
                        result.add (descriptor);
                    }
                    catch (IntrospectionException e){
                        logger.info("no prop descriptor available for " + propName + " in " + targetClass.getSimpleName() +
                            " due to " + e.getMessage()
                        );
                    }
                }
            }
            return result;

    }

    protected TableFieldSchema fieldFromPropertyDescriptor(PropertyDescriptor prop) {
        if ( definedFields.containsKey(prop.getName()) ) {
            // defined elsewhere
            return null;
        }
        if ( excludedProperties.contains(prop.getName()) ) {
            // excluded
            return null;
        }
        PropInfo propInfo = PropInfo.create(prop);
        FieldType fieldType = FieldType.fromClass(propInfo.getPropertyClass());
        FieldMode mode = propInfo.isOptional() ? FieldMode.NULLABLE : defaultFieldMode();
        TableFieldSchema result = field(prop.getName(),fieldType,mode);
        if ( fieldType == FieldType.RECORD ){
            result.setFields(createFieldsForClass(prop.getPropertyType()));
        }
        return result;
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

    protected Map<String,Object> toRow (E item){
        Map<String,Object> map = toMap(item, tableFields());
        if ( logger.isDebugEnabled() ){
            logger.debug("row map into bigquery ->\n" + map);
        }
        return map;
    }

    protected Map<String,Object> toMap (Object item, Collection<TableFieldSchema> fields) {
        Map<String,Object> row = new HashMap<>();
        for (TableFieldSchema field : fields) {
            PropertyDescriptor descriptor = propertyDescriptorMap.get(field.getName());
            if ( descriptor != null && !excludedProperties.contains(field.getName())) {
                try{
                    Object value = valueForProperty(descriptor,item);
                    if ( value != null ){
                        value = convertValueIntoColumnData(value, field);
                        row.put(descriptor.getName(),value);
                    }
                }
                catch (Exception e){
                    logger.warn("Failed to read value for property " + descriptor.getName() + " of item " + item, e );
                }
            }
        }
        return row;
    }

    protected Object valueForProperty(PropertyDescriptor descriptor, Object item) {
        try {
            Object value = descriptor.getReadMethod().invoke(item);
            if ( value instanceof Optional ) {
                Optional optional = (Optional) value;
                value = optional.orElse(null);
            }
            return value;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object convertValueIntoColumnData(Object value, TableFieldSchema field) {
        switch ( FieldType.valueOf(field.getType()) ) {
            case RECORD :
                if ( value instanceof Map ){
                    return value;
                }
                return toMap(value,field.getFields());

            case STRING:
                return String.valueOf(value);

            case TIMESTAMP:
                if ( value instanceof ZonedDateTime ) {
                    ZonedDateTime zdt = (ZonedDateTime) value;
                    return zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                }

            default:
                return value;
        }
    }
}
