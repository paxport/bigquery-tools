package com.paxport.bigquery;

import static org.junit.Assert.*;
import org.junit.Test;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.util.Map;

public class PropInfoTest {


    @Test
    public void testOptional() {
        PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(ExampleItem.class,"optionalString");
        PropInfo propInfo = PropInfo.create(pd);
        assertEquals(true,propInfo.isOptional());
        assertEquals(String.class,propInfo.getPropertyClass());
    }

    @Test
    public void testNonOptional() {
        PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(ExampleItem.class,"requiredString");
        PropInfo propInfo = PropInfo.create(pd);
        assertEquals(false,propInfo.isOptional());
        assertEquals(String.class,propInfo.getPropertyClass());
    }

    @Test
    public void testMap() {
        PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(ExampleItem.class,"tracking");
        PropInfo propInfo = PropInfo.create(pd);
        assertEquals(false,propInfo.isOptional());
        assertTrue(Map.class.isAssignableFrom(propInfo.getPropertyClass()));
    }

    @Test
    public void testLong() {
        PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(ExampleItem.class,"timeTaken");
        PropInfo propInfo = PropInfo.create(pd);
        assertEquals(false,propInfo.isOptional());
        assertEquals(Long.TYPE,propInfo.getPropertyClass());
        FieldType type = FieldType.fromClass(propInfo.getPropertyClass());
        assertEquals(FieldType.INTEGER,type);
    }

    @Test
    public void testOptionalInteger() {
        PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(ExampleItem.class,"optionalInteger");
        PropInfo propInfo = PropInfo.create(pd);
        assertEquals(true,propInfo.isOptional());
        assertEquals(Integer.class,propInfo.getPropertyClass());
        FieldType type = FieldType.fromClass(propInfo.getPropertyClass());
        assertEquals(FieldType.INTEGER,type);
    }

    @Test
    public void testOptionalLong() {
        PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(ExampleItem.class,"optionalLong");
        PropInfo propInfo = PropInfo.create(pd);
        assertEquals(true,propInfo.isOptional());
        assertEquals(Long.class,propInfo.getPropertyClass());
        FieldType type = FieldType.fromClass(propInfo.getPropertyClass());
        assertEquals(FieldType.INTEGER,type);
    }

}
