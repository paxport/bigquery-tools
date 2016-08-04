package com.cloudburst.bigquery;

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
}
