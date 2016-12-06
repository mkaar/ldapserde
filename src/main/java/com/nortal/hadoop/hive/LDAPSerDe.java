package com.nortal.hadoop.hive;

import org.apache.directory.shared.ldap.ldif.LdifEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

import java.util.*;

/**
 * Created by Marko Kaar.
 */

public class LDAPSerDe implements Deserializer {

    ObjectInspector inspector;
    ArrayList<Object> row;
    int numColumns;
    List<String> columnNames;

    @Override
    public void initialize(Configuration cfg, Properties props) throws SerDeException {
        String columnNameProperty = props.getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(columnNameProperty.split(","));
        numColumns = columnNames.size();

        String columnTypeProperty = props.getProperty(Constants.LIST_COLUMN_TYPES);
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

        // Ensure we have the same number of column names and types
        assert numColumns == columnTypes.size();

        List<ObjectInspector> inspectors = new ArrayList<>(numColumns);
        row = new ArrayList<>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            inspectors.add(oi);
            row.add(null);
        }
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }


    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        ObjectWritable obj = (ObjectWritable) writable;
        LdifEntry entry = (LdifEntry) obj.get();
        for (int i = 0; i < numColumns; i++) {
            String columName = columnNames.get(i);
            Object value = entry.get(columName);
            row.set(i, value);
        }
        return row;
    }


    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }


}
