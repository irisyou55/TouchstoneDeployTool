package Schema;

import DataType.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.QueryInstantiator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class SchemaReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInstantiator.class);

    public SchemaReader() {

    }

    public static void main(String[] args) {

        SchemaReader schemaReader = new SchemaReader();
        schemaReader.read("src/test/input/ssb_schema_sf_1_D.txt");
    }

    public List<Table> read(String databaseSchemaInput) {
        List<String> tableInfos = new ArrayList<String>();
        List<String> dataInfos = new ArrayList<String>();

        String inputLine = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(databaseSchemaInput)))){
            while ((inputLine = br.readLine()) != null){
                if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
                    continue;
                }

                inputLine = inputLine.toLowerCase();
                inputLine = inputLine.replaceAll("[ \\t]+", "");

                if (inputLine.matches("[ ]*t[ ]*\\[[\\s\\S^\\]]+\\][ ]*")) {
                    tableInfos.add(inputLine);
                }
                else if (inputLine.matches("[ ]*d[ ]*\\[[\\s\\S^\\]]+\\][ ]*")) {
                    dataInfos.add(inputLine);
                }
                else {
                    LOGGER.error("\n\tUnrecognized input information: " + inputLine);
                    System.exit(0);
                }
            }
        }
        catch (Exception e) {
            LOGGER.error("\n\tError input line: " + inputLine);
            e.printStackTrace();
            System.exit(0);
        }

        Map<String, String> attrDataInfoMap = new HashMap<String, String>();
        for (int i = 0; i < dataInfos.size(); i++) {
            String tmp = dataInfos.get(i);
            tmp = tmp.substring(tmp.indexOf('[') + 1, tmp.indexOf(']'));
            String[] arr = tmp.split(";", 2);
            attrDataInfoMap.put(arr[0], arr[1]);
        }

        List<Table> tables = new ArrayList<Table>();
        for (int i = 0; i < tableInfos.size(); i++){
            String tmp = tableInfos.get(i);
            tmp = tmp.substring(tmp.indexOf('[') + 1, tmp.indexOf(']'));
            String[] tableInfoArr = tmp.split(";", -1);

            String tableName = tableInfoArr[0];
            long tableSize = Long.parseLong(tableInfoArr[1]);
            List<String> primaryKey = new ArrayList<String>();
            List<ForeignKey> foreignKeys = new ArrayList<ForeignKey>();
            List<Attribute> attributes = new ArrayList<Attribute>();

            Set<String> keys = new HashSet<String>();

            for (int j = 2; j < tableInfoArr.length; j++) {
                if (tableInfoArr[j].matches("p\\([\\s\\S^\\]]+\\)")) {
                    String primaryKeyInfo = tableInfoArr[j].substring(tableInfoArr[j].indexOf("(") + 1,
                            tableInfoArr[j].indexOf(")"));
                    primaryKey.addAll(Arrays.asList(primaryKeyInfo.split(",")));
                    keys.addAll(primaryKey);
                    // add the prefix of table name
                    for (int k = 0; k < primaryKey.size(); k++) {
                        primaryKey.set(k, tableName + "." + primaryKey.get(k));
                    }
                }
                else if (tableInfoArr[j].matches("f\\([\\s\\S^\\]]+\\)")) {
                    String[] foreignKeyInfoArr = tableInfoArr[j].substring(tableInfoArr[j].indexOf("(") + 1,
                            tableInfoArr[j].indexOf(")")).split(",");
                    foreignKeys.add(new ForeignKey(foreignKeyInfoArr[0], foreignKeyInfoArr[1]));
                    keys.add(foreignKeyInfoArr[0]);
                }
            }
            for (int j = 2; j < tableInfoArr.length; j++) {
                if (!tableInfoArr[j].matches("p\\([\\s\\S^\\]]+\\)") &&
                        !tableInfoArr[j].matches("f\\([\\s\\S^\\]]+\\)")) {
                    if (tableInfoArr[j].split(",").length != 2) {
                        LOGGER.error("\n\tExpect to have a comma! " + "Error input: " + tableInfoArr[j]);
                    }
                    String attrName = tableInfoArr[j].split(",")[0];
                    String dataType = tableInfoArr[j].split(",")[1];
                    if (keys.contains(attrName)) {
                        if (dataType.equals("integer")) {
                            continue;
                        }
                        else {
                            LOGGER.error("\n\tThe data type of primary key and foreign key must be integer! "
                                    + "Error input: " + tableInfoArr[j]);
                            System.exit(0);
                        }
                    }
                    String dataInfo = attrDataInfoMap.get(tableName + "." + attrName);
                    TSDataTypeInfo dataTypeInfo = null;
                    try {
                        dataTypeInfo = newTSDataType(dataType, dataInfo);
                    } catch (Exception e) {
                        LOGGER.error("\n\tThe basic data characteristic information can not be recognized! "
                                + "Error input: " + tableInfoArr[j] + ", " + dataInfo);
                        e.printStackTrace();
                        System.exit(0);
                    }
                    attributes.add(new Attribute(attrName, dataType, dataTypeInfo));
                }
            }
            tables.add(new Table(tableName, tableSize, primaryKey, foreignKeys, attributes));
        }
//        logger.debug("\nThe schema of database instance (include the basic data characteristic): " + tables);
        return tables;
    }
    private TSDataTypeInfo newTSDataType(String dataType, String dataInfo){
        TSDataTypeInfo dataTypeInfo = null;
        String[] arr = null;
        if (dataInfo != null) {
            arr = dataInfo.split(";");
        }
        switch (dataType) {
            case "integer":
                if (dataInfo != null) {
                    dataTypeInfo = new TSInteger(Float.parseFloat(arr[0]), Long.parseLong(arr[1]),
                            Long.parseLong(arr[2]), Long.parseLong(arr[3]));
                } else {
                    dataTypeInfo = new TSInteger();
                }
                break;
            case "real":
                if (dataInfo != null) {
                    dataTypeInfo = new TSReal(Float.parseFloat(arr[0]),
                            Double.parseDouble(arr[1]), Double.parseDouble(arr[2]));
                } else {
                    dataTypeInfo = new TSReal();
                }
                break;
            case "decimal":
                if (dataInfo != null) {
                    dataTypeInfo = new TSDecimal(Float.parseFloat(arr[0]),
                            Double.parseDouble(arr[1]), Double.parseDouble(arr[2]));
                } else {
                    dataTypeInfo = new TSDecimal();
                }
                break;
            case "date":
                if (dataInfo != null) {
                    dataTypeInfo = new TSDate(Float.parseFloat(arr[0]), arr[1], arr[2]);
                } else {
                    dataTypeInfo = new TSDate();
                }
                break;
            case "datetime":
                if (dataInfo != null) {
                    dataTypeInfo = new TSDateTime(Float.parseFloat(arr[0]), arr[1], arr[2]);
                } else {
                    dataTypeInfo = new TSDateTime();
                }
                break;
            case "varchar":
                if (dataInfo != null) {
                    dataTypeInfo = new TSVarchar(Float.parseFloat(arr[0]),
                            Float.parseFloat(arr[1]), Integer.parseInt(arr[2]));
                } else {
                    dataTypeInfo = new TSVarchar();
                }
                break;
            case "bool":
                if (dataInfo != null) {
                    if (arr.length == 1) {
                        dataTypeInfo = new TSBool(Float.parseFloat(arr[0]));
                    } else if (arr.length == 2) {
                        dataTypeInfo = new TSBool(Float.parseFloat(arr[0]), Float.parseFloat(arr[1]));
                    }
                } else {
                    dataTypeInfo = new TSBool();
                }
                break;
            default:
                LOGGER.error("\n\tUnrecognized data type: " + dataType);
        }
        return dataTypeInfo;
    }

}
