package Pretreatment;

import ConstraintChains.*;
import QueryInstantiation.Parameter;
import QueryInstantiation.QueryInstantiator;
import Schema.Attribute;
import Schema.ForeignKey;
import Schema.SchemaReader;
import Schema.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class Preprocessor {
    private List<Table> tables = null;
    private List<ConstraintChain> constraintChains = null;
    private List<Parameter> parameters = null;
    private Logger logger = null;

    public Preprocessor(List<Table> tables, List<ConstraintChain> constraintChains, List<Parameter> parameters) {
        super();
        this.tables = tables;
        this.constraintChains = constraintChains;
        this.parameters = parameters;
    }

    public List<String> getPartialOrder() {
        Set<String> allTables = new HashSet<String>();

        Set<String> nonMetaTables = new HashSet<String>();

        Map<String, ArrayList<String>> tableDependencyInfo = new HashMap<String, ArrayList<String>>();
        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            allTables.add(table.getTableName());
            if (table.getForeignKeys().size() != 0) {
                nonMetaTables.add(table.getTableName());
                List<ForeignKey> foreignKeys = table.getForeignKeys();
                ArrayList<String> referencedTables = new ArrayList<String>();
                for (int j = 0; j < foreignKeys.size(); j++) {
                    referencedTables.add(foreignKeys.get(j).getReferencedKey().split("\\.")[0]);
                }
                tableDependencyInfo.put(table.getTableName(), referencedTables);
            }
        }

        allTables.removeAll(nonMetaTables);
        Set<String> partialOrder = new LinkedHashSet<String>();
        partialOrder.addAll(allTables);
        Iterator<Map.Entry<String, ArrayList<String>>> iterator = tableDependencyInfo.entrySet().iterator();
        while (true) {
            while (iterator.hasNext()) {
                Map.Entry<String, ArrayList<String>> entry = iterator.next();
                if (partialOrder.containsAll(entry.getValue())) {
                    partialOrder.add(entry.getKey());
                }
            }
            if (partialOrder.size() == tables.size()) {
                break;
            }
            iterator = tableDependencyInfo.entrySet().iterator();
        }

        logger.debug("\nThe partial order of tables: \n\t" + partialOrder);
        return partialOrder.stream().collect(Collectors.toList());
    }
    public Map<String, TableGeneTemplate> getTableGeneTemplates(int shuffleMaxNum, int pkvsMaxSize){
        Map<Integer, Parameter> parameterMap = new HashMap<Integer, Parameter>();
        for (int j = 0; j < parameters.size(); j++) {
            parameterMap.put(parameters.get(j).getId(), parameters.get(j));
        }

        Map<String, TableGeneTemplate> tableGeneTemplateMap = new HashMap<String, TableGeneTemplate>();

        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            String tableName = table.getTableName();
            long tableSize = table.getTableSize();
            String pkStr = table.getPrimaryKey().toString();
            List<Key> keys = new ArrayList<Key>();
            List<Attribute> attributes = table.getAttributes();
            List<ConstraintChain> tableConstraintChains = new ArrayList<ConstraintChain>();
            List<String> referencedKeys = new ArrayList<String>();
            Map<String, String> referKeyForeKeyMap = new HashMap<String, String>();
            Map<Integer, Parameter> localParameterMap = new HashMap<Integer, Parameter>();
            Map<String, Attribute> attributeMap = new HashMap<String, Attribute>();

            List<String> primaryKey = table.getPrimaryKey();
            List<ForeignKey> foreignKeys = table.getForeignKeys();
            loop : for (int j = 0; j < primaryKey.size(); j++) {
                for (int k = 0; k < foreignKeys.size(); k++) {
                    if (foreignKeys.get(k).getAttrName().equals(primaryKey.get(j).split("\\.")[1])) {
                        continue loop;
                    }
                }
                keys.add(new Key(primaryKey.get(j), 0));
            }

            if (keys.size() == 0) {
                keys.add(new Key("unique_number", 0));
            }
            for (int j = 0; j < foreignKeys.size(); j++) {
                keys.add(new Key(tableName + "." + foreignKeys.get(j).getAttrName(), 1));
            }

            for (int j = 0; j < constraintChains.size(); j++) {
                if (constraintChains.get(j).getTableName().equals(tableName)) {
                    tableConstraintChains.add(constraintChains.get(j));
                }
            }

            foreignKeys.sort((x, y) -> x.getReferencedKey().compareTo(y.getReferencedKey()));
            for (int index = 0, j = 0; j < foreignKeys.size(); j++) {
                if ((j < foreignKeys.size() - 1)) {
                    if (foreignKeys.get(j).getReferencedKey().split("\\.")[0].equals(
                            foreignKeys.get(j + 1).getReferencedKey().split("\\.")[0])) {
                        continue;
                    }
                }
                String fksStr = "[";
                for (int k = index; k <= j; k++) {
                    fksStr = fksStr + foreignKeys.get(k).getReferencedKey();
                    if (k != j) {
                        fksStr = fksStr + ", ";
                    }
                }
                fksStr = fksStr + "]";
                referencedKeys.add(fksStr);
                index = j + 1;
            }

            for (int j = 0; j < foreignKeys.size(); j++) {
                referKeyForeKeyMap.put(foreignKeys.get(j).getReferencedKey(),
                        tableName + "." + foreignKeys.get(j).getAttrName());
            }

            for (int j = 0; j < tableConstraintChains.size(); j++) {
                List<CCNode> nodes = tableConstraintChains.get(j).getNodes();
                for (int k = 0; k < nodes.size(); k++) {
                    if (nodes.get(k).getType() == 0) {
                        Filter filter = (Filter)nodes.get(k).getNode();
                        FilterOperation[] operations = filter.getFilterOperations();
                        for (int l = 0; l < operations.length; l++) {
                            localParameterMap.put(operations[l].getId(), parameterMap.get(operations[l].getId()));
                        }
                    }
                }
            }

            for (int j = 0; j < attributes.size(); j++) {
                attributeMap.put(attributes.get(j).getAttrName(), attributes.get(j));
            }

            TableGeneTemplate tableGeneTemplate = new TableGeneTemplate(tableName, tableSize, pkStr,
                    keys, attributes, tableConstraintChains, referencedKeys, referKeyForeKeyMap,
                    localParameterMap, attributeMap, shuffleMaxNum, pkvsMaxSize);
            tableGeneTemplateMap.put(tableName, tableGeneTemplate);
        }

        logger.debug("\nThe generation template map of tables: \n" + tableGeneTemplateMap);
        return tableGeneTemplateMap;
    }

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(".//test//lib//log4j.properties");
        System.setProperty("com.wolfram.jlink.libdir",
                "C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink");

        SchemaReader schemaReader = new SchemaReader();
        List<Table> tables = schemaReader.read(".//test//input//tpch_schema_sf_1.txt");
        ConstraintChainReader constraintChainReader = new ConstraintChainReader();
        List<ConstraintChain> constraintChains = constraintChainReader.read(".//test//input//tpch_cardinality_constraints_sf_1.txt");
        ComputingThreadPool computingThreadPool = new ComputingThreadPool(4, 20, 0.00001);
        QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
        queryInstantiator.iterate();
        List<Parameter> parameters = queryInstantiator.getParameters();

        Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
        preprocessor.getPartialOrder();
        Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(1000, 10000);

        TableGeneTemplate template = tableGeneTemplateMap.entrySet().iterator().next().getValue();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(".//data//template"));
        oos.writeObject(template);
        oos.close();
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(".//data//template"));
        TableGeneTemplate template2 = (TableGeneTemplate)ois.readObject();
        ois.close();
        System.out.println("-----------------------");
        System.out.println(template);
        System.out.println(template2);
    }
}
