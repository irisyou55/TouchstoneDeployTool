package QueryInstantiation;

import Schema.Attribute;

import java.util.*;
import java.util.stream.Collectors;

public class ComputingTask {

    private int id;
    private String expression = null;
    private String operator = null;
    private List<String> attrNames = null;
    private List<Attribute> attributes = null;
    private float probability;
    private float inputDataSize;
    private boolean isBet;

    private List<String> childrensConstraints = null;
    private List<Integer> children = null;

    public ComputingTask(int id, String expression, String operator, float probability,
                         Map<String, Attribute> attributeMap, String tableName, float inputDataSize, boolean isBet) {
        this.id = id;
        this.operator = operator;
        this.probability = probability;
        this.inputDataSize = inputDataSize;
        this.isBet = isBet;

        attrNames = new ArrayList<String>();
        attributes = new ArrayList<Attribute>();
        String[] arr = expression.split("[\\+\\-\\*/\\^\\(\\)]");
        Set<String> set = new HashSet<String>();

        for (int i = 0; i < arr.length; i++){
            if (!arr[i].matches("[\\d\\.]*")) {
                if (set.contains(arr[i])){
                    continue;
                }
                else{
                    set.add(arr[i]);
                    String tmp = arr[i].replaceAll("[\\._]", "");
                    attrNames.add(tmp);
                    expression = expression.replaceAll(arr[i], tmp);
                    attributes.add(attributeMap.get(tableName + "." + arr[i]));
                }
            }
        }
        this.expression = expression;
    }
    public ComputingTask(int id, String expression, String operator, float probability, List<Integer> children,
                         Map<String, Attribute> attributeMap, Map<Integer, ComputingTask> taskMap, float inputDataSize, boolean isBet){
        this.id = id;
        this.operator = operator;
        this.probability = probability;
        this.children = children;
        this.inputDataSize = inputDataSize;
        this.isBet = isBet;
        childrensConstraints = new ArrayList<String>();

        attrNames = new ArrayList<String>();
        attributes = new ArrayList<Attribute>();
        String[] arr = expression.split("[\\+\\-\\*/\\^\\(\\)]");
        Set<String> set = new HashSet<String>();
        for (int i = 0; i < arr.length; i++) {
            if (!arr[i].matches("[\\d\\.]*")) {
                if (set.contains(arr[i])){
                    continue;
                }
                else{
                    set.add(arr[i]);
                    String tmp = arr[i].replaceAll("[\\._]", "");
                    attrNames.add(tmp);
                    expression = expression.replaceAll(arr[i], tmp);
                    attributes.add(attributeMap.get(arr[i]));
                }
            }
        }
        this.expression = expression;
        set = set.stream().map(x -> x.replaceAll("[\\._]", "")).collect(Collectors.toSet());
        for (int i = 0; i < children.size(); i++){
            List<String> childsAttrNames = taskMap.get(children.get(i)).getAttrNames();
            List<Attribute> childsAttributes = taskMap.get(children.get(i)).getAttributes();
            for (int j = 0; j < childsAttrNames.size(); j++) {
                if (!set.contains(childsAttrNames.get(j))) {
                    attrNames.add(childsAttrNames.get(j));
                    attributes.add(childsAttributes.get(j));
                    set.add(childsAttrNames.get(j));
                }
            }
        }
    }

    public int getId() {
        return id;
    }

    public String getExpression() {
        return expression;
    }

    public String getOperator() {
        return operator;
    }

    public List<String> getAttrNames() {
        return attrNames;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public float getProbability() {
        return probability;
    }

    public float getInputDataSize() {
        return inputDataSize;
    }

    public boolean isBet() {
        return isBet;
    }

    public List<String> getChildrensConstraints() {
        return childrensConstraints;
    }

    public List<Integer> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return "\n\tComputingTask [id=" + id + ", expression=" + expression + ", operator=" + operator + ", attrNames="
                + attrNames + ", attributes=" + attributes + ", probability=" + probability + ", inputDataSize="
                + inputDataSize + ", childrensConstraints=" + childrensConstraints + ", children=" + children + "]";
    }

}
