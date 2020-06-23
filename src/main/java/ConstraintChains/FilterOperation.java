package ConstraintChains;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

import QueryInstantiation.Parameter;
import Schema.Attribute;
import parsii.eval.Expression;
import parsii.eval.Scope;
import parsii.eval.Variable;
import parsii.eval.Parser;
import parsii.tokenizer.ParseException;

public class FilterOperation implements Serializable {

    private static final long serialVersionUID = 1L;

    private int id;

    private String expression = null;
    private String operator = null;
    private float probability;

    public FilterOperation(int id, String expression, String operator, float probability) {
        super();
        this.id = id;
        this.expression = expression;
        this.operator = operator;
        this.probability = probability;
    }

    public FilterOperation(FilterOperation filterOperation) {
        super();
        this.id = filterOperation.id;
        this.expression = filterOperation.expression;
        this.operator = filterOperation.operator;
        this.probability = filterOperation.probability;
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

    public float getProbability() {
        return probability;
    }

    private transient Expression exp = null;

    private transient List<Variable> variables = null;

    private transient List<String> attrNames = null;

    private transient List<String> values = null;

    public void initParsii(Parameter para, Map<String, Attribute> attributeMap) {
        if (attributeMap.containsKey(expression) && attributeMap.get(expression).getDataType().equals("varchar")) {
            values = para.getValues();
            return;
        }

        Scope scope = new Scope();
        variables = new ArrayList<Variable>();
        attrNames = new ArrayList<String>();

        String[] arr = expression.split("[\\+\\-\\*/\\^\\(\\)]");
        Set<String> set = new HashSet<String>();
        for (int i = 0; i < arr.length; i++) {
            if (!arr[i].matches("[\\d\\.]*")) {
                if (set.contains(arr[i])) {
                    continue;
                }
                else {
                    attrNames.add(arr[i]);
                    set.add(arr[i]);

                    String tmp = arr[i].replaceAll("[\\._]", "");
                    expression = expression.replaceAll(arr[i], tmp);
                    variables.add(scope.create(tmp));
                }
            }
        }
        if (operator.equals("bet")) {
            expression = expression + ">=" + new BigDecimal(para.getValues().get(0)).toPlainString() + "&&" +
                    expression + "<" + new BigDecimal(para.getValues().get(1)).toPlainString();
        }
        else if (operator.matches("in\\([0-9]+\\)")) {
            String tmp = "";
            for (int i = 0; i < para.getValues().size(); i++) {
                tmp = tmp + expression + "=" + new BigDecimal(para.getValues().get(i)).toPlainString();
                if (i != para.getValues().size() - 1) {
                    tmp = tmp + "||";
                }
            }
            expression = tmp;
        }
        else {
            expression = expression + operator + new BigDecimal(para.getValues().get(0)).toPlainString();
        }

        try {
            exp = Parser.parse(expression, scope);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public boolean isSatisfied(Map<String, String> attributeValueMap) {
        if (values != null) {
            String geneValue = attributeValueMap.get(expression);
            if (operator.equals("like")) {
                if (geneValue.contains(values.get(0))) {
                    return true;
                }
                else {
                    return false;
                }
            }
            else {
                for (int i = 0; i < values.size(); i++) {
                    if (geneValue.equals(values.get(i))) {
                        return true;
                    }
                }
                return false;
            }
        }

        for (int i = 0; i < attrNames.size(); i++) {
            String value = attributeValueMap.get(attrNames.get(i));
            // if any attribute of this expression is null, the value of expression is null
            if (value.equals("null")) {
                return false;
            }
            variables.get(i).setValue(new Double(value));
        }
        return exp.evaluate() == 0 ? false : true;
    }

    @Override
    public String toString() {
        return "\n\t\tFilterOperation [id=" + id + ", expression=" + expression + ", operator=" + operator
                + ", probability=" + probability + "]";
    }

}
