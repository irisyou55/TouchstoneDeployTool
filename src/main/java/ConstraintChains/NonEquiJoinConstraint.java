package ConstraintChains;

import java.util.ArrayList;
import java.util.List;

public class NonEquiJoinConstraint {

    private int id;

    private String expression = null;
    private String operator = null;

    private float probability;

    private float inputDataSize;

    private List<Integer> children = null;

    public NonEquiJoinConstraint(int id, String expression, String operator, float probability, float inputDataSize) {
        this.id = id;
        this.expression = expression;
        this.operator = operator;
        this.probability = probability;
        this.inputDataSize = inputDataSize;
        this.children = new ArrayList<Integer>();
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

    public float getInputDataSize() {
        return inputDataSize;
    }

    public List<Integer> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return "\n\tNonEquiJoinConstraint [id=" + id + ", expression=" + expression + ", operator=" + operator
                + ", probability=" + probability + ", inputDataSize=" + inputDataSize + ", children=" + children + "]";
    }
}
