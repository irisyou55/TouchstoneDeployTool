package ConstraintChains;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public class Filter implements Serializable {

    private static final long serialVersionUID = 1L;

    private FilterOperation[] filterOperations = null;

    private int logicalRelation;

    private float probability;

    public Filter(FilterOperation[] filterOperations, int logicalRelation, float probability) {
        super();
        this.filterOperations = filterOperations;
        this.logicalRelation = logicalRelation;
        this.probability = probability;
    }

    public Filter(Filter filter) {
        super();
        this.filterOperations = new FilterOperation[filter.filterOperations.length];
        for (int i = 0; i < filter.filterOperations.length; i++) {
            this.filterOperations[i] = new FilterOperation(filter.filterOperations[i]);
        }
        this.logicalRelation = filter.logicalRelation;
        this.probability = filter.probability;
    }

    public boolean isSatisfied(Map<String, String> attributeValueMap) {
        boolean res = false;
        if (logicalRelation == -1) {
            res = filterOperations[0].isSatisfied(attributeValueMap);
        }
        else if (logicalRelation == 0) {
            res = true;
            for (int i = 0; i < filterOperations.length; i++) {
                if (!filterOperations[i].isSatisfied(attributeValueMap)) {
                    res = false;
                    break;
                }
            }
        }
        else if (logicalRelation == 1) {
            res = false;
            for (int i = 0; i < filterOperations.length; i++) {
                if (filterOperations[i].isSatisfied(attributeValueMap)) {
                    res = true;
                    break;
                }
            }
        }
        return res;
    }

    public FilterOperation[] getFilterOperations() {
        return filterOperations;
    }

    public int getLogicalRelation() {
        return logicalRelation;
    }

    public float getProbability() {
        return probability;
    }

    @Override
    public String toString() {
        return "\n\tFilter [filterOperations=" + Arrays.toString(filterOperations) + ", \n\t\tlogicalRelation="
                + logicalRelation + ", probability=" + probability + "]";
    }
}
