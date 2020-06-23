package ConstraintChains;

import java.util.Arrays;
import java.util.List;

public class FKJoinAdjustment {

    private int order;

    private boolean[] joinStatuses = null;

    private List<FKJoinAdjustRule> rules = null;

    private float probability;

    public FKJoinAdjustment(int order, boolean[] joinStatuses, List<FKJoinAdjustRule> rules, float probability) {
        super();
        this.order = order;
        this.joinStatuses = joinStatuses;
        this.rules = rules;
        this.probability = probability;
    }

    public List<FKJoinAdjustRule> getRules() {
        return rules;
    }

    public float getProbability() {
        return probability;
    }

    public boolean canJoin() {
        loop : for (int i = 0; i < rules.size(); i++) {
            boolean[] cause = rules.get(i).getCause();
            for (int j = 0; j < cause.length; j++) {
                if (cause[j] != joinStatuses[j]) {
                    continue loop;
                }
            }
            return joinStatuses[order] = rules.get(i).getEffect();
        }

        if (Math.random() < probability) {
            return joinStatuses[order] = true;
        } else {
            return joinStatuses[order] = false;
        }
    }
    @Override
    public String toString() {
        return "FKJoinAdjustment [order=" + order + ", joinStatuses=" + Arrays.toString(joinStatuses) +
                ", rules=" + rules + ", probability=" + probability + "]";
    }
}
