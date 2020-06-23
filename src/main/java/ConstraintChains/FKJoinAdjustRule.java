package ConstraintChains;

import java.util.Arrays;

public class FKJoinAdjustRule implements Comparable<FKJoinAdjustRule> {

    private boolean[] cause = null;
    private boolean effect;

    public FKJoinAdjustRule(boolean[] joinStatuses) {
        super();
        cause = Arrays.copyOf(joinStatuses, joinStatuses.length - 1);
        effect = !joinStatuses[joinStatuses.length - 1];
    }

    public boolean[] getCause() {
        return cause;
    }

    public boolean getEffect() {
        return effect;
    }

    @Override
    public String toString() {
        return "FKJoinAdjustRule [cause=" + Arrays.toString(cause) + ", effect=" + effect + "]";
    }

    @Override
    public int compareTo(FKJoinAdjustRule other) {
        return Arrays.toString(other.cause).compareTo(Arrays.toString(this.cause));
    }
}
