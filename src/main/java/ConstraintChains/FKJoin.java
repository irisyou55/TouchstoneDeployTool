package ConstraintChains;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class FKJoin implements Serializable {

    private static final long serialVersionUID = 1L;

    private String[] foreignKeys = null;
    private float probability;
    private String[] primakryKeys = null;
    private int canJoinNum;
    private int cantJoinNum;

    private String fkStr = null;
    private String rpkStr = null;

    private transient FKJoinAdjustment fkJoinAdjustment = null;

    private transient float accumulativeProbability;

    public FKJoin(String[] foreignKeys, float probability, String[] primakryKeys, int canJoinNum, int cantJoinNum) {
        super();
        this.foreignKeys = foreignKeys;
        this.probability = probability;
        this.primakryKeys = primakryKeys;
        this.canJoinNum = canJoinNum;
        this.cantJoinNum = cantJoinNum;
        fkStr = Arrays.toString(this.foreignKeys);
        rpkStr = Arrays.toString(this.primakryKeys);
    }

    public FKJoin(FKJoin fkJoin) {
        super();
        this.foreignKeys = Arrays.copyOf(fkJoin.foreignKeys, fkJoin.foreignKeys.length);
        this.probability = fkJoin.probability;
        this.primakryKeys = Arrays.copyOf(fkJoin.primakryKeys, fkJoin.primakryKeys.length);
        this.canJoinNum = fkJoin.canJoinNum;
        this.cantJoinNum = fkJoin.cantJoinNum;
        this.fkStr = fkJoin.fkStr;
        this.rpkStr = fkJoin.rpkStr;
    }

    public void setFkJoinAdjustment(FKJoinAdjustment fkJoinAdjustment) {
        this.fkJoinAdjustment = fkJoinAdjustment;
    }

    public void setAccumulativeProbability(float accumulativeProbability) {
        this.accumulativeProbability = accumulativeProbability;
    }

    public String[] getForeignKeys() {
        return foreignKeys;
    }

    public float getProbability() {
        return probability;
    }

    public String[] getPrimakryKeys() {
        return primakryKeys;
    }

    public int getCanJoinNum() {
        return canJoinNum;
    }

    public int getCantJoinNum() {
        return cantJoinNum;
    }

    public String getFkStr() {
        return fkStr;
    }

    public String getRpkStr() {
        return rpkStr;
    }

    public FKJoinAdjustment getFkJoinAdjustment() {
        return fkJoinAdjustment;
    }

    public float getAccumulativeProbability() {
        return accumulativeProbability;
    }

    public boolean canJoin() {
        return fkJoinAdjustment.canJoin();
    }

    @Override
    public String toString() {
        return "\n\tFKJoin [foreignKeys=" + Arrays.toString(foreignKeys) + ", probability=" + probability
                + ", primakryKeys=" + Arrays.toString(primakryKeys) + ", canJoinNum=" + canJoinNum
                + ", cantJoinNum=" + cantJoinNum + ", fkStr=" + fkStr + ", rpkStr=" + rpkStr  + "]";
    }
}
