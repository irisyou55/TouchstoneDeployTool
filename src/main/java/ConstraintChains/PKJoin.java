package ConstraintChains;

import java.io.Serializable;
import java.util.Arrays;

public class PKJoin implements Serializable {

    private static final long serialVersionUID = 1L;

    private String[] primakryKeys = null;

    private int[] canJoinNum = null;
    private int[] cantJoinNum = null;

    private String pkStr = null;

    public PKJoin(String[] primakryKeys, int[] canJoinNum, int[] cantJoinNum) {
        super();
        this.primakryKeys = primakryKeys;
        this.canJoinNum = canJoinNum;
        this.cantJoinNum = cantJoinNum;
        pkStr = Arrays.toString(this.primakryKeys);
    }

    public PKJoin(PKJoin pkJoin) {
        super();
        this.primakryKeys = Arrays.copyOf(pkJoin.primakryKeys, pkJoin.primakryKeys.length);
        this.canJoinNum = Arrays.copyOf(pkJoin.canJoinNum, pkJoin.canJoinNum.length);
        this.cantJoinNum = Arrays.copyOf(pkJoin.cantJoinNum, pkJoin.cantJoinNum.length);
        this.pkStr = pkJoin.pkStr;
    }

    public String[] getPrimakryKeys() {
        return primakryKeys;
    }

    public int[] getCanJoinNum() {
        return canJoinNum;
    }

    public int[] getCantJoinNum() {
        return cantJoinNum;
    }

    public String getPkStr() {
        return pkStr;
    }

    @Override
    public String toString() {
        return "\n\tPKJoin [primakryKeys=" + Arrays.toString(primakryKeys) + ", canJoinNum=" + Arrays.toString(canJoinNum)
                + ", cantJoinNum=" + Arrays.toString(cantJoinNum) + ", pkStr=" + pkStr + "]";
    }
}
