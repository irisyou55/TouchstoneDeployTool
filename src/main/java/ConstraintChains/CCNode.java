package ConstraintChains;

import java.io.Serializable;

public class CCNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private int type;

    private Object node = null;

    public CCNode(int type, Object node) {
        super();
        this.type = type;
        this.node = node;
    }

    public CCNode(CCNode ccNode) {
        super();
        this.type = ccNode.type;
        switch (this.type) {
            case 0:
                this.node = new Filter((Filter)ccNode.node);
                break;
            case 1:
                this.node = new PKJoin((PKJoin)ccNode.node);
                break;
            case 2:
                this.node = new FKJoin((FKJoin)ccNode.node);
                break;
        }
    }

    public int getType() {
        return type;
    }

    public Object getNode() {
        return node;
    }

    @Override
    public String toString() {
        return node.toString();
    }
}
