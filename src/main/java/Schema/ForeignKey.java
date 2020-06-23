package Schema;

public class ForeignKey {

    private String attrName = null;

    private String referencedKey = null;

    public ForeignKey(String attrName, String referencedKey) {
        super();
        this.attrName = attrName;
        this.referencedKey = referencedKey;
    }

    public String getAttrName() {
        return attrName;
    }

    public String getReferencedKey() {
        return referencedKey;
    }

    @Override
    public String toString() {
        return "ForeignKey [attrName=" + attrName + ", referencedKey=" + referencedKey + "]";
    }
}
