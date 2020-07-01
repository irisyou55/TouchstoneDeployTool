package Pretreatment;

import ConstraintChains.*;
import QueryInstantiation.Parameter;
import Schema.Attribute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.QueryInstantiator;
import run.Statistic;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableGeneTemplate implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableGeneTemplate.class);

    private static final long serialVersionUID = 1L;

    private String tableName = null;
    private long tableSize;

    private String pkStr = null;

    private List<Key> keys = null;

    private List<Attribute> attributes = null;

    private List<ConstraintChain> constraintChains = null;

    private List<String> referencedKeys = null;

    private Map<String, String> referKeyForeKeyMap = null;

    private Map<Integer, Parameter> parameterMap = null;
    private transient Map<String, Attribute> attributeMap = null;

    private int shuffleMaxNum;

    private int pkvsMaxSize;

    public TableGeneTemplate(String tableName, long tableSize, String pkStr, List<Key> keys, List<Attribute> attributes,
                             List<ConstraintChain> constraintChains, List<String> referencedKeys, Map<String, String> referKeyForeKeyMap,
                             Map<Integer, Parameter> parameterMap, Map<String, Attribute> attributeMap, int shuffleMaxNum,
                             int pkvsMaxSize) {
        super();
        this.tableName = tableName;
        this.tableSize = tableSize;
        this.pkStr = pkStr;
        this.keys = keys;
        this.attributes = attributes;
        this.constraintChains = constraintChains;
        this.referencedKeys = referencedKeys;
        this.referKeyForeKeyMap = referKeyForeKeyMap;
        this.parameterMap = parameterMap;
        this.attributeMap = attributeMap;
        this.shuffleMaxNum = shuffleMaxNum;
        this.pkvsMaxSize = pkvsMaxSize;
    }

    private Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo = null;

    public void setFksJoinInfo(Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo) {
        this.fksJoinInfo = fksJoinInfo;
    }

    private transient Map<String, ArrayList<JoinStatusesSizePair>> fksJoinInfoSizeMap = null;

    private transient List<JoinStatusesSizePair> satisfiedFkJoinInfo = null;

    private transient Map<Integer, ArrayList<long[]>> pkJoinInfo = null;

    private transient Map<Integer, Long> pkJoinInfoSizeMap = null;

    private transient String[] pkStrArr = null;

    private transient Map<String, String[]> rpkStrToArray = null;

    private transient Map<String, String> attributeValueMap = null;

    private transient Map<String, Integer> fkJoinStatusesMap = null;

    private transient int pkJoinStatuses;

    private transient SimpleDateFormat dateSdf = null;
    private transient SimpleDateFormat dateTimeSdf = null;

    public void init() {
        LOGGER.debug("\n\tStart the initialization of table " + tableName);

        fksJoinInfoSizeMap = new HashMap<String, ArrayList<JoinStatusesSizePair>>();
        satisfiedFkJoinInfo = new ArrayList<JoinStatusesSizePair>();

        Iterator<Map.Entry<String, Map<Integer, ArrayList<long[]>>>> iterator =
                fksJoinInfo.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<Integer, ArrayList<long[]>>> entry = iterator.next();
            fksJoinInfoSizeMap.put(entry.getKey(), new ArrayList<JoinStatusesSizePair>());
            Iterator<Map.Entry<Integer, ArrayList<long[]>>> iterator2 = entry.getValue().entrySet().iterator();
            while (iterator2.hasNext()) {
                Map.Entry<Integer, ArrayList<long[]>> entry2 = iterator2.next();
                fksJoinInfoSizeMap.get(entry.getKey()).add(
                        new JoinStatusesSizePair(entry2.getKey(), entry2.getValue().size()));
            }
        }
        LOGGER.debug("\nThe fksJoinInfoSizeMap is: " + fksJoinInfoSizeMap);

        pkJoinInfo = new HashMap<Integer, ArrayList<long[]>>();
        pkJoinInfoSizeMap = new HashMap<Integer, Long>();

        pkStrArr = pkStr.substring(1, pkStr.length() - 1).replaceAll(" ", "").split(",");

        rpkStrToArray = new HashMap<String, String[]>();
        for (int i = 0; i < referencedKeys.size(); i++) {
            String rpkStr = referencedKeys.get(i);
            String[] rpkStrArr = rpkStr.substring(1, rpkStr.length() - 1).replaceAll(" ", "").split(",");
            rpkStrToArray.put(rpkStr, rpkStrArr);
        }

        attributeValueMap = new HashMap<String, String>();
        fkJoinStatusesMap = new HashMap<String, Integer>();

        dateSdf = new SimpleDateFormat("yyyy-MM-dd");
        dateTimeSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (int i = 0; i < constraintChains.size(); i++) {
            float accumulativeProbability = 1;
            List<CCNode> nodes = constraintChains.get(i).getNodes();
            for (int j = 0; j < nodes.size(); j++) {
                int type = nodes.get(j).getType();
                switch (type) {
                    case 0:
                        accumulativeProbability *= ((Filter) nodes.get(j).getNode()).getProbability();
                        break;
                    case 1:
                        // 'PKJoin' node must be at the end of the constraint chain
                        break;
                    case 2:
                        FKJoin fkJoin = (FKJoin) nodes.get(j).getNode();
                        fkJoin.setAccumulativeProbability(accumulativeProbability);
                        accumulativeProbability *= fkJoin.getProbability();
                        break;
                }
            }
        }

        for (int i = 0; i < shuffleMaxNum; i++) {
            boolean isSuccessful = adjustFksGeneStrategy();
            if (isSuccessful) {
                break;
            } else {
                Collections.shuffle(constraintChains);
                LOGGER.debug("\n\tShuffle the constraint chains!, and the number of times is " + i);
            }
        }
        LOGGER.info("\n\t The number of rules in constraint chains:" + getRulesNum());

        // initialize the 'parsii' for all basic filter operations (FilterOperation)
        initParsii();
    }

    public TableGeneTemplate(TableGeneTemplate template) {
        super();
        this.tableName = template.tableName;
        this.tableSize = template.tableSize;
        this.pkStr = template.pkStr;
        this.keys = new ArrayList<Key>();
        for (int i = 0; i < template.keys.size(); i++) {
            this.keys.add(new Key(template.keys.get(i)));
        }
        this.attributes = new ArrayList<Attribute>();
        for (int i = 0; i < template.attributes.size(); i++) {
            this.attributes.add(new Attribute(template.attributes.get(i)));
        }
        this.constraintChains = new ArrayList<ConstraintChain>();
        for (int i = 0; i < template.constraintChains.size(); i++) {
            this.constraintChains.add(new ConstraintChain(template.constraintChains.get(i)));
        }
        this.referencedKeys = new ArrayList<String>();
        this.referencedKeys.addAll(template.referencedKeys);
        this.referKeyForeKeyMap = new HashMap<String, String>();
        this.referKeyForeKeyMap.putAll(template.referKeyForeKeyMap);
        this.parameterMap = new HashMap<Integer, Parameter>();
        Iterator<Map.Entry<Integer, Parameter>> iterator = template.parameterMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Parameter> entry = iterator.next();
            this.parameterMap.put(entry.getKey(), new Parameter(entry.getValue()));
        }
        this.attributeMap = new HashMap<String, Attribute>();
        for (int i = 0; i < this.attributes.size(); i++) {
            this.attributeMap.put(this.attributes.get(i).getAttrName(), this.attributes.get(i));
        }

        this.shuffleMaxNum = template.shuffleMaxNum;
        this.pkvsMaxSize = template.pkvsMaxSize;

        this.fksJoinInfo = template.fksJoinInfo;
        init();
    }

    public String[] geneTuple(long uniqueNum) {
        String[] tuple = new String[keys.size() + attributes.size()];
        attributeValueMap.clear();
        fkJoinStatusesMap.clear();
        pkJoinStatuses = 0;

        for (int i = 0; i < attributes.size(); i++) {
            tuple[keys.size() + i] = attributes.get(i).geneData();
            attributeValueMap.put(attributes.get(i).getAttrName(), tuple[keys.size() + i]);
        }

        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).getKeyType() == 0) {
                tuple[i] = uniqueNum + "";
                attributeValueMap.put(keys.get(i).getKeyName(), tuple[i]);
                // There is only one column with unique number in TPC-H,
                // but it's not for SSB (lineorder: lo_orderkey & lo_linenumber)
                // ------
                // break;
            }
        }

        for (int i = 0; i < constraintChains.size(); i++) {
            List<CCNode> nodes = constraintChains.get(i).getNodes();
            boolean flag = true;
            for (int j = 0; j < nodes.size(); j++) {
                int type = nodes.get(j).getType();
                switch (type) {
                    case 0:
                        // if the 'Filter' node is at the end of constraint chain, we can ignore it
                        if (j == nodes.size() - 1) {
                            continue;
                        }
                        Filter filter = (Filter) nodes.get(j).getNode();
                        if (!filter.isSatisfied(attributeValueMap)) {
                            // 'flag = false' indicates that the join statues of all following 'PKJoin's is false
                            //  and the data (tuple) can't flow to following 'FKJoin's
                            flag = false;
                        }
                        break;
                    case 1:
                        PKJoin pkJoin = (PKJoin) nodes.get(j).getNode();
                        // only one primary key -> only one variable (pkJoinStatuses)
                        if (flag) { // can join
                            for (int k = 0; k < pkJoin.getCanJoinNum().length; k++) {
                                pkJoinStatuses += pkJoin.getCanJoinNum()[k];
                            }
                        } else { // can't join
                            for (int k = 0; k < pkJoin.getCantJoinNum().length; k++) {
                                pkJoinStatuses += pkJoin.getCantJoinNum()[k];
                            }
                        }
                        break;
                    case 2:
                        // the tuple can flow to current node
                        if (flag) {
                            FKJoin fkJoin = (FKJoin) nodes.get(j).getNode();
                            int numCount = 0;
                            if (fkJoinStatusesMap.containsKey(fkJoin.getRpkStr())) {
                                numCount = fkJoinStatusesMap.get(fkJoin.getRpkStr());
                            }
                            if (fkJoin.canJoin()) { // can join
                                numCount += fkJoin.getCanJoinNum();
                            } else { // can't join
                                numCount += fkJoin.getCantJoinNum();
                                flag = false;
                            }
                            fkJoinStatusesMap.put(fkJoin.getRpkStr(), numCount);
                        }
                        break;
                }
            }
        }
        Iterator<Map.Entry<String, Integer>> iterator = fkJoinStatusesMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            int numCount = entry.getValue();

            ArrayList<JoinStatusesSizePair> joinStatusesSizePairs = fksJoinInfoSizeMap.get(entry.getKey());
            satisfiedFkJoinInfo.clear();

            int cumulant = 0;
            for (int i = 0; i < joinStatusesSizePairs.size(); i++) {
                if ((joinStatusesSizePairs.get(i).getJoinStatuses() & numCount) == numCount) {
                    cumulant += joinStatusesSizePairs.get(i).getSize();
                    satisfiedFkJoinInfo.add(new JoinStatusesSizePair(
                            joinStatusesSizePairs.get(i).getJoinStatuses(), cumulant));
                }
            }

            if (cumulant == 0) {
                LOGGER.error("\n\tfkMissCount: " + Statistic.fkMissCount.incrementAndGet() +
                        ", referenced primary key: " + entry.getKey() + ", numCount: " + numCount);
                return tuple;
            }

            ArrayList<long[]> candidates = null;
            cumulant = (int) (Math.random() * cumulant);
            for (int i = 0; i < satisfiedFkJoinInfo.size(); i++) {
                if (cumulant < satisfiedFkJoinInfo.get(i).getSize()) {
                    candidates = fksJoinInfo.get(entry.getKey()).get(satisfiedFkJoinInfo.get(i).getJoinStatuses());
                    break;
                }
            }

            long[] fkValues = candidates.get((int) (Math.random() * candidates.size()));
            String[] rpkNames = rpkStrToArray.get(entry.getKey());
            for (int i = 0; i < rpkNames.length; i++) {
                attributeValueMap.put(referKeyForeKeyMap.get(rpkNames[i]), fkValues[i] + "");
            }
        }

        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).getKeyType() == 1) { // foreign key
                tuple[i] = attributeValueMap.get(keys.get(i).getKeyName());
            }
        }

        String[] pkNames = pkStrArr;
        long[] pkValues = new long[pkNames.length];
        for (int i = 0; i < pkNames.length; i++) {
            pkValues[i] = Long.parseLong(attributeValueMap.get(pkNames[i]));
        }
        if (!pkJoinInfo.containsKey(pkJoinStatuses)) {
            pkJoinInfo.put(pkJoinStatuses, new ArrayList<long[]>());
            pkJoinInfoSizeMap.put(pkJoinStatuses, 0L);
        }

        ArrayList<long[]> candidates = pkJoinInfo.get(pkJoinStatuses);
        long size = pkJoinInfoSizeMap.get(pkJoinStatuses) + 1;
        pkJoinInfoSizeMap.put(pkJoinStatuses, size);
        if (candidates.size() < pkvsMaxSize) {
            candidates.add(pkValues);
        } else {
            if (Math.random() < ((double) pkvsMaxSize / size)) {
                candidates.set((int) (Math.random() * candidates.size()), pkValues);
            }
        }

        // for Date and DateTime typed attributes, convert their values from long form to string form
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getDataType().equals("date")) {
                tuple[keys.size() + i] = dateSdf.format(new Date(Long.parseLong(tuple[keys.size() + i])));
            } else if (attributes.get(i).getDataType().equals("datetime")) {
                tuple[keys.size() + i] = dateTimeSdf.format(new Date(Long.parseLong(tuple[keys.size() + i])));
            }
        }

        return tuple;
    }

    private boolean adjustFksGeneStrategy() {
        for (int i = 0; i < referencedKeys.size(); i++) {
            // get all 'FKJoin' nodes associated with current foreign key
            List<FKJoin> fkJoinNodes = new ArrayList<FKJoin>();
            for (int j = 0; j < constraintChains.size(); j++) {
                List<CCNode> nodes = constraintChains.get(j).getNodes();
                for (int k = 0; k < nodes.size(); k++) {
                    if (nodes.get(k).getType() == 2) {
                        FKJoin fkJoin = (FKJoin) nodes.get(k).getNode();
                        if (fkJoin.getRpkStr().equals(referencedKeys.get(i))) {
                            fkJoinNodes.add(fkJoin);
                        }
                    }
                }
            }
            LOGGER.debug("\nAll 'FKJoin' nodes of " + referencedKeys.get(i) + fkJoinNodes);

            // all 'FKJoinAdjustment' only share one array 'joinStatuses'
            boolean[] joinStatuses = new boolean[fkJoinNodes.size()];

            // set the 'fkJoinAdjustment' for every 'FKJoin' node
            for (int j = 0; j < fkJoinNodes.size(); j++) {
                // we don't need to adjust the generation strategy of the first 'FKJoin' node
                if (j == 0) {
                    fkJoinNodes.get(0).setFkJoinAdjustment(new FKJoinAdjustment(0, joinStatuses,
                            new ArrayList<FKJoinAdjustRule>(), fkJoinNodes.get(0).getProbability()));
                    continue;
                }

                int order = j;
                List<FKJoinAdjustRule> rules = getRules(fkJoinNodes, order);
                float probability = getProbability(fkJoinNodes, rules, order);

                if (probability < 0 || probability > 1) {
                    LOGGER.error("probability is " + probability + ", adjustment is fail!");
                    return false;
                } else {
                    FKJoinAdjustment fkJoinAdjustment = new FKJoinAdjustment(order, joinStatuses, rules, probability);
                    fkJoinNodes.get(j).setFkJoinAdjustment(fkJoinAdjustment);
                    LOGGER.debug("\n\tAdjustment of fkJoins " + j + ": " + fkJoinAdjustment);
                }
            }
        }
        return true;
    }

    private List<FKJoinAdjustRule> getRules(List<FKJoin> fkJoinNodes, int order){
        List<FKJoinAdjustRule> rules = new ArrayList<FKJoinAdjustRule>();
        int joinStatusesNum = (int)Math.pow(2, order + 1);
        for (int i = 0; i < joinStatusesNum; i++){
            String str = new StringBuilder(Integer.toBinaryString(i)).reverse().toString();
            boolean[] joinStatuses = new boolean[order + 1];
            for (int j = 0; j < str.length(); j++) {
                joinStatuses[j] = str.charAt(j) == '1'? true : false;
            }
            for(int j = str.length(); j < order + 1; j++) {
                joinStatuses[j] = false;
            }
            int numCount = 0;
            for (int j = 0; j < order + 1; j++) {
                if (joinStatuses[j]) {
                    numCount += fkJoinNodes.get(j).getCanJoinNum();
                } else {
                    numCount += fkJoinNodes.get(j).getCantJoinNum();
                }
            }
            String rpkStr = fkJoinNodes.get(0).getRpkStr();
            ArrayList<JoinStatusesSizePair> joinStatusesSizePairs = fksJoinInfoSizeMap.get(rpkStr);
            boolean existent = false;
            for (int j = 0; j < joinStatusesSizePairs.size(); j++) {
                if ((joinStatusesSizePairs.get(j).getJoinStatuses() & numCount) == numCount) {
                    existent = true;
                    break;
                }
            }
            if (!existent) {
                rules.add(new FKJoinAdjustRule(joinStatuses));
            }
        }
        Collections.sort(rules);
        for (int i = 0; i < rules.size(); i++) {
            if (i == rules.size() - 1) {
                break;
            }
            if (Arrays.toString(rules.get(i).getCause()).equals(Arrays.toString(rules.get(i + 1).getCause()))) {
                rules.remove(i);
                rules.remove(i);
                i = i - 1;
            }
        }
        return rules;
    }

    private float getProbability(List<FKJoin> fkJoinNodes, List<FKJoinAdjustRule> rules, int order){
        float trueProbability = 0, falseProbability = 0;
        for (int i = 0; i < rules.size(); i++){
            boolean[] cause = rules.get(i).getCause();
            float probabilityOfCause = 1;
            for (int j = 0; j < cause.length; j++){
                boolean[] frontPartCause = Arrays.copyOf(cause, cause.length - j);
                FKJoin frontFkJoin = fkJoinNodes.get(cause.length - j - 1);
                List<FKJoinAdjustRule> frontFkJoinRules = frontFkJoin.getFkJoinAdjustment().getRules();

                boolean flag = false;
                for (int k = 0; k < frontFkJoinRules.size(); k++) {
                    boolean[] causeAndEffect = new boolean[frontPartCause.length];
                    System.arraycopy(frontFkJoinRules.get(k).getCause(), 0,
                            causeAndEffect, 0, frontPartCause.length - 1);
                    causeAndEffect[causeAndEffect.length - 1] = frontFkJoinRules.get(k).getEffect();
                    if (Arrays.equals(frontPartCause, causeAndEffect)) {
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    float accumulativeProbability = frontFkJoin.getAccumulativeProbability();
                    FKJoinAdjustment fkJoinAdjustment = frontFkJoin.getFkJoinAdjustment();
                    if (frontPartCause[frontPartCause.length - 1]) {
                        probabilityOfCause *= (accumulativeProbability * fkJoinAdjustment.getProbability());
                    } else {
                        probabilityOfCause *= (accumulativeProbability * ( 1 - fkJoinAdjustment.getProbability()));
                    }
                }
            }
            if (rules.get(i).getEffect()) {
                trueProbability += probabilityOfCause;
            } else {
                falseProbability += probabilityOfCause;
            }
        }
        FKJoin fkJoin = fkJoinNodes.get(order);
        float originalTrueProbability = fkJoin.getAccumulativeProbability() * fkJoin.getProbability();
        float originalFalseProbability = fkJoin.getAccumulativeProbability() * (1 - fkJoin.getProbability());
        float probability = (originalTrueProbability - trueProbability) /
                ((originalTrueProbability - trueProbability) + (originalFalseProbability - falseProbability));
        return probability;
    }
    private void initParsii() {
        for (int i = 0; i < constraintChains.size(); i++) {
            List<CCNode> nodes = constraintChains.get(i).getNodes();
            for (int j = 0; j < nodes.size(); j++) {
                if (nodes.get(j).getType() == 0) {
                    Filter filter = (Filter)nodes.get(j).getNode();
                    FilterOperation[] operations = filter.getFilterOperations();
                    for (int k = 0; k < operations.length; k++) {
                        operations[k].initParsii(parameterMap.get(operations[k].getId()), attributeMap);
                    }
                }
            }
        }
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableSize() {
        return tableSize;
    }

    public String getPkStr() {
        return pkStr;
    }

    public List<String> getReferencedKeys() {
        return referencedKeys;
    }

    public Map<Integer, ArrayList<long[]>> getPkJoinInfo() {
        return pkJoinInfo;
    }

    @Override
    public String toString() {
        return "\nTableGeneTemplate [tableName=" + tableName + ", tableSize=" + tableSize + ", pkStr=" + pkStr
                + ", \nkeys=" + keys + ", \nattributes=" + attributes + ", \nconstraintChains=" + constraintChains
                + ", \nreferencedKeys=" + referencedKeys + ", \nreferKeyForeKeyMap=" + referKeyForeKeyMap
                + ", \nparameterMap=" + parameterMap + ", \nshuffleMaxNum=" + shuffleMaxNum + ", pkvsMaxSize="
                + pkvsMaxSize + "]";
    }

    public int getConstraintChainsNum() {
        return constraintChains.size();
    }

    public int getConstraintsNum() {
        int count = 0;
        for (int i = 0; i < constraintChains.size(); i++) {
            count += constraintChains.get(i).getNodes().size();
        }
        return count;
    }

    public int getEntriesNum() {
        int count = 0;
        Iterator<Map.Entry<String, Map<Integer, ArrayList<long[]>>>> iterator = fksJoinInfo.entrySet().iterator();
        while (iterator.hasNext()) {
            int tmp = iterator.next().getValue().size();
            if (tmp > count) {
                count = tmp;
            }
        }
        return count;
    }

    public int getRulesNum() {
        int count = 0;
        for (int i = 0; i < constraintChains.size(); i++) {
            List<CCNode> nodes = constraintChains.get(i).getNodes();
            for (int j = 0; j < nodes.size(); j++) {
                if (nodes.get(j).getType() == 2) {
                    FKJoin fkJoin = (FKJoin)nodes.get(j).getNode();
                    count += fkJoin.getFkJoinAdjustment().getRules().size();
                }
            }
        }
        return count;
    }
}
class Key implements Serializable {
    private static final long serialVersionUID = 1L;

    private String keyName = null;

    private int keyType;

    public Key(String keyName, int keyType) {
        super();
        this.keyName = keyName;
        this.keyType = keyType;
    }

    public Key(Key key) {
        super();
        this.keyName = key.keyName;
        this.keyType = key.keyType;
    }

    public String getKeyName() {
        return keyName;
    }

    public int getKeyType() {
        return keyType;
    }

    @Override
    public String toString() {
        return "Key [keyName=" + keyName + ", keyType=" + keyType + "]";
    }
}

class JoinStatusesSizePair {
    private int joinStatuses;
    private int size;

    public JoinStatusesSizePair(int joinStatuses, int size) {
        super();
        this.joinStatuses = joinStatuses;
        this.size = size;
    }

    public JoinStatusesSizePair(JoinStatusesSizePair joinStatusesSizePair) {
        super();
        this.joinStatuses = joinStatusesSizePair.joinStatuses;
        this.size = joinStatusesSizePair.size;
    }

    public int getJoinStatuses() {
        return joinStatuses;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "\n\tJoinStatusesSizePair [joinStatuses=" + joinStatuses + ", size="
                + size + "]";
    }

}
