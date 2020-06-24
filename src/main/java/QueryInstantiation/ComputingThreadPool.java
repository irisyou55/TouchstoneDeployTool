package QueryInstantiation;

import DataType.TSDataTypeInfo;
import DataType.TSInteger;
import Mathematica.Mathematica;
import Schema.Attribute;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ComputingThreadPool {

    private int threadNum;
    private Map<Integer, Parameter> parameterMap = null;

    private int QUEUE_SIZE = 100;
    private List<ArrayBlockingQueue<ComputingTask>> tasksList= null;
    private List<ComputingThread> computingThreads = null;

    public ComputingThreadPool(int threadNum, int maxIterations, double requiredRelativeError) {
        this.threadNum = threadNum;
        init(maxIterations, requiredRelativeError);
    }

    private void init(int maxIterations, double requiredRelativeError) {
        tasksList = new ArrayList<ArrayBlockingQueue<ComputingTask>>();
        parameterMap = new ConcurrentHashMap<Integer, Parameter>();
        computingThreads = new ArrayList<ComputingThread>();
        for (int i = 0; i < threadNum; i++) {
            tasksList.add(new ArrayBlockingQueue<ComputingTask>(QUEUE_SIZE));
            ComputingThread computingThread = new ComputingThread(tasksList.get(i),
                    parameterMap, maxIterations, requiredRelativeError);
            computingThreads.add(computingThread);
            new Thread(computingThread).start();
        }
    }

    public synchronized void addTask(ComputingTask task) {
        try {
            tasksList.get((int)(Math.random() * tasksList.size())).put(task);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void waitFinished() {
        while (true) {
            boolean finished = true;
            for (int i = 0; i < tasksList.size(); i++) {
                if (tasksList.get(i).size() != 0) {
                    finished = false;
                    break;
                }
            }
            for (int i = 0; i < computingThreads.size(); i++) {
                if (!computingThreads.get(i).isInactive()) {
                    finished = false;
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (finished) {
                break;
            }
        }
    }

    public Map<Integer, Parameter> getParameterMap() {
        return parameterMap;
    }

    public void clearParameterMap() {
        parameterMap.clear();
    }

}
class ComputingThread implements Runnable {

    private ArrayBlockingQueue<ComputingTask> tasksQueue = null;
    private Map<Integer, Parameter> parameterMap = null;
    private int maxIterations;
    private double requiredRelativeError;

    private Mathematica mathematica = null;
    private boolean inactive;
    private Logger logger = null;

    public ComputingThread(ArrayBlockingQueue<ComputingTask> tasksQueue, Map<Integer, Parameter> parameterMap,
                           int maxIterations, double requiredRelativeError) {
        this.tasksQueue = tasksQueue;
        this.parameterMap = parameterMap;
        this.maxIterations = maxIterations;
        this.requiredRelativeError = requiredRelativeError;
        init();
    }

    private void init() {
        inactive = true;
    }

    public boolean isInactive() {
        return inactive;
    }

    public void run() {
        try {
            while (true) {
                inactive = true;
                ComputingTask task = tasksQueue.take();
                inactive = false;
                if (task.getChildren() == null) {
                    synchronized (ComputingThread.class) {
                        if (parameterMap.containsKey(task.getId())) {
                            // operator is 'bet'
                            parameterMap.get(task.getId()).merge(solve(task));
                        } else {
                            parameterMap.put(task.getId(), solve(task));
                        }
                    }
                    // non-equi join
                } else {
                    List<Integer> children = task.getChildren();
                    for (int i = 0; i < children.size(); i++) {
                        int childId = children.get(i);
                        if (parameterMap.containsKey(childId)) {
                            Parameter para = parameterMap.get(childId);
                            if ((para.isBet() && (para.getValues().size() == 2)) || !para.isBet()) {
                                task.getChildrensConstraints().add(para.getConstraint());
                            }
                        }
                    }
                    if (task.getChildren().size() == task.getChildrensConstraints().size()) {
                        synchronized (ComputingThread.class) {
                            if (parameterMap.containsKey(task.getId())) {
                                parameterMap.get(task.getId()).merge(solve(task));
                            } else {
                                parameterMap.put(task.getId(), solve(task));
                            }
                        }
                    } else {
                        task.getChildrensConstraints().clear();
                        tasksQueue.put(task);
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Parameter solve(ComputingTask task) {
        Parameter para = null;
        // if the expression is only a single attribute, we do not need the Mathematica's calculation
        if (task.getAttrNames().size() == 1 && task.getAttrNames().get(0).equals(task.getExpression())) {
            para = subSolve1(task);
        } else {
            para = subSolve2(task);
        }
        return para;
    }

    private Parameter subSolve1(ComputingTask task){
        String operator = task.getOperator();
        Attribute attribute = task.getAttributes().get(0);
        float probability = task.getProbability();

        if (operator.equals(">") || operator.equals(">=")) {
            probability = 1 - probability;
        }
        double paraValue;
        long deviation = 0;
        if (attribute.getDataType().equals("integer")) {
            TSInteger dataTypeInfo = (TSInteger)attribute.getDataTypeInfo();
            if (dataTypeInfo.isAdjusted()) {
                long randomIndex = dataTypeInfo.getCorrespondingIndex(probability);
                paraValue = dataTypeInfo.getGeneData(randomIndex);
                Map<Long, Double> indexProbabilityMap = dataTypeInfo.getIndexProbabilityMap();
                if (indexProbabilityMap.containsKey(randomIndex)) {
                    deviation = (int)(task.getInputDataSize() * indexProbabilityMap.get(randomIndex) / 2);
                }
            } else {
                double min = dataTypeInfo.getMinValue();
                double max = dataTypeInfo.getMaxValue();
                paraValue = (max - min) * probability + min;
            }
            //date, datetime, real, decimal
        }
        else{
            double min = attribute.getDataTypeInfo().getMinValue();
            double max = attribute.getDataTypeInfo().getMaxValue();
            paraValue = (max - min) * probability + min;
        }

        List<String> values = new ArrayList<String>();
        values.add(new Double(paraValue).toString());
        long cardinality = (long)(task.getInputDataSize() * probability);
        // 'constraint' is only used in non-equi join
        String constraint = task.getExpression() + " " + operator + " " + new BigDecimal(paraValue).toPlainString();
        Parameter parameter = new Parameter(task.getId(), values, cardinality, deviation, task.isBet(), constraint);

        logger.debug("" + task + parameter);
        return parameter;

    }

    private Parameter subSolve2(ComputingTask task) {
        if (mathematica == null) {
            mathematica = new Mathematica();
        }

        String expression = task.getExpression();
        String operator = task.getOperator();
        List<String> attrNames = task.getAttrNames();
        List<Attribute> attributes = task.getAttributes();
        float probability = task.getProbability();
        List<String> childrensConstraints = task.getChildrensConstraints();

        double minValue = mathematica.getMostValue(expression, attrNames, attributes, childrensConstraints, false);
        double maxValue = mathematica.getMostValue(expression, attrNames, attributes, childrensConstraints, true);

        double mathSpaceSize = 1;
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getDataType().equals("integer")) {
                TSInteger dataTypeInfo = (TSInteger) attributes.get(i).getDataTypeInfo();
                if (dataTypeInfo.isAdjusted()) {
                    continue;
                }
            }
            TSDataTypeInfo dataTypeInfo = attributes.get(i).getDataTypeInfo();
            mathSpaceSize *= (dataTypeInfo.getMaxValue() - dataTypeInfo.getMinValue());
        }

        double predictedValue = (minValue + maxValue) / 2;
        double bestValue = predictedValue;
        float bestRelativeError = Float.MAX_VALUE;

        logger.debug(task + "\n\tminValue: " + minValue + "\tmaxValue: " + maxValue +
                "\tmathSpaceSize: " + mathSpaceSize);

        for (int i = 0; i < maxIterations; i++) {
            double inteSpaceSize = mathematica.integrate(expression, operator, attrNames, attributes,
                    predictedValue, childrensConstraints);
            float mathProbability = (float) (inteSpaceSize / mathSpaceSize);
            float relativeError = Math.abs(mathProbability - probability);
            logger.debug("\n\tmathematica iterations: " + i + "\tcurrent probability: " +
                    mathProbability + "\trelative error: " + relativeError);

            if (relativeError < bestRelativeError) {
                bestValue = predictedValue;
                bestRelativeError = relativeError;
            }
            if (bestRelativeError <= requiredRelativeError) {
                break;
            }

            if ((operator.equals(">") || operator.equals(">=")) ^ (mathProbability > probability)) {
                maxValue = predictedValue;
            } else {
                minValue = predictedValue;
            }
            predictedValue = (minValue + maxValue) / 2;
        }

        List<String> values = new ArrayList<String>();
        values.add(new Double(bestValue).toString());
        long cardinality = (long) (task.getInputDataSize() * probability);
        long deviation = (long) (task.getInputDataSize() * bestRelativeError);
        String constraint = task.getExpression() + " " + operator + " " + new BigDecimal(bestValue).toPlainString();
        Parameter parameter = new Parameter(task.getId(), values, cardinality, deviation, task.isBet(), constraint);

        logger.debug(parameter);
        return parameter;
    }

}