package Mathematica;

import java.math.BigDecimal;
import java.util.List;

import DataType.TSDataTypeInfo;
import DataType.TSInteger;
import Schema.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wolfram.jlink.KernelLink;
import com.wolfram.jlink.MathLinkException;
import com.wolfram.jlink.MathLinkFactory;
import run.QueryInstantiator;


public class Mathematica {
    // the interaction interface between Mathematica and Java (JLink)
    private KernelLink ml = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(Mathematica.class);

    // initialize KernelLink ml
    public Mathematica() {
        try {
            ml = MathLinkFactory.createKernelLink("-linkmode launch -linkname "
                    + "'/Applications/Mathematica.app/Contents/MacOS/MathKernel'");
            // empty the computing environment
            ml.discardAnswer();
        } catch (MathLinkException e) {
            e.printStackTrace();
        }
    }

    public double getMostValue(String expression, List<String> attrNames, List<Attribute> attributes,
                               List<String> childrensConstraints, boolean isMax) {
        StringBuilder sb = new StringBuilder();
        int size = attrNames.size();
        for (int i = 0; i < size; i++) {
            sb.append("Clear[" + attrNames.get(i) + "]\n");
        }
        if (isMax) {
            sb.append("FindMaxValue[{" + expression + ", ");
        } else {
            sb.append("FindMinValue[{" + expression + ", ");
        }
        for (int i = 0; i < size; i++) {
            TSDataTypeInfo dataTypeInfo = attributes.get(i).getDataTypeInfo();
            BigDecimal minValue = new BigDecimal(dataTypeInfo.getMinValue());
            BigDecimal maxValue = new BigDecimal(dataTypeInfo.getMaxValue());
            sb.append(minValue.toPlainString() + " <= " + attrNames.get(i) + " <= " + maxValue.toPlainString());
            if (i != size - 1) {
                sb.append(" && ");
            }
        }
        if (childrensConstraints != null) {
            for (int i = 0; i < childrensConstraints.size(); i++) {
                sb.append(" && " + childrensConstraints.get(i));
            }
        }
        sb.append("}, {");
        for (int i = 0; i < size; i++) {
            sb.append(attrNames.get(i));
            if (i != size - 1) {
                sb.append(", ");
            }
        }
        sb.append("}]");
        LOGGER.debug("\n" + sb.toString());

        try {
            ml.evaluate(sb.toString());
            ml.waitForAnswer();
            return ml.getDouble();
        } catch (MathLinkException e) {
            e.printStackTrace();
            System.exit(0);
        }
        return Double.MIN_VALUE;
    }

    public double integrate(String expression, String operator, List<String> attrNames, List<Attribute> attributes,
                            double predictedValue, List<String> childrensConstraints) {
        StringBuilder sb = new StringBuilder();
        int size = attrNames.size();
        for (int i = 0; i < size; i++) {
            if (attributes.get(i).getDataType().equals("integer")) {
                TSInteger dataTypeInfo = (TSInteger)attributes.get(i).getDataTypeInfo();
                if (dataTypeInfo.isAdjusted()) {
                    sb.append("Clear[x" + i + "]\n");
                    continue;
                }
            }
            sb.append("Clear[" + attrNames.get(i) + "]\n");
        }
        for (int i = 0; i < size; i++) {
            if (attributes.get(i).getDataType().equals("integer")) {
                TSInteger dataTypeInfo = (TSInteger)attributes.get(i).getDataTypeInfo();
                if (dataTypeInfo.isAdjusted()) {
                    expression = expression.replaceAll(attrNames.get(i), dataTypeInfo.getPiecewiseFunction("x" + i));
                }
            }
        }
        sb.append("NIntegrate[Boole[" + expression + operator + (new BigDecimal(predictedValue).toPlainString()));
        if (childrensConstraints != null) {
            for (int i = 0; i < childrensConstraints.size(); i++) {
                sb.append(" && " + childrensConstraints.get(i));
            }
        }
        sb.append("]");
        for (int i = 0; i < size; i++) {
            if (attributes.get(i).getDataType().equals("integer")) {
                TSInteger dataTypeInfo = (TSInteger)attributes.get(i).getDataTypeInfo();
                if (dataTypeInfo.isAdjusted()) {
                    sb.append(", {x" + i + ", 0, 1}");
                    continue;
                }
            }
            TSDataTypeInfo dataTypeInfo = attributes.get(i).getDataTypeInfo();
            BigDecimal minValue = new BigDecimal(dataTypeInfo.getMinValue());
            BigDecimal maxValue = new BigDecimal(dataTypeInfo.getMaxValue());
            sb.append(", {" + attrNames.get(i) + ", " + minValue.toPlainString() + ", " + maxValue.toPlainString() + "}");
        }
        sb.append("]");
        LOGGER.debug("\n" + sb.toString());

        try {
            ml.evaluate(sb.toString());
            ml.waitForAnswer();
            return ml.getDouble();
        } catch (MathLinkException e) {
            e.printStackTrace();
            System.exit(0);
        }
        return Double.MIN_VALUE;
    }
}
