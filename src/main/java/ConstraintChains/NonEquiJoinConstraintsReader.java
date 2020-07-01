package ConstraintChains;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.QueryInstantiator;


public class NonEquiJoinConstraintsReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(NonEquiJoinConstraintsReader.class);

    // test
    public static void main(String[] args) {
        NonEquiJoinConstraintsReader nonEquiJoinConstraintsReader = new NonEquiJoinConstraintsReader();
        nonEquiJoinConstraintsReader.read("src/test/input/non_equi_join_test.txt");
    //    nonEquiJoinConstraintsReader.read("src/test/input/function_test_non_equi_join_0.txt");
    }

    // the definition of child nodes should be precede the definition of parent node
    public List<NonEquiJoinConstraint> read(String nonEquiJoinConstraintsInput) {
        List<NonEquiJoinConstraint> nonEquiJoinConstraints = new ArrayList<NonEquiJoinConstraint>();
        Map<Integer, NonEquiJoinConstraint> nonEquiJoinConstraintMap = new HashMap<Integer, NonEquiJoinConstraint>();

        String inputLine = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new
                FileInputStream(nonEquiJoinConstraintsInput)))) {
            while ((inputLine = br.readLine()) != null) {
                // skip the blank lines and comments
                if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
                    continue;
                }
                // convert the input line to lower case and remove the spaces and tabs
                inputLine = inputLine.toLowerCase();
                inputLine = inputLine.replaceAll("[ \\t]+", "");

                String[] arr = inputLine.split(",");
                if (inputLine.matches("[ ]*c[ ]*\\[[\\s\\S^\\]]+\\][ ]*") && arr.length == 4) {
                    int id = Integer.parseInt(arr[0].substring(arr[0].indexOf('[') + 1));
                    String expression = arr[1].split("@")[0];
                    String operator = arr[1].split("@")[1];
                    float probability = Float.parseFloat(arr[2]);
                    float inputDataSize = Float.parseFloat(arr[3].substring(0, arr[3].indexOf(']')));
                    NonEquiJoinConstraint nonEquiJoinConstraint = new NonEquiJoinConstraint(id, expression,
                            operator, probability, inputDataSize);
                    nonEquiJoinConstraints.add(nonEquiJoinConstraint);
                    nonEquiJoinConstraintMap.put(id, nonEquiJoinConstraint);
                } else if (inputLine.matches("[ ]*r[ ]*\\[[\\s\\S^\\]]+\\][ ]*") && arr.length == 2) {
                    int id1 = Integer.parseInt(arr[0].substring(arr[0].indexOf('[') + 1));
                    int id2 = Integer.parseInt(arr[1].substring(0, arr[1].indexOf(']')));
                    nonEquiJoinConstraintMap.get(id1).getChildren().add(id2);
                    nonEquiJoinConstraintMap.get(id1).getChildren().addAll(nonEquiJoinConstraintMap.get(id2).getChildren());
                } else {
                    LOGGER.error("\n\tUnable to parse the non-equi join constraint information: " + inputLine);
                    System.exit(0);
                }
            }
        } catch (Exception e) {
            LOGGER.error("\n\tError input line: " + inputLine);
            e.printStackTrace();
            System.exit(0);
        }
        LOGGER.debug("\nThe non-equi join constraints: " + nonEquiJoinConstraints);
        return nonEquiJoinConstraints;
    }


}
