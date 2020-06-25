package run;

import java.util.List;
import java.util.Map;

import ConstraintChains.ConstraintChain;
import ConstraintChains.ConstraintChainReader;
import Controller.Controller;
import Pretreatment.Preprocessor;
import Pretreatment.TableGeneTemplate;
import QueryInstantiation.ComputingThreadPool;
import QueryInstantiation.Parameter;
import Schema.SchemaReader;
import Schema.Table;
import org.apache.log4j.PropertyConfigurator;


public class RunController {

    // args[0]: src/test/touchstone.conf
    public static void main(String[] args) {
        Configurations configurations = new Configurations(args[0]);

        PropertyConfigurator.configure(configurations.getLog4jConfFile());
        System.setProperty("com.wolfram.jlink.libdir", configurations.getjLinkPath());

        SchemaReader schemaReader = new SchemaReader();
        List<Table> tables = schemaReader.read(configurations.getDatabaseSchemaInput());

        ConstraintChainReader constraintChainsReader = new ConstraintChainReader();
        List<ConstraintChain> constraintChains = constraintChainsReader.
                read(configurations.getCardinalityConstraintsInput());

        ComputingThreadPool computingThreadPool = new ComputingThreadPool(
                configurations.getQueryInstantiationThreadNum(),
                configurations.getParaInstantiationMaxIterations(),
                configurations.getParaInstantiationRelativeError());
        QueryInstantiator queryInstantiator = new QueryInstantiator(
                tables, constraintChains, null,
                configurations.getQueryInstantiationMaxIterations(),
                configurations.getQueryInstantiationGlobalRelativeError(),
                computingThreadPool);
        queryInstantiator.iterate();
        List<Parameter> parameters = queryInstantiator.getParameters();

        Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
        List<String> tablePartialOrder = preprocessor.getPartialOrder();
        Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(
                configurations.getShuffleMaxNum(), configurations.getPkvsMaxSize());
        /*unchecked*/
        Controller controller = new Controller(tablePartialOrder, tableGeneTemplateMap, configurations);
        controller.setUpNetworkThreads();
        controller.geneData();
    }
}
