package Controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import ConstraintChains.ConstraintChain;
import ConstraintChains.ConstraintChainReader;
import Pretreatment.Preprocessor;
import Pretreatment.TableGeneTemplate;
import QueryInstantiation.ComputingThreadPool;
import QueryInstantiation.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.QueryInstantiator;
import Schema.SchemaReader;
import Schema.Table;

import run.Configurations;


// the controller of the distributed data generator (Touchstone)
// main function: 1. assign the data generation tasks to all data generators,
//                2. merge the join information of the primary key maintained by data generators
public class Controller {
    private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

    // table names stored in the partial order
    private List<String> tablePartialOrder = null;

    // map: table name -> its generation template
    private Map<String, TableGeneTemplate> tableGeneTemplateMap = null;

    // running configurations
    private Configurations configurations = null;

    public Controller(List<String> tablePartialOrder, Map<String, TableGeneTemplate> tableGeneTemplateMap,
                      Configurations configurations) {
        super();
        this.tablePartialOrder = tablePartialOrder;
        this.tableGeneTemplateMap = tableGeneTemplateMap;
        this.configurations = configurations;
    }

    // the clients are linked with the servers of data generators
    // they are used for sending data generation task
    private List<ControllerClient> clients = null;

    // store all 'pkJoinInfo's received from data generators
    private static List<Map<Integer, ArrayList<long[]>>> pkJoinInfoList = null;

    // control the time point of merging the join information of the primary key (pkJoinInfoList)
    private static CountDownLatch countDownLatch = null;

    // set up the server and clients of the controller
    // server: receive the join information of the primary key (pkJoinInfo)
    // clients: send the data generation task
    public void setUpNetworkThreads() {
        new Thread(new ControllerServer(configurations.getControllerPort())).start();

        clients = new ArrayList<ControllerClient>();
        List<String> dataGeneratorIps = configurations.getDataGeneratorIps();
        List<Integer> dataGeneratorPorts = configurations.getDataGeneratorPorts();
        for (int i = 0; i < dataGeneratorIps.size(); i++) {
            ControllerClient client = new ControllerClient(dataGeneratorIps.get(i), dataGeneratorPorts.get(i));
            new Thread(client).start();
            clients.add(client);
        }
    }

    // generate data: tables are generated one by one in accordance with the partial order
    public void geneData() {

        pkJoinInfoList = new ArrayList<Map<Integer, ArrayList<long[]>>>();

        // map: primary key -> reference count
        // it is used to clear the unnecessary join information of primary keys in time
        Map<String, Integer> pkReferenceCountMap = new HashMap<String, Integer>();
        Iterator<Entry<String, TableGeneTemplate>> iterator = tableGeneTemplateMap.entrySet().iterator();
        while (iterator.hasNext()) {
            TableGeneTemplate template = iterator.next().getValue();
            List<String> referencedKeys = template.getReferencedKeys();
            for (int i = 0; i < referencedKeys.size(); i++) {
                if (pkReferenceCountMap.containsKey(referencedKeys.get(i))) {
                    int count = pkReferenceCountMap.get(referencedKeys.get(i)) + 1;
                    pkReferenceCountMap.put(referencedKeys.get(i), count);
                } else {
                    pkReferenceCountMap.put(referencedKeys.get(i), 1);
                }
            }
        }
        LOGGER.info("\n\tThe 'pkReferenceCountMap' (primary key -> reference count) is: " + pkReferenceCountMap);

        // map: primary key (its string representation) -> (combined join statuses -> primary keys list)
        // 'neededPKJoinInfo' -> 'fksJoinInfo'
        Map<String, Map<Integer, ArrayList<long[]>>> neededPKJoinInfo = new HashMap<String,
                Map<Integer, ArrayList<long[]>>>();

        // wait until all clients are connected to the server of the data generator
        waitClientsConnected();

        long startTime = System.currentTimeMillis();

        LOGGER.info("\n\tStart generating data!");

        for (int i = 0; i < tablePartialOrder.size(); i++) {
            String tableName = tablePartialOrder.get(i);
            TableGeneTemplate template = tableGeneTemplateMap.get(tableName);

            LOGGER.info("\n\tStart generating table " + tableName + "!");

            List<String> referencedKeys = template.getReferencedKeys();
            Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo =
                    new HashMap<String, Map<Integer, ArrayList<long[]>>>();
            for (int j = 0; j < referencedKeys.size(); j++) {
                fksJoinInfo.put(referencedKeys.get(j), neededPKJoinInfo.get(referencedKeys.get(j)));
                int count = pkReferenceCountMap.get(referencedKeys.get(j)) - 1;
                pkReferenceCountMap.put(referencedKeys.get(j), count);
                if (count == 0) {
                    // the controller releases the unnecessary join information
                    neededPKJoinInfo.remove(referencedKeys.get(j));
                }
            }
            template.setFksJoinInfo(fksJoinInfo);
            LOGGER.info("\n\tThe 'fkJoinInfo' has been set!");
            LOGGER.info("\n\tThe key set of neededPKJoinInfo is: " + neededPKJoinInfo.keySet());

            // for experiments
            LOGGER.info("\n\tThe number of constraint chains: " + template.getConstraintChainsNum());
            LOGGER.info("\n\tThe number of constraints in constraint chains: " + template.getConstraintsNum());
            LOGGER.info("\n\tThe number of entries in join information table: " + template.getEntriesNum());

            countDownLatch = new CountDownLatch(configurations.getDataGeneratorIps().size());
            for (int j = 0; j < clients.size(); j++) {
                clients.get(j).send(template);
            }
            LOGGER.info(String.valueOf(template));
            LOGGER.info("\n\tThe template of " + tableName + " has been successfully sent!");

            // wait for all data generators to return the join information of primary key
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("\n\tThe primary key join information (pkJoinInfo) of all data generators has been received!");

            LOGGER.info("\n\tStart merging 'pkJoinInfoList' ...");
            neededPKJoinInfo.put(template.getPkStr(),
                    JoinInfoMerger.merge(pkJoinInfoList, configurations.getPkvsMaxSize()));
            LOGGER.info("\n\tMerge end!");
            LOGGER.info("\n\tThe key set of neededPKJoinKeyInfo is: " + neededPKJoinInfo.keySet());

            pkJoinInfoList.clear();
        }

        long endTime = System.currentTimeMillis();
        LOGGER.info("\n\tTime of data generation: " + (endTime - startTime) + "ms");
    }

    private void waitClientsConnected() {
        loop : while (true) {
            for (int i = 0; i < clients.size(); i++) {
                if (!clients.get(i).isConnected()) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    LOGGER.info("\n\tAll data generators do not startup successful!");
                    continue loop;
                }
            }
            break;
        }
        LOGGER.info("\n\tAll data generators startup successful!");
    }

    // it's called by 'ControllerServerHandler' when receiving a 'pkJoinInfo'
    public static synchronized void receivePkJoinInfo(Map<Integer, ArrayList<long[]>> pkJoinInfo) {
        pkJoinInfoList.add(pkJoinInfo);
        countDownLatch.countDown();
    }

    // test
    public static void main(String[] args) {
        System.setProperty("com.wolfram.jlink.libdir",
                "/Applications/Mathematica.app/Contents/SystemFiles/Links/JLink");

        // TPC-H
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read("src/test/input/tpch_schema_sf_1.txt");
		ConstraintChainReader constraintChainReader = new ConstraintChainReader();
		List<ConstraintChain> constraintChains = constraintChainReader.read("src/test/input/tpch_cardinality_constraints_sf_1.txt");
		ComputingThreadPool computingThreadPool = new ComputingThreadPool(2, 20, 0.00001);
		QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
		queryInstantiator.iterate();
		List<Parameter> parameters = queryInstantiator.getParameters();

		Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
		List<String> tablePartialOrder = preprocessor.getPartialOrder();
		Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(1000, 10000);
		Configurations configurations = new Configurations("src/test/touchstone.conf");
		Controller controller = new Controller(tablePartialOrder, tableGeneTemplateMap, configurations);
		controller.setUpNetworkThreads();
		controller.geneData();


    }
}
