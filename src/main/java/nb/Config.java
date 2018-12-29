package nb;

/**
 * Created by Yang Liu on 2018/12/10
 */
public class Config {
    // hdfs的base path
    public static final String BASE_PATH = "hdfs://192.168.1.109:9001/input/";

    public static final String BASE_TRAIN_DATA_PATH = "hdfs://192.168.1.109:9001/input/train";

    public static final String BASE_TEST_DATA_PATH = "hdfs://192.168.1.109:9001/input/test";

    public static final String SEQUENCE_INPUT_TRAIN_DATA = BASE_PATH + "SequenceInputTrainData";

    public static final String SEQUENCE_INPUT_TEST_DATA = BASE_PATH + "SequenceInputTestData";

    public static final String DOC_NUM_IN_CLASS_OUTPUT_PATH = BASE_PATH + "DocNumInClass";

    public static final String EACH_WORD_NUM_IN_CLASS_OUTPUT_PATH = BASE_PATH + "EachWordNumInClass";

    public static final String ALL_WORD_NUM_IN_CLASS_OUTPUT_PATH = BASE_PATH + "AllWordNumInClass";

    public static final String RESULT_OF_CLASSFICATION = BASE_PATH + "ResultOfClassfication";

    public static final String EVALUATION_OUTPUT_PATH = BASE_PATH + "EvaluationResult";

    public static final String LOG_FILE = "classgroup.txt";
}
