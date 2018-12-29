package nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by Yang Liu on 2018/12/29
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // 将训练集中的小文件合并成SequenceFile
        Configuration confOfTrain = new Configuration();
        confOfTrain.set("INPUTPATH", Config.BASE_TRAIN_DATA_PATH);
        confOfTrain.set("OUTPUTPATH", Config.SEQUENCE_INPUT_TRAIN_DATA);

        ConvertFilesToSequenceFile convFToSf = new ConvertFilesToSequenceFile();

        ToolRunner.run(confOfTrain, convFToSf, args);

        // 计算每个类别中的文档数
        CalcDocNumInClass calcDocNumInClass = new CalcDocNumInClass();
        ToolRunner.run(confOfTrain, calcDocNumInClass, args);

        // 计算每个类中每个单词出现的次数
        CalcEachWordNumInClass calcEachWordNumInClass = new CalcEachWordNumInClass();
        ToolRunner.run(confOfTrain, calcEachWordNumInClass, args);

        // 计算每个类中的总共单词数
        CalcWordNumInClass calcWordNumInClass = new CalcWordNumInClass();
        ToolRunner.run(confOfTrain, calcWordNumInClass, args);

        // 将测试集中的小文件合并成SequenceFile
        Configuration confOfTest = new Configuration();
        confOfTest.set("INPUTPATH", Config.BASE_TEST_DATA_PATH);
        confOfTest.set("OUTPUTPATH", Config.SEQUENCE_INPUT_TEST_DATA);

        ToolRunner.run(confOfTest, convFToSf, args);

        // 朴素贝叶斯分类
        Configuration confOfBayes = new Configuration();
        NaiveBayes naiveBayes = new NaiveBayes();
        BufferedReader in = new BufferedReader(new FileReader(Config.LOG_FILE));
        String s;
        StringBuilder stringBuilder = new StringBuilder();
        while ((s = in.readLine()) != null) {
            stringBuilder.append(s);
        }
        in.close();
        System.out.println(stringBuilder.toString());
        confOfBayes.set("CLASSGROUP", stringBuilder.toString());
        ToolRunner.run(confOfBayes, naiveBayes, args);

        // 评价，计算混淆矩阵
        Configuration confOfEvaluation = new Configuration();
        Evaluation evaluation = new Evaluation();
        ToolRunner.run(confOfEvaluation, evaluation, args);
    }
}
