package nb;
/**
 * Created by Yang Liu on 2018/12/8
 */

import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Random;

public class TrainTestSplit {
    // 将hdfs上的文件划分为训练集和测试集
    public static void trainTestSplit(double testRatio, String path) {
        HDFSFileSystemUtils fsUtils = new HDFSFileSystemUtils("hdfs://192.168.1.109:9001");
        String typeName = path.substring(path.lastIndexOf('/') + 1);
        String trainPath = "/input/train/" + typeName;
        String testPath = "/input/test/" + typeName;
        fsUtils.createDir(trainPath);
        fsUtils.createDir(testPath);
        List<Path> paths = fsUtils.listStatus(path);
        int size = paths.size();
        Random random = new Random();
        int testSize = (int) (testRatio * size);
        // 测试集
        for (int i = 0; i < testSize; i++) {
            int index = random.nextInt(size--);
            Path targetPath = paths.get(index);
            String targetFileName = targetPath.toString().substring(targetPath.toString().lastIndexOf('/'));
            fsUtils.copy(path + "/" + targetFileName, testPath + "/" + targetFileName);
            paths.remove(index);
        }
        // 训练集
        for (Path targetPath : paths) {
            String targetFileName = targetPath.toString().substring(targetPath.toString().lastIndexOf('/'));
            fsUtils.copy(path + "/" + targetFileName, trainPath + "/" + targetFileName);
        }
    }

    public static void main(String[] args) {
        trainTestSplit(0.3, "/input/Country/AUSTR");
        trainTestSplit(0.3, "/input/Country/CANA");
        trainTestSplit(0.3, "/input/Country/BELG");
        trainTestSplit(0.3, "/input/Country/BRAZ");
    }
}
