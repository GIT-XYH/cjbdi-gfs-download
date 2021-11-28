package test;

/**
 * @Author: XYH
 * @Date: 2021/11/26 9:59 上午
 * @Description:
 */
public class ArgsTest {
    public static void main(String[] args) {
        MyTest(args);
    }

    public static void MyTest(String[] args) {
        System.out.println("start produce data");
        String rootPath = args[0];
        String dataSet = args[1];
        String savePath = args[2];
        System.out.println(rootPath +"***" + dataSet + "***" + savePath);
    }
}
