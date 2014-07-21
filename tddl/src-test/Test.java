import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Created by pinian.lpn on 2014/6/4.
 */
public class Test {

    public static void main(String[] args) {
        fileEncodingConvert("D:\\Java\\workspaces\\Intellij-idea\\tddl\\tddl\\src", "D:\\Java\\workspaces\\Intellij-idea\\tddl\\tddl\\src-test","GBK","UTF-8");
    }


    /**
     * 文件编码转换
     * @param srcPath 源码路径,如："D:\\dev\\workspace\\src"
     * @param targetPath 转换后的存放路径,如："D:\\dev\\workspace\\target"
     * @param srcEncoding 源编码,中:"GBK"
     * @param targetEncoding 转换后的编码,中:"UTF-8"
     */
    public static void fileEncodingConvert(String srcPath, String targetPath, String srcEncoding, String targetEncoding) {
        //获取所有java文件
        Collection<File> srcFileCol = FileUtils.listFiles(new File(srcPath), new String[]{"java"}, true);

        for (File srcFile : srcFileCol) {
            //UTF8格式文件路径
            String targetFilePath = targetPath + srcFile.getAbsolutePath().substring(srcPath.length());
            //使用GBK读取数据，然后用UTF-8写入数据
            try {
                FileUtils.writeLines(new File(targetFilePath), targetEncoding, FileUtils.readLines(srcFile, srcEncoding));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
