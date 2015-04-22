package com.voxlearning.bigdata.MrOutput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class MultipleTextFiles extends MultipleTextOutputFormat<Text, Text> {
    /**
     * 目前在 luiti 里采用的 reducer 函数里面是用
     *     yield "", "{"json key": "json value"}"
     * 格式的。所以现在分多个目录的话，就用没用上的 key 就好了。
     *
     * 代码参考: http://blog.csdn.net/lmc_wy/article/details/7532213
     */

    protected String generateFileNameForKeyValue(Text key, Text value, String name)
    {
        String outputName = key.toString();      // 取得当前文件名
        key.set("");                             // 输出里只需要 value(json 格式) 就可以了。
        return new Path(outputName, name).toString();   // 参考 https://github.com/klbostee/feathers
    }

}


/*
 * deploy ref: https://github.com/klbostee/feathers/blob/master/build.sh
 */
