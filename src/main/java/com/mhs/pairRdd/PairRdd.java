package com.mhs.pairRdd;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRdd {
    public static final String master = "local";
    public static void main(String[] args) {
        // 创建一个Java版本的Spark Context
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取我们的输入数据
        JavaRDD<String> input = sc.textFile("/Users/mac/IdeaProjects/demo/src/main/resources/res.txt");
        //定义数据
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        //计算平方
        JavaRDD<Integer> result = rdd.map(s -> s * s);
        //union 操作
        JavaRDD<String> unio=input.union(rdd.map(s->s+""));
        //合并
        System.out.println(StringUtils.join(result.collect(), ","));
        //笛卡尔积
        /**
         * 例如，A={a,b}, B={0,1,2}，则
         *
         * A×B={(a, 0), (a, 1), (a, 2), (b, 0), (b, 1), (b, 2)}
         * B×A={(0, a), (0, b), (1, a), (1, b), (2, a), (2, b)}
         */
        JavaPairRDD<Integer, String> dkr= result.cartesian(input);

        List<Tuple2<Integer, String>> list=dkr.collect();
        for (Tuple2 tuple2:list){
            System.out.println("Tuple2_1"+tuple2._1);
            System.out.println("Tuple2_2"+tuple2._2);
        }
    }
}
