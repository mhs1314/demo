package com.mhs;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DemoApplication {
    public static final String master = "local";

    public static void main(String[] args) {

        // 创建一个Java版本的Spark Context
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取我们的输入数据
        JavaRDD<String> input = sc.textFile("D:\\git\\demo\\src\\main\\resources\\res.txt");

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(s -> s * s);
        JavaRDD<String> unio=input.union(rdd.map(s->s+""));
        System.out.println(StringUtils.join(result.collect(), ","));
         //笛卡尔积 相似度时
        JavaPairRDD<Integer, String> dkr= result.cartesian(input);
        List<Tuple2<Integer, String>> list=dkr.collect();
        System.out.println("Tuple2"+list.toArray().toString());
        // 切分为单词
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) {
                        System.out.println("word" + x);
                        Iterator<String> iterator = Arrays.asList(x.split(" ")).iterator();
                        System.out.println("iterator" + iterator);
                        return iterator;
                    }
                });
        //取前2条
        for (String line : words.take(2)) {
            System.out.println("take" + line);
        }
        //取全部
        for (String line : words.collect()) {
            System.out.println("collect" + line);
        }
        //去重
        words = words.distinct();
        for (String line : words.collect()) {
            System.out.println("distinct" + line);
        }
        //合并的
        for (String line : unio.collect()) {
            System.out.println("unio" + line);
        }
        //只返回包含input的
        input= unio.intersection(input);
        for (String line : words.collect()) {
            System.out.println("intersection" + line);
        }
        //返回不包含words的
        input=unio.subtract(input);
        for (String line : input.collect()) {
            System.out.println("subtract" + line);
        }


        JavaRDD<String> errors = words.filter(new Function<String, Boolean>() {
            public Boolean call(String x) {
                return x.contains("error");
            }
        });
        // 转换为键值对并计数
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        System.out.println(x);
                        return new Tuple2(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                System.out.println("x:" + x + " y");
                return x + y;
            }
        });

        JavaRDD<String> errorsRDD = input.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String x) {
                        return x.contains("hadoop");
                    }
                }
        );
        JavaRDD<String> errors1 = input.filter(s -> s.contains("word"));
        for (String line : errors1.collect()) {
            System.out.println("errors1" + line);
        }
        // 将统计出来的单词总数存入一个文本文件，引发求值
        System.out.println(counts.count());
        System.out.println(counts.values().toString());
        // counts.saveAsTextFile("D:\\git\\demo\\src\\main\\resources\\word.txt");
        System.out.println("end");
    }
}