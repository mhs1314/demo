package com.mhs.pairRdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Map;

public class CombineByKey {
    public static class AvgCount implements Serializable {
        public AvgCount(int total, int num) {   total_ = total;  num_ = num; }
        public int total_;
        public int num_;
        public float avg() {   return total_/(float)num_; }
    }
    public static void main(String[] args){

        Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
            public AvgCount call(Integer x) {
                return new AvgCount(x, 1);
            }
        };
        Function2<AvgCount, Integer, AvgCount> addAndCount =
                new Function2<AvgCount, Integer, AvgCount>() {
                    public AvgCount call(AvgCount a, Integer x) {
                        a.total_ += x;
                        a.num_ += 1;
                        return a;
                    } };
        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) {
                        a.total_ += b.total_;
                        a.num_ += b.num_;
                        return a;
                    } };

        AvgCount initial = new AvgCount(0,0);
        JavaPairRDD<String, AvgCount> avgCounts =
                nums.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Map.Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }


}
