/**
 * Created by qingping on 11/22/16.
 */
import java.util.*;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
public class PrefixSpanCSR {
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setAppName("Simple Application")
                .setMaster("local[4]");//use 4 cores
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<List<List<Integer>>> sequences = sc.parallelize(Arrays.asList(
                Arrays.asList(Arrays.asList(1), Arrays.asList(3)),
                Arrays.asList(Arrays.asList(1), Arrays.asList(3), Arrays.asList(4)),
                Arrays.asList(Arrays.asList(1), Arrays.asList(5)),
                Arrays.asList(Arrays.asList(6))
        ), 2);
        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(0.5)
                .setMaxPatternLength(5);
        PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
        for (PrefixSpan.FreqSequence<Integer> freqSeq: model.freqSequences().toJavaRDD().collect()) {
            System.out.println("pattern:" + freqSeq.javaSequence() + ", " + freqSeq.freq());
        }
    }
}
