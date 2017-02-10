package com.he.mllib.vector; /**
 * Created by hx on 2017/1/22.
 */
import groovy.ui.SystemOutputInterceptor;
import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.ArrayList;
import java.util.List;

//import org.apache.spark.mllib.util.MLUtils;
public class SparkDataType {
    @SuppressWarnings("unused")
    public static void main(String[] args) {
        //局部向量
        //Create a dense vector(1.0,0.0,3.0)
        Vector dv = Vectors.dense(1.0, 0.0, 3.0);
        // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
        Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
        System.out.println(dv);
        System.out.println(sv);
        //标记点
        // Create a labeled point with a positive label and a dense feature vector.
        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

        // Create a labeled point with a negative label and a sparse feature vector.
        LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));

        //稀疏数据 MLUtils.loadLibSVMFile reads training examples stored in LIBSVM format.
        //LIBSVM 是 一个简单、易于使用和快速有效的SVM模式识别与回归的软件包。

        //源码解读                                             http://blog.csdn.net/wangzfox/article/details/45787725
        SparkContext jsc = null;
//		JavaRDD<LabeledPoint> examples = MLUtils.loadLibSVMFile(jsc, "data/mllib/sample_libsvm_data.txt").toJavaRDD(); //     jsc.sc()  ---jsc的原属性待定

        //Local matrix
        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

        //http://blog.csdn.net/sinat_29508201/article/details/54089771   参考
        // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
        Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});

        System.out.println(dm+"------------");
        System.out.println(sm);

        System.out.print("111111");

        List<String> a = new ArrayList<String>();
    }
}
