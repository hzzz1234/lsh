package com.dianping.search.offline.utils;

import com.github.fommil.netlib.F2jBLAS;

/**
 * Created by zhen.huaz on 2017/10/30.
 */
public class VectorUtils {
    public static F2jBLAS f2jBLAS = new F2jBLAS();

    public static double dotT(double v2[]){
        double score = 0;
        for(int i=0;i<v2.length;i++){
            score+=v2[i]*v2[i];
        }
        return score;
    }

}
