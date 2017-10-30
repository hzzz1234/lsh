package com.dianping.search.offline.utils;

import com.github.fommil.netlib.F2jBLAS;

/**
 * Created by zhen.huaz on 2017/10/30.
 */
public class VectorUtils {
    public static F2jBLAS f2jBLAS = new F2jBLAS();

    public static double dotT(double v2[]){
        return f2jBLAS.dnrm2(v2.length,v2,0,1);
    }
}
