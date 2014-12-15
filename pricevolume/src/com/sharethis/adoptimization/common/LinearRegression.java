package com.sharethis.adoptimization.common;

import java.util.List;

import org.apache.log4j.Logger;

public class LinearRegression { 
	private static final Logger sLogger = Logger.getLogger(LinearRegression.class);
	
	private List<Double> x;
	private List<Double> y;
	private double beta0 = 0;
	private double beta1 = 0;
	
	public LinearRegression(List<Double> x, List<Double> y){
		this.x = x;
		this.y = y;
	}

    public double generatingLRM() { 
//        sLogger.info("Linear Regression starts ...");
        double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;
        int n = x.size();
        for(int i=0; i<n; i++) {
        	double x_tmp = (Double) x.get(i);
            sumx  += x_tmp;
            sumx2 += x_tmp * x_tmp;
            sumy  += (Double) y.get(i);
        }
        double xbar = sumx / n;
        double ybar = sumy / n;

        double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
        for (int i=0; i<n; i++) {
        	double x_tmp = (Double) x.get(i);
        	double y_tmp = (Double) y.get(i);       	
            xxbar += (x_tmp - xbar) * (x_tmp - xbar);
            yybar += (y_tmp - ybar) * (y_tmp - ybar);
            xybar += (x_tmp - xbar) * (y_tmp - ybar);
        }
        beta1 = xybar / xxbar;
        beta0 = ybar - beta1 * xbar;

        //sLogger.info("y = " + beta0 + " + " + beta1 + " * x ");

        int df = n - 2;
        double rss = 0.0;      // residual sum of squares
        double ssr = 0.0;      // regression sum of squares
        for (int i=0; i<n; i++) {
        	double x_tmp = (Double) x.get(i);
        	double y_tmp = (Double) y.get(i);       	
            double fit = beta0 + beta1*x_tmp;
            rss += (fit-y_tmp)*(fit-y_tmp);
            ssr += (fit-ybar)*(fit-ybar);
        }
        double r_square = ssr/yybar;
        double svar  = rss/df;
        double svar1 = svar/xxbar;
        double svar0 = svar/n + xbar*xbar*svar1;
        //sLogger.info("r_square            = " + r_square);
        //sLogger.info("std error of beta_1 = " + Math.sqrt(svar1));
        //sLogger.info("std error of beta_0 = " + Math.sqrt(svar0));
        svar0 = svar * sumx2 / (n * xxbar);
        //sLogger.info("std error of beta_0 = " + Math.sqrt(svar0));

        //sLogger.info("SSTO = " + yybar);
        //sLogger.info("SSE  = " + rss);
        //sLogger.info("SSR  = " + ssr);
//        sLogger.info("Linear Regression is done.");
        return r_square;
    }
    
    public double getBeta0(){
    	return beta0;
    }
    
    public double getBeta1(){
    	return beta1;
    }
}
