package com.example.java;

import java.lang.reflect.Array;
import java.util.*;
import java.io.*;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.BufferedWriter;

public class PageRank {
    public int graph[][];
    public double pagerank[];
    public double pagerank_prev[];
    public int outdeg[];
    public double dampingFactor;
    public int numUrls;
    public double epsilon;

    public void init(int numUrls) {
        this.numUrls = numUrls;
        this.epsilon = 0.00000001;
        dampingFactor = 0.85;

        pagerank = new double[numUrls+1];
        pagerank_prev = new double[numUrls+1];
        for(int i=1; i<=numUrls; ++i) {
            pagerank[i] = 1.0/numUrls;
            pagerank_prev[i] = 1.0/numUrls;
        }

        outdeg = new int[numUrls+1];
        for(int i=1; i<=numUrls; ++i) {
            for(int one : graph[i])
                outdeg[i] += one;
            if(outdeg[i] == 0) outdeg[i] = 1;
        }
        for(int r:outdeg) {
            System.out.print(r + " ");
        }
        System.out.println();
        calcPR();
    }

    public void calcPR() {
        for(int i=1; i<=numUrls; ++i) {
            pagerank[i] = ((1 - dampingFactor) / numUrls) + (dampingFactor * (outdegCalc(i)));
        }
    }

    public double outdegCalc(int i) {
        double result = 0.0;
        for(int j=1; j<=numUrls; ++j) {
            if(graph[j][i] == 1) {
                result += pagerank_prev[j] / outdeg[j];
            }
        }
        return result;
    }

    public boolean hasConverged() {
        for(int i=1; i<=numUrls; ++i) {
            if(Math.abs(pagerank[i] - pagerank_prev[i]) > epsilon) {
                for(int j=1; j<=numUrls; ++j) {
                    pagerank_prev[j] = pagerank[j];
                }
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        PageRank pr = new PageRank();
        String line = null;
        int numUrls = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String urlPath = args[2];

        int ite = 1;

        try {
            BufferedReader br = new BufferedReader(new FileReader(inputPath));
            pr.graph = new int[numUrls+1][numUrls+1];
            while ((line = br.readLine()) != null) {
                String[] buffer = line.split(" ");
                int i = Integer.parseInt(buffer[0]);
                int j = Integer.parseInt(buffer[1]);
                pr.graph[i][j] = 1;
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        pr.init(numUrls);
        while (!pr.hasConverged()) {
            pr.calcPR();
            ite += 1;
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(urlPath));
            System.out.printf("Page Rank: \n");
            while ((line = br.readLine()) != null) {
                String[] buffer = line.split(" ");
                int i = Integer.parseInt(buffer[0]);
                String url = buffer[1];
                System.out.printf(String.format("%.3f", pr.pagerank[i]) + ": "+ i + "." + url +"\n");
            }
            System.out.println("\n" + ite);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
