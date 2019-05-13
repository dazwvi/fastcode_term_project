package com.example.java;

import java.lang.reflect.Array;
import java.util.*;
import java.io.*;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.BufferedWriter;


public class PageRank {
    HashMap<Integer, ArrayList<Integer>> graph;
    HashMap<Integer, ArrayList<Integer>> M;
    Set<Integer> links;
//    public double pagerank[];
    HashMap<Integer, Double> pagerank;
    HashMap<Integer, Double> pagerank_prev;
//    public double pagerank_prev[];
    HashMap<Integer, Integer> outdeg;
    public double dampingFactor;
    public int numUrls;
    public double epsilon;

    public void init(int numUrls) {
        this.numUrls = numUrls;
        this.epsilon = 0.00000001;
        dampingFactor = 0.85;

        pagerank = new HashMap<Integer, Double>(numUrls);
        pagerank_prev = new HashMap<Integer, Double>(numUrls);
        for (int key: graph.keySet()){
            pagerank.put(key, 1.0/numUrls);
            pagerank_prev.put(key, 1.0/numUrls);
        }

        outdeg = new HashMap<Integer, Integer>(numUrls);
        for (int key: graph.keySet()){
            outdeg.put(key, graph.get(key).size());
        }
//        System.out.println("Outdegrees:");
//        for(int r:outdeg.keySet()) {
//            System.out.print("key: " +r +" "+ outdeg.get(r) + " ");
//        }
//        System.out.println();
        calcPR();
    }

    public void calcPR() {
        for(int i:pagerank.keySet()) {

            pagerank.put(i, (((1 - dampingFactor) / numUrls) + (dampingFactor * (outdegCalc(i)))));
        }
    }

    public double outdegCalc(int i) {
        double result = 0.0;
        try{
            for(int node : M.get(i)){
                result += pagerank_prev.get(node) / outdeg.get(node);
            }
        }catch (NullPointerException e) {
            return 0.0;
        }
        return result;
    }

    public boolean hasConverged() {
        for(int node: pagerank.keySet()) {
            if(Math.abs(pagerank.get(node) - pagerank_prev.get(node)) > epsilon) {
                for(int j : pagerank.keySet()) {
                    pagerank_prev.put(j, pagerank.get(j));
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
//        String urlPath = args[2];

        int ite = 1;

        try {
            BufferedReader br = new BufferedReader(new FileReader(inputPath));
            pr.graph = new HashMap<Integer, ArrayList<Integer>>(numUrls);
            pr.M = new HashMap<Integer, ArrayList<Integer>>(numUrls);
//            pr.links = new HashSet<Integer>();
            while ((line = br.readLine()) != null) {
                String[] buffer = line.split("\\s+");
                if(buffer[0].equals("#")) continue;;
                int i = Integer.parseInt(buffer[0]);
                int j = Integer.parseInt(buffer[1]);
                ArrayList<Integer> g_arrli = new ArrayList<Integer>();
                ArrayList<Integer> m_arrli = new ArrayList<Integer>();
//                pr.links.add(j);
                if(pr.graph.get(i) != null) {
                    pr.graph.get(i).add(j);
                }
                else {
                    g_arrli.add(j);
                    pr.graph.put(i, g_arrli);
                }
                if(pr.M.get(j) != null) {
                    pr.M.get(j).add(i);
                }
                else {
                    m_arrli.add(i);
                    pr.M.put(j, m_arrli);
                }
            }
//            System.out.println("Set:");
//            for(int link : pr.links) {
//                if(!pr.graph.containsKey(link)) {
//                    ArrayList<Integer> arr = new ArrayList<Integer>();
//                    for(int node : pr.graph.keySet()) {
//                        if(node != link) {
//                            arr.add((node));
//                        }
//                    }
//                    pr.graph.put(link, arr);
//                }
//                System.out.println(link + " ");
//            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        pr.init(numUrls);
        while (!pr.hasConverged()) {
            pr.calcPR();
            ite += 1;
        }

//        try {
//            BufferedReader br = new BufferedReader(new FileReader(urlPath));
//            System.out.printf("Page Rank: \n");
//            while ((line = br.readLine()) != null) {
//                String[] buffer = line.split(" ");
//                int i = Integer.parseInt(buffer[0]);
//                String url = buffer[1].replaceAll("%", "%%");
//                System.out.printf(pr.pagerank[i] + ": "+ i + "." + url + "\n");
//            }
//        }catch (IOException e) {
//            e.printStackTrace();
//        }
        for(int r:pr.pagerank.keySet())
            System.out.printf(r +": "+pr.pagerank.get(r) + "\n");
//        for (int key: pr.graph.keySet()){
//            System.out.print("Key: " +key+ " values: [");
//            for(int j : pr.graph.get(key))
//                System.out.print(j + " ");
//            System.out.println("]");
//        }
//        System.out.println();
//        for (int key: pr.M.keySet()){
//            System.out.print("Key: " +key+ " values: [");
//            for(int j : pr.M.get(key))
//                System.out.print(j + " ");
//            System.out.println("]");
//        }
        System.out.println("Number of iterations: \n" + ite);
        double sum_ = 0;
        for(int r:pr.pagerank.keySet()) {
            sum_ += pr.pagerank.get(r);
        }
        System.out.println("Sum of ranks: " + sum_);
    }
}
