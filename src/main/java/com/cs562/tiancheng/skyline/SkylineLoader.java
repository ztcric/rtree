package com.cs562.tiancheng.skyline;

import com.github.davidmoten.rtree.*;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class SkylineLoader {

    private RTree<Object, Point> tree = RTree.star().create();

    public RTree<Object, Point> getTree() {
        return tree;
    }

    public void setTree(RTree<Object, Point> tree) {
        this.tree = tree;
    }

    private List<Entry<Object, Point>> inputList = new ArrayList<Entry<Object, Point>>();

    public List<Entry<Object, Point>> getInputList() {
        return inputList;
    }


    /**
     * Reading in data from txt file
     */
    public void readIn(){
        File file = new File("/Users/tianchengzhu/Downloads/greek-earthquakes-1964-2000.txt");

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            String st;
            Entry<Object, Point> entry;
            while ((st = br.readLine()) != null) {
                String[] strings = new String[2];
                strings = st.split(" ");
                double x = Double.parseDouble(strings[0]);
                double y = Double.parseDouble(strings[1]);
                entry = Entries.entry(new Object(), Geometries.point(x, y));
                inputList.add(entry);
            }
        }catch (Exception e){

        }
    }

    /**
     * Insert the list of Entries into R-Tree
     */
    public void insertTree(){
        for(Entry<Object, Point> entry : inputList){
            tree = tree.add(entry);
        }
    }

    class NodeComparator implements Comparator<Node<Object, Point>> {

        @Override
        public int compare(Node<Object, Point> o1, Node<Object, Point> o2) {
            if((o1.geometry().mbr().x1() + o1.geometry().mbr().y1()) > (o2.geometry().mbr().x1() + o2.geometry().mbr().y1())){
                return 1;
            }
            else if ((o1.geometry().mbr().x1() + o1.geometry().mbr().y1()) <  (o2.geometry().mbr().x1() + o2.geometry().mbr().y1())){
                return -1;
            }
            else {
                return 0;
            }
        }
    }

    class EntryComparator implements Comparator<Entry<Object, Point>>{
        @Override
        public int compare(Entry<Object, Point> o1, Entry<Object, Point> o2) {
            if((o1.geometry().mbr().x1() + o1.geometry().mbr().y1()) > (o2.geometry().mbr().x1() + o2.geometry().mbr().y1())){
                return 1;
            }
            else if ((o1.geometry().mbr().x1() + o1.geometry().mbr().y1()) <  (o2.geometry().mbr().x1() + o2.geometry().mbr().y1())){
                return -1;
            }
            else {
                return 0;
            }
        }
    }

    /**
     * Find all skyline points by passing in the R-Tree
     * @param rTree
     * @return return a list of Entries
     */
    public List<Entry<Object, Point>> findSkylinePoints(RTree<Object, Point> rTree){
        Double xLowerBound = Double.MAX_VALUE;
        Double yLowerBound = Double.MAX_VALUE;

        PriorityQueue<Node<Object, Point>> priorityQueue = new PriorityQueue<Node<Object, Point>>(new NodeComparator());
        List<Entry<Object, Point>> result = new ArrayList<Entry<Object, Point>>();
        Node root = rTree.root().or(null);

        if(root != null){

            priorityQueue.add(root);
            Node head =priorityQueue.poll();
            while(head != null) {
                if (head instanceof NonLeaf) {
                    NonLeaf<Object, Point> nonLeaf = (NonLeaf<Object, Point>) head;
                    List<Node<Object, Point>> children = nonLeaf.children();
                    for (Node<Object, Point> node : children) {
                        priorityQueue.add(node);
                    }
                }
                else if (head instanceof Leaf) {

                    Leaf<Object, Point> leaf = (Leaf<Object, Point>) head;
                    List<Entry<Object, Point>> entries = leaf.entries();
                    PriorityQueue<Entry<Object, Point>> entriesQueue = new PriorityQueue(new EntryComparator());
                    entriesQueue.addAll(entries);

                    while(entriesQueue.size() > 0){
                        Entry<Object, Point> temp = entriesQueue.poll();
                        if(checkValidEntry(temp, xLowerBound, yLowerBound)){
                            result.add(temp);
                            // Updating dominance region boundaries.
                            if(temp.geometry().y() < yLowerBound){
                                yLowerBound = temp.geometry().y();
                            }
                            if(temp.geometry().x() < xLowerBound){
                                xLowerBound = temp.geometry().x();
                            }
                        }
                    }
                    // updating heap, remove anything that is in dominance region
                    PriorityQueue<Node<Object, Point>> priorityQueue_temp = new PriorityQueue<>(new NodeComparator());
                    for (Node<Object, Point> e : priorityQueue) {
                        if (e.geometry().mbr().x1() >= xLowerBound && e.geometry().mbr().y1() >= yLowerBound) {
                        } else {
                            priorityQueue_temp.add(e);
                        }
                    }
                    priorityQueue = priorityQueue_temp;
                }
                head = priorityQueue.poll();
            }
        }
        result.sort((o1, o2) -> {
            if(o1.geometry().x() - o2.geometry().x() > 0){
                return 1;
            }
            else if(o1.geometry().x() - o2.geometry().x() < 0){
                return -1;
            }
            else{
                return 0;
            }
        });
        return result;
    }


    /**
     * Check if the entry is outside of the dominance region.
     * @param entry
     * @param xLowerBound
     * @param yLowerBound
     * @return
     */
    public boolean checkValidEntry(Entry<Object, Point> entry, Double xLowerBound, Double yLowerBound){
        if(entry.geometry().x() < xLowerBound || entry.geometry().y() < yLowerBound){
            return true;
        }
        else {
            return false;
        }
    }

    public static void main(String[] args) {
        SkylineLoader skylineLoader;
        List<Entry<Object, Point>> list;
        skylineLoader = new SkylineLoader();
        skylineLoader.readIn();
        skylineLoader.insertTree();
        list = skylineLoader.findSkylinePoints(skylineLoader.getTree());
        System.out.println("Number of Skyline Points: "+list.size());

        for(Entry<Object, Point> entry : list){
            System.out.println("X:"+entry.geometry().x() +", Y:"+ entry.geometry().y());
        }

    }


}


