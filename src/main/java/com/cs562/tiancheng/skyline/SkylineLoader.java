package com.cs562.tiancheng.skyline;

import com.github.davidmoten.rtree.*;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observables.StringObservable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;



public class SkylineLoader {
    private final static Precision precision = Precision.DOUBLE;

    private RTree<Object, Point> tree = RTree.star().create();

    public RTree<Object, Point> getTree() {
        return tree;
    }

    public void setInputList(List<Entry<Object, Point>> inputList) {
        this.inputList = inputList;
    }

    public void setTree(RTree<Object, Point> tree) {
        this.tree = tree;
    }

    private List<Entry<Object, Point>> inputList = new ArrayList<Entry<Object, Point>>();

    public List<Entry<Object, Point>> getInputList() {
        return inputList;
    }



    public static Observable<Entry<Object, Point>> entries(final Precision precision) {
        Observable<String> source = Observable.using(new Func0<InputStream>() {
            @Override
            public InputStream call() {
                try {
                    InputStream inputStream = new GZIPInputStream(SkylineLoader.class
                            .getResourceAsStream("/greek-earthquakes-1964-2000.txt.gz"));
                    return new GZIPInputStream(SkylineLoader.class
                            .getResourceAsStream("/greek-earthquakes-1964-2000.txt.gz"));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, new Func1<InputStream, Observable<String>>() {
            @Override
            public Observable<String> call(InputStream is) {
                return StringObservable.from(new InputStreamReader(is));
            }
        }, new Action1<InputStream>() {
            @Override
            public void call(InputStream is) {
                try {
                    is.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return StringObservable.split(source, "\n")
                .flatMap(new Func1<String, Observable<Entry<Object, Point>>>() {

                    @Override
                    public Observable<Entry<Object, Point>> call(String line) {
                        if (line.trim().length() > 0) {
                            String[] items = line.split(" ");
                            double lat = Double.parseDouble(items[0]);
                            double lon = Double.parseDouble(items[1]);
                            Entry<Object, Point> entry;
                            if (precision == Precision.DOUBLE)
                                entry = Entries.entry(new Object(), Geometries.point(lat, lon));
                            else
                                entry = Entries.entry(new Object(),
                                        Geometries.point((float) lat, (float) lon));
                            return Observable.just(entry);
                        } else
                            return Observable.empty();
                    }
                });
    }

    /**
     * Reading in data from txt file
     */
    static List<Entry<Object, Point>> entriesList(Precision precision) {
        List<Entry<Object, Point>> result = entries(precision).toList().toBlocking().single();
        System.out.println("loaded greek earthquakes into list");
        return result;
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
        skylineLoader.setInputList(SkylineLoader.entriesList(SkylineLoader.precision));
        skylineLoader.insertTree();
        list = skylineLoader.findSkylinePoints(skylineLoader.getTree());
        System.out.println("Number of Skyline Points: "+list.size());
        for(Entry<Object, Point> entry : list){
            System.out.println("X:"+entry.geometry().x() +", Y:"+ entry.geometry().y());
        }

    }


}


