package com.cs562.tiancheng.skyline;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.Precision;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Point;
import org.junit.Before;
import org.junit.Test;

import javax.swing.border.EmptyBorder;
import java.util.List;

import static org.junit.Assert.*;

public class SkylineLoaderTest {
    SkylineLoader skylineLoader;
    List<Entry<Object, Point>> list;
    @Before
    public void setUp() throws Exception {
        skylineLoader = new SkylineLoader();
        skylineLoader.readIn();
        skylineLoader.insertTree();
        list = skylineLoader.findSkylinePoints(skylineLoader.getTree());
    }

    @Test
    public void readIn() {

        assertTrue(skylineLoader.getInputList().size() > 0);


    }

    @Test
    public void insertTree() {
        assertTrue(skylineLoader.getTree().size() > 0);

    }

    @Test
    public void findSkylinePoints() {
        System.out.println("Number of Skyline Points: "+list.size());

        for(Entry<Object, Point> entry : list){
            System.out.println("X:"+entry.geometry().x() +", Y:"+ entry.geometry().y());
        }
    }
}