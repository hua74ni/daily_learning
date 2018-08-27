package com.storm1.groups;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class ModuleGrouping implements CustomStreamGrouping, Serializable {

    int numTasks = 0;


    @Override
    public void prepare(int i) {
        this.numTasks = i;
    }

    @Override
    public List<Integer> taskIndices(Tuple tuple) {

        List<Integer> boltIds = new ArrayList<Integer>();
        List<Object> values = tuple.getValues();

        if(values.size()>0){
            String str = values.get(0).toString();
            if(str.isEmpty()){
                boltIds.add(0);
            }else{
                boltIds.add(str.charAt(0) % numTasks);
            }
        }
        return boltIds;
    }
}
