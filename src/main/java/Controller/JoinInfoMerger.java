package Controller;

import java.util.*;

public class JoinInfoMerger {

    public static Map<Integer, ArrayList<long[]>> merge(List<Map<Integer,
            ArrayList<long[]>>> pkJoinInfoList, int pkvsMaxSize) {
        Map<Integer, ArrayList<long[]>> mergedPkJoinInfo = pkJoinInfoList.get(0);

        for (int i = 1; i < pkJoinInfoList.size(); i++) {
            Map<Integer, ArrayList<long[]>> pkJoinInfo = pkJoinInfoList.get(i);
            Iterator<Map.Entry<Integer, ArrayList<long[]>>> iterator = pkJoinInfo.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<Integer, ArrayList<long[]>> entry = iterator.next();
                if (!mergedPkJoinInfo.containsKey(entry.getKey())) {
                    mergedPkJoinInfo.put(entry.getKey(), entry.getValue());
                } else {
                    ArrayList<long[]> list = mergedPkJoinInfo.get(entry.getKey());
                    list.addAll(entry.getValue());
                    Collections.shuffle(list);
                    if (list.size() > pkvsMaxSize) {
                        list = new ArrayList<long[]>(list.subList(0, pkvsMaxSize));
                    }
                    mergedPkJoinInfo.put(entry.getKey(), list);
                }
            }
        }
        return mergedPkJoinInfo;
    }
}
