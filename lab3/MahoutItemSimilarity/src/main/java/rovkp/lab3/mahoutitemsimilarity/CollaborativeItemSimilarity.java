/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab3.mahoutitemsimilarity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class CollaborativeItemSimilarity implements ItemSimilarity {

    private final double[][] matrix;
    private final double[] norms;
    private final Map<Integer, Long> seqIdMap;
    private final Map<Long, Integer> idSeqMap;

    public CollaborativeItemSimilarity(DataModel model) throws TasteException {
        int n = model.getNumItems();
        matrix = new double[n][n];
        norms = new double[n];
        seqIdMap = new HashMap<>();
        idSeqMap = new HashMap<>();

        calculateCollaborativeModelMatrix(model);
    }

    private void calculateCollaborativeModelMatrix(DataModel model) throws TasteException {

        int counter = 0;
        LongPrimitiveIterator iterator = model.getUserIDs();
        while (iterator.hasNext()) {
            long userId = iterator.nextLong();

            for (long ratedItemId1 : model.getItemIDsFromUser(userId)) {

                //get correct item1 seq num
                Integer seqId1 = idSeqMap.get(ratedItemId1);
                if (seqId1 == null) {
                    seqId1 = counter++;
                    seqIdMap.put(seqId1, ratedItemId1);
                    idSeqMap.put(ratedItemId1, seqId1);
                }

                norms[seqId1] += Math.pow(model.getPreferenceValue(userId, ratedItemId1), 2);

                for (long ratedItemId2 : model.getItemIDsFromUser(userId)) {

                    //get correct item2 seq num
                    Integer seqId2 = idSeqMap.get(ratedItemId2);
                    if (seqId2 == null) {
                        seqId2 = counter++;
                        seqIdMap.put(seqId2, ratedItemId2);
                        idSeqMap.put(ratedItemId2, seqId2);
                    }

                    matrix[seqId1][seqId2] += model.getPreferenceValue(userId, ratedItemId1)
                            * model.getPreferenceValue(userId, ratedItemId2);
                }
            }
        }

        //get cosine similarity from similarity sums
        for (int seqId1 = 0; seqId1 < matrix.length; seqId1++) {
            for (int seqId2 = 0; seqId2 < matrix.length; seqId2++) {
                if (matrix[seqId1][seqId2] != 0) {
                    matrix[seqId1][seqId2] /= Math.sqrt(norms[seqId1]) * Math.sqrt(norms[seqId2]);
                }
            }
        }
    }

    @Override
    public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
        int seq1 = idSeqMap.get(itemID1);
        int seq2 = idSeqMap.get(itemID2);
        return matrix[seq1][seq2];
    }

    @Override
    public double[] itemSimilarities(long itemID1, long[] itemID2s) throws TasteException {
        int seq1 = idSeqMap.get(itemID1);
        double[] similarities = new double[itemID2s.length];
        int i = 0;
        for (long id : itemID2s){
            similarities[i] = matrix[seq1][idSeqMap.get(id)];
            i++;
        }
        return similarities;
    }

    @Override
    public long[] allSimilarItemIDs(long itemID) throws TasteException {
        int seq1 = idSeqMap.get(itemID);
        List<Long> items = new ArrayList<>();
        seqIdMap.keySet().stream().filter((id) -> (matrix[seq1][id] > 0.0 && seq1 != id)).forEachOrdered((id) -> {
            items.add(seqIdMap.get(id));
        });
        long[] result = new long[items.size()];
        int i = 0;
        for(long item : items){
            result[i] = item;
            i++;            
        }
        return result;
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
