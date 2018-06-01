/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab3.mahoutitemsimilarity;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author bruno
 */
public class HybridSimilarity {

    static final String SIMILARITY = "item_similarity.csv";
    static final String RATINGS = "jester_ratings.csv";
    static final String PATH = "hybrid_similarity.csv";

    public static void main(String[] args) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(RATINGS));
        ItemSimilarity sim = new FileItemSimilarity(new File(SIMILARITY));
        CollaborativeItemSimilarity coll = new CollaborativeItemSimilarity(model);

        double factor = 0.5;

        try (PrintWriter writer = new PrintWriter(new FileWriter(PATH))) {
            for (int i = 1; i <= 150; i++) {
                for (int j = i + 1; j <= 150; j++) {
                    try {
                        double coef = factor * sim.itemSimilarity(i, j)
                                + (1 - factor) * coll.itemSimilarity(i, j);
                        if (!Double.isNaN(coef)) {
                            writer.println(i + "," + j + "," + coef);
                        }
                        System.out.println("Success");
                    } catch (TasteException te) {
                        System.err.println("File exception");
                    } catch (NullPointerException npe){
                        System.err.println("No similarity");
                    }
                }
            }
        }
    }
}
