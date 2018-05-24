/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.dz3.mahoutrecommender;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 *
 * @author bruno
 */
public class UserBasedRecommend {

    DataModel model;

    public UserBasedRecommend(DataModel model) {
        this.model = model;
    }

    public Recommender buildRecommender() throws TasteException {
        // initialize user similarity        
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

        // initialize nearest neighbours
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(model.getNumUsers(), 0.1, similarity, model);

        // initialize user based recommender
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

        return recommender;
    }

    public void top(int users, int recs) throws TasteException, IOException {
        LongPrimitiveIterator it = model.getUserIDs();
        Recommender r = buildRecommender();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("top.csv"))) {
            for (int i = 0; i < users; i++) {
                // get user id
                long id = it.nextLong();
                for (RecommendedItem ri : r.recommend(id, recs)) {
                    writer.write(String.format("%d,%d,%f%n", id, ri.getItemID(), ri.getValue()));
                }
            }
        }
    }
}
