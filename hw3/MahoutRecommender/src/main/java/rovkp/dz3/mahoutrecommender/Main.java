/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.dz3.mahoutrecommender;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author bruno
 */
public class Main {

    public static void main(String[] args) throws IOException, TasteException {
        String ratings, similarities;
        if (args.length != 2) {
            ratings = "jester_ratings.dat";
            similarities = "item_similarity.csv";
        } else {
            ratings = args[0];
            similarities = args[1];
        }

        // initialize similarities
        ItemSimilarity similarity = new FileItemSimilarity(new File(similarities));
        // initialize ratings model
        DataModel model = new FileDataModel(new File(ratings));

        System.out.println("Item based recommend");
        Recommender IBrecommender = (new ItemBasedRecommend()).buildRecommender(model, similarity);

        List<RecommendedItem> IBrecommendations = IBrecommender.recommend(220, 10);
        IBrecommendations.forEach((recommendation) -> {
            System.out.println(recommendation);
        });

        System.out.println("User based recommend");
        UserBasedRecommend ubr = new UserBasedRecommend(model);
        Recommender recommender = ubr.buildRecommender();
        // top 10 recommendations for user 220
        List<RecommendedItem> recommendations = recommender.recommend(220, 10);
        recommendations.forEach((recommendation) -> {
            System.out.println(recommendation);
        });
        
        System.out.println("100 users, 10 rec");
        ubr.top(100,10);
    }
}
