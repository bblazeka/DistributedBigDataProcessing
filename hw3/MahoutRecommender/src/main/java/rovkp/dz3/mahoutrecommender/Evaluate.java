/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.dz3.mahoutrecommender;

import java.io.File;
import java.io.IOException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author bruno
 */
public class Evaluate {

    public static void main(String[] args) throws IOException, TasteException {
        String ratings = "jester_ratings.dat";
        String similarities = "item_similarity.csv";
        // initialize ratings model
        DataModel model = new FileDataModel(new File(ratings));

        RecommenderBuilder builderIB = (DataModel model1) -> {
            ItemSimilarity similarity = new FileItemSimilarity(new File(similarities));
            return (new ItemBasedRecommend()).buildRecommender(model1, similarity);
        };

        // evaluate
        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(builderIB, null, model, 0.300001, 0.3);
        System.out.println("item based: "+score);
        
        RecommenderBuilder builder = (DataModel model1) -> {
            return (new UserBasedRecommend(model1)).buildRecommender();
        };
        
        score = recEvaluator.evaluate(builder, null, model, 0.300001, 0.3);
        System.out.println("user based: "+score);
    }
}
