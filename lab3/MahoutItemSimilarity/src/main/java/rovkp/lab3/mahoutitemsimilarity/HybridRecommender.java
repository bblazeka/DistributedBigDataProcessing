/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab3.mahoutitemsimilarity;

import java.io.File;
import java.io.IOException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author bruno
 */
public class HybridRecommender {

    static final String PATH = "hybrid_similarity.csv";
    static final String RATINGS = "jester_ratings.csv";

    public static void main(String[] args) throws TasteException, IOException {
        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        DataModel model = new FileDataModel(new File(RATINGS));

        RecommenderBuilder builder = (DataModel model1) -> {
            ItemSimilarity similarity = new FileItemSimilarity(new File(PATH));
            return new GenericItemBasedRecommender(model1, similarity);
        };
                
        double score = recEvaluator.evaluate(builder, null, model, 0.7, 1.0);
        System.out.println(score);
    }
}