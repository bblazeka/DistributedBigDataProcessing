/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.dz3.mahoutrecommender;

import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author bruno
 */
public class ItemBasedRecommend {
    
    public Recommender buildRecommender(DataModel model, ItemSimilarity similarity){
        return new GenericItemBasedRecommender(model, similarity);
    }
}
