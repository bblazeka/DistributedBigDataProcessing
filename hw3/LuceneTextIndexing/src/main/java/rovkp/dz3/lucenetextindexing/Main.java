/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.dz3.lucenetextindexing;

import java.nio.file.*;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

/**
 *
 * @author bruno
 */
public class Main {

    public static void main(String[] args) throws Exception {
        String input,output;
        if (args.length != 2) {
            input = "jester_items.dat";
            output = "item_similarity.csv";
        }
        else {
            input = args[0];
            output = args[1];
        }

        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory index = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(index, config);

        Path inputFile = Paths.get(input);
        Path outputFile = Paths.get(output);

        // 1st part
        Map<Integer, String> jokes = CommonMethods.getJokes(inputFile);

        // 2nd part
        for (Map.Entry<Integer, String> joke : jokes.entrySet()) {
            writer.addDocument(CommonMethods.generateDocument(joke));
        }
        writer.close();

        // 3rd part
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        float[][] similarityMatrix = CommonMethods.getSimilarity(jokes, searcher, analyzer);
        reader.close();
        
        // 4th part
        float[][] normalizedMatrix = CommonMethods.normalize(similarityMatrix);
        
        // 5th part
        int lines = CommonMethods.fileWrite(normalizedMatrix,outputFile);
        
        System.out.println("Number of lines: "+lines);
        System.out.println("Joke number one: "+jokes.get(1));
        System.out.println("Most similar joke: "+
                CommonMethods.getSimilarJoke(jokes,normalizedMatrix,1));
    }
}
