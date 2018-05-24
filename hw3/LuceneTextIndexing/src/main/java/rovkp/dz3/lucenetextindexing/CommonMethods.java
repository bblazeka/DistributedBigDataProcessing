/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.dz3.lucenetextindexing;

import java.io.*;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 *
 * @author bruno
 */
public class CommonMethods {

    private static final FieldType ID_FIELD_TYPE;
    private static final FieldType TEXT_FIELD_TYPE;

    static {
        ID_FIELD_TYPE = new FieldType();
        ID_FIELD_TYPE.setStored(true);
        ID_FIELD_TYPE.setTokenized(false);
        ID_FIELD_TYPE.setIndexOptions(IndexOptions.NONE);

        TEXT_FIELD_TYPE = new FieldType();
        TEXT_FIELD_TYPE.setStored(false);
        TEXT_FIELD_TYPE.setTokenized(true);
        TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
    }

    public static Map<Integer, String> getJokes(Path path) throws IOException {
        Map<Integer, String> jokes = new HashMap<>();
        String line;
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            while ((line = reader.readLine()) != null) {
                StringBuilder jokeText = new StringBuilder("");

                // 1st line contains joke index
                Integer id = Integer.parseInt(line.split(":")[0]);

                // other lines contain a joke
                while (!(line = reader.readLine()).isEmpty()) {
                    line = line.trim();
                    jokeText.append(line);
                }

                String joke = StringEscapeUtils.unescapeXml(jokeText.toString().toLowerCase().replaceAll("<.*?>", ""));
                jokes.put(id, joke);
            }

        }

        return jokes;
    }

    public static Document generateDocument(Map.Entry<Integer, String> entry) {
        Document doc = new Document();
        doc.add(new Field("ID", entry.getKey().toString(), ID_FIELD_TYPE));
        doc.add(new Field("TEXT", entry.getValue(), TEXT_FIELD_TYPE));
        return doc;
    }

    public static float[][] getSimilarity(Map<Integer, String> jokes, IndexSearcher searcher, StandardAnalyzer analyzer) throws ParseException, IOException {
        int n = jokes.size();
        float[][] similarityArray = new float[n + 1][n + 1];
        QueryParser qp = new QueryParser("TEXT", analyzer);
        for (Map.Entry<Integer, String> entry : jokes.entrySet()) {
            int i = entry.getKey();
            Query query = qp.parse(QueryParser.escape(entry.getValue()));
            TopDocs docs = searcher.search(query, n);
            for (ScoreDoc doc : docs.scoreDocs) {
                int j = Integer.parseInt(searcher.doc(doc.doc).get("ID"));
                similarityArray[i][j] = doc.score;
            }
        }
        return similarityArray;
    }

    public static float[][] normalize(float[][] matrix) {
        // normalization of similarity array
        for (int i = 0; i < matrix.length; i++) {
            float max = 0;
            // find max in a row
            for (int j = 0; j < matrix[i].length; j++) {
                max = Math.max(max, matrix[i][j]);
            }
            // convert everything to range 0,1
            for (int j = 0; j < matrix[i].length; j++) {
                matrix[i][j] /= max;
            }
        }

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                float average = (matrix[i][j] + matrix[j][i]) / 2;
                matrix[i][j] = average;
                matrix[j][i] = average;
            }
        }
        return matrix;
    }

    public static int fileWrite(float[][] matrix, Path path) throws IOException {
        int lines = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            // writing
            for (int i = 0; i < matrix.length; i++) {
                for (int j = i+1; j < matrix[i].length; j++) {
                    if (matrix[i][j] > 0) {
                        writer.write(String.format("%d,%d,%f%n", i, j, matrix[i][j]));
                        lines++;
                    }
                }
            }
        }
        return lines;
    }

    public static String getSimilarJoke(Map<Integer, String> map, float[][] similarityMatrix, int id) {
        int maxId = 0;
        float max = 0;
        for (int i = 0; i < similarityMatrix[id].length; i++) {
            if(id == i){
                continue;
            }
            if (similarityMatrix[id][i] > max) {
                maxId = i;
                max = similarityMatrix[id][i];
            }
        }
        return map.get(maxId);
    }
}
