/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab4.collectionstreamshandler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 *
 * @author bruno
 */
public class DataHandler {

    private final String folderPath;
    private final List<File> files;
    Stream<PollutionReading> records;

    public DataHandler(String path) {
        folderPath = path;
        files = new ArrayList();
        records = Stream.empty();
    }

    public void loadAndSortData(String output) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(output));
        Files.list(Paths.get(folderPath))
                .flatMap(ThrowingFunction.wrap(Files::lines))
                .map(PollutionReading::create)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(PollutionReading::getTimestamp))
                .map(PollutionReading::toString)
                .forEach(writer::println);
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> extends Function<T, R> {

        @Override
        public default R apply(T t) {
            try {
                return throwingApply(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static <T, R> Function<T, R> wrap(ThrowingFunction<T, R> f) {
            return f;
        }

        R throwingApply(T t) throws Exception;
    }
}
