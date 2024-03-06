package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StopWords {
    private final List<String> stopWords = new ArrayList<>();
    public StopWords(String stopWordsPath) {
        try (BufferedReader br = new BufferedReader(new FileReader(stopWordsPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line);
            }
        } catch (IOException e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

    public boolean containsWord(String word) {
        return stopWords.contains(word);
    }
}
