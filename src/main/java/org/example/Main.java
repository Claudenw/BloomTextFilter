package org.example;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.*;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.text.similarity.LevenshteinDistance;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.*;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {
    static int counter;

    static final int NUMBER_OF_SELECTED_WORDS = 50;
    static final int NUMBER_OF_PHRASES = 500;
    static final int NUMBER_OF_PATTERNS = 100;

    static final int NUMBER_OF_TESTS = 5;

    static final Comparator<String> naivePatternSorter = Comparator.comparingInt((String s) -> compress(s).length()).thenComparing(s -> s);

    private static int adjust(int value, int count) {
        return (int) (value + (count * .5 * value));
    }

    private static class Statistics {
        final int numberOfPhrases;
        final int numberOfPatterns;
        int patternTokens;
        int phraseTokens;
        Shape shape;
        long cumulativeOrderTime = 0;
        long cumulativeBFTime = 0;
        Statistics(int i) {
            numberOfPhrases = adjust(NUMBER_OF_PHRASES, i);
            numberOfPatterns = adjust(NUMBER_OF_PATTERNS, i);
        }

        void printSynopsis() {
            System.out.format("Phrases: %s Patterns: %s%n", numberOfPhrases, numberOfPatterns);
            System.out.println("maximum pattern tokens: " + patternTokens);
            System.out.println("maximum phrase tokens " + phraseTokens);
            System.out.format("Shape %s (%s bytes)%n", shape, (shape.getNumberOfBits() / Byte.SIZE) + (shape.getNumberOfBits() % Byte.SIZE > 0 ? 1 : 0));
            System.out.format("%s seconds cumulative Tree Time%n", +cumulativeOrderTime);
            System.out.format("%s seconds cumulative BF Time%n", cumulativeBFTime);
        }
    }

    public static void main(String[] args) throws IOException {
        counter = 0;
        Statistics[] stats = new Statistics[NUMBER_OF_TESTS];
        System.out.println("Initializing");
        List<String> words = new ArrayList<>();
        System.out.println("Reading word list");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Main.class.getResourceAsStream("/org/example/words.txt")))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                words.add(line);
            }
        }

        System.out.format("%s words read%n", words.size());
        Random random = new Random();
        System.out.format("Selecting %s words%n", NUMBER_OF_SELECTED_WORDS);
        List<String> selectedWords = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_SELECTED_WORDS; i++) {
            selectedWords.add(words.get(random.nextInt(0, words.size() - 1)));
        }

        for (int statNum=0;statNum<NUMBER_OF_TESTS;statNum++) {
            stats[statNum] = new Statistics(statNum);
            StringBuilder sb = new StringBuilder();
            Set<String> phrases = new HashSet<>();
            System.out.format("Generating %s phrases%n", stats[statNum].numberOfPhrases);
            for (int i = 0; i < stats[statNum].numberOfPhrases; i++) {
                int wordCount = random.nextInt(2, 5);
                for (int j = 0; j < wordCount; j++) {
                    sb.append(selectedWords.get(random.nextInt(0, selectedWords.size()))).append("-");
                }
                sb.delete(sb.length() - 1, sb.length());
                String token = sb.toString();
                // call hash to set maxTopicCounter
                hash(token);
                phrases.add(token);
                sb.setLength(0);
            }
            stats[statNum].phraseTokens = counter;
            counter = 0;
            // Build a Bloom filter shape based upon the maximum number of sequences in a phrase.
            stats[statNum].shape = Shape.fromNP(stats[statNum].phraseTokens, 0.001);

            Map<String, BloomFilter> patterns = new TreeMap<>(naivePatternSorter);
            System.out.format("Generating %s patterns%n", stats[statNum].numberOfPatterns);
            String[] keys = phrases.toArray(new String[0]);
            for (int i = 0; i < stats[statNum].numberOfPatterns; i++) {
                String token = keys[random.nextInt(0, keys.length)];
                String[] parts = token.split("-");
                int wildCount = random.nextInt(1, parts.length);
                for (int j = 0; j < wildCount; j++) {
                    parts[random.nextInt(0, parts.length)] = "*";
                }
                final String pattern = String.join("-", parts);
                BloomFilter bf = new SimpleBloomFilter(stats[statNum].shape);
                bf.merge(hash(pattern));
                patterns.put(pattern, bf);
            }
            stats[statNum].patternTokens = counter;

            System.out.println("Starting tests");
            for (String phrase : phrases) {
                stats[statNum].cumulativeOrderTime += orderedSearchTime(phrase, patterns.keySet());
                stats[statNum].cumulativeBFTime += bfSearchTime(phrase, patterns, stats[statNum].shape);
            }

            stats[statNum].printSynopsis();
        }
        printCSV(stats);
    }

    static void printCSV(Statistics[] stats) {
        double NANO_TO_SECONDS = 0.000000001;
        final DecimalFormat df1 = new DecimalFormat( "#.###" );
        System.out.print("'n Patterns'" );
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", stats[i].numberOfPatterns);
        }
        System.out.println();
        System.out.print("'Ordered'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", df1.format(stats[i].cumulativeOrderTime*NANO_TO_SECONDS));
        }
        System.out.println();
        System.out.print("'Bloom Filter'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", df1.format(stats[i].cumulativeBFTime*NANO_TO_SECONDS));
        }
        System.out.println();
        System.out.print("'Shape.m'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", stats[i].shape.getNumberOfBits());
        }
        System.out.println();
        System.out.print("'Shape.k'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", stats[i].shape.getNumberOfHashFunctions());
        }
        System.out.println();
        System.out.print("'Phrase Tokens'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", stats[i].phraseTokens);
        }
        System.out.println();
        System.out.print("'Pattern Tokens'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", stats[i].patternTokens);
        }
        System.out.println();
        System.out.print("'n Phrases'");
        for (int i=0;i<NUMBER_OF_TESTS;i++) {
            System.out.format(",%s", stats[i].numberOfPhrases);
        }
        System.out.println();
    }

    public static Pattern createRegex(String pattern) {
        String[] parts = pattern.split("\\*");
        String regex = String.join(".*", parts);
        return Pattern.compile(regex);
    }

    public static long orderedSearchTime(String token, Collection<String> patterns) {
        StopWatch stopWatch = StopWatch.create();
        long result = 0;
        String cToken = compress(token);
        List<String> matches = new ArrayList<>();
        for (String pattern : patterns) {
            String cPattern = compress(pattern);
            if (cPattern.length() < cToken.length()) {
                Pattern regex = createRegex(pattern);
                stopWatch.reset();
                stopWatch.start();
                if (regex.matcher(token).matches()) {
                    matches.add(pattern);
                }
                stopWatch.stop();
                result += stopWatch.getNanoTime();
            } else {
                break;
            }
        }
        if (matches.size() > 1) {
            stopWatch.reset();
            stopWatch.start();
            findMatch(token, matches);
            stopWatch.stop();
            result += stopWatch.getNanoTime();
        }
        return result;
    }

    public static long bfSearchTime(String token, Map<String,BloomFilter> bfMap, Shape shape) {
        StopWatch stopWatch = StopWatch.create();
        long result = 0;
        stopWatch.start();
        Set<String> patterns = locate(token, bfMap, shape);
        List<String> matches = new ArrayList<>();
        stopWatch.stop();
        result += stopWatch.getNanoTime();
        for (String pattern: patterns) {
            Pattern regex = createRegex(pattern);
            stopWatch.reset();
            stopWatch.start();
            if (regex.matcher(token).matches()) {
                matches.add(pattern);
            }
            stopWatch.stop();
            result += stopWatch.getNanoTime();
        }
        if (matches.size() > 1) {
            stopWatch.reset();
            stopWatch.start();
            findMatch(token, matches);
            stopWatch.stop();
            result += stopWatch.getNanoTime();
        }
        return result;
    }

    // doesn't really find a match just executes the steps to do so.
    static void findMatch(String target, List<String> matches) {
        int match = -1;
        int distance = Integer.MAX_VALUE;
        for (int i = 0;i<matches.size();i++) {
            String pattern = matches.get(i);
            int dist = LevenshteinDistance.getDefaultInstance().apply(target,pattern);
            if (dist < distance) {
                distance = dist;
                match = i;
            } else if (dist == distance && match > -1) {
                if (pattern.length() > matches.get(match).length()) {
                    match = i;
                }
            }
        }
    }

    public static Set<String> locate(String s, Map<String,BloomFilter> map, Shape shape) {
        Hasher hasher = hash(s);
        BloomFilter key = new SimpleBloomFilter(shape);
        key.merge(hasher);
        return map.entrySet().stream().filter(entry -> key.contains(entry.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    private static final String compress(String text) {
        StringBuilder sb = new StringBuilder(text);
        for (int i = sb.length()-1; i>-1; i--) {
            if (!Character.isLetterOrDigit(sb.charAt(i))) {
                sb.delete(i,i+1);
            }
        }
        return sb.toString();
    }


    public static Hasher hash(String text) {
        Map<String,Hasher> hashers = new HashMap<>();
        Character lastChar = null;
        int keyCount = 0;

        for (int i = 0; i < text.length(); i++) {
            char at = text.charAt(i);
            if (lastChar == null) {
                if (Character.isLetterOrDigit(at)) {
                    lastChar = Character.valueOf(at);
                }
            } else {
                if (Character.isLetterOrDigit(at)) {
                    String key = lastChar.toString() + at;
                    if (!hashers.containsKey(key)) {
                        ++keyCount;
                        long[] hash = MurmurHash3.hash128x64(key.getBytes(StandardCharsets.UTF_8));
                        hashers.put(key, new EnhancedDoubleHasher(hash[0], hash[1]));
                    }
                    lastChar = Character.valueOf(at);
                } else {
                    lastChar = null;
                }
            }
        }
        counter = Math.max(keyCount,counter);
        return new GroupOfHashers(hashers.values());
    }

    public static class GroupOfHashers implements Hasher {
        Collection<Hasher> producers;

        public GroupOfHashers(Collection<Hasher> producers) {
            this.producers = producers;
        }

        @Override
        public IndexProducer indices(Shape shape) {
            return predicate -> {
                for (Hasher hash : producers) {
                    if (!hash.indices(shape).forEachIndex(predicate)) {
                        return false;
                    }
                }
                return true;
            };
        }
    }
}