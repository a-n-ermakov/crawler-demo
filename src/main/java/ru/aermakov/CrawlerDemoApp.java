package ru.aermakov;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * HTTP crawler for word counting demo
 * Input: args[0] - url to parse
 *        args[1] - links depth
 * Output: stdout
 */
public class CrawlerDemoApp {

    private static final int MAX_DEPTH_DEFAULT = 10;
    private static final List<String> SKIPPED_EXTS = List.of(
            "jpg", "jpeg", "png", "gif", "tiff", "bmp", "svg"
    );

    private static final ForkJoinPool pool = ForkJoinPool.commonPool();
    static final Set<URL> urlsVisited = Collections.synchronizedSet(new HashSet<>());
    static final AtomicInteger processedCount = new AtomicInteger(0);
    private static int maxDepth = MAX_DEPTH_DEFAULT;

    /**
     * Entry point
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java -jar crawler-demo.jar <url> <max_depth>");
            System.exit(0);
        }
        var urlStr = args[0];
        try {
            maxDepth = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.printf("Incorrect max_depth [%s], using default [%d]%n", args[1], MAX_DEPTH_DEFAULT);
        }
        URL url;
        try {
            url = new URL(urlStr);
            var wordCounts = crawl(url);
            System.out.println("Visited urls: " + urlsVisited);
            wordCounts.entrySet().stream()
                    .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                    .limit(100)
                    .forEach(e -> System.out.printf("%s: %d%n", e.getKey(), e.getValue()));
        } catch (MalformedURLException e) {
            System.out.println("Incorrect URL: " + urlStr);
        }
    }

    /**
     * Crawl to specified URL
     *
     * @param url URL
     * @return word counts map
     */
    public static Map<String, Integer> crawl(URL url) {
        var task = new CrawlerTask(url, 0);
        urlsVisited.add(url);
        processedCount.incrementAndGet();
        return pool.invoke(task);
    }

    /**
     * Task for fork/join
     */
    public static class CrawlerTask extends RecursiveTask<Map<String, Integer>> {
        private final URL url;
        private final int depth;
        private final String domain;

        public CrawlerTask(URL url, int depth) {
            this.url = url;
            this.depth = depth;
            if (nonNull(url.getAuthority()) && !url.getAuthority().isEmpty()) {
                this.domain = String.format("%s://%s", url.getProtocol(), url.getAuthority());
            } else {  //for test urls
                var path = url.getPath();
                var lastIdx = path.lastIndexOf("/");
                this.domain = String.format("%s:%s", url.getProtocol(), path.substring(0, lastIdx));
            }
        }

        @Override
        protected Map<String, Integer> compute() {
            Map<String, Integer> result = new HashMap<>();
            try {
                Document doc = timeLog(() -> {
                    try (var is = url.openStream()){
                        System.out.println("Parsing url: " + url.toString());
                        return Jsoup.parse(is, "UTF-8", domain);
                    } catch (IOException e) {
                        return null;
                    }
                }, "parse");
                //counting words
                if (doc == null || doc.body() == null) {
                    return result;
                }
                var text = doc.body().text();
                if (text == null || text.isEmpty()) {
                    return result;
                }
                // System.out.println(text);
                timeLog(() -> {
                    for (String token : text.toLowerCase().split("[^\\w\\d]")) {
                        var word = token.trim();
                        if (word.length() < 3) {
                            continue;
                        }
                        result.merge(word, 1, Integer::sum);
                    }
                    return null;
                }, "merge1");
                if (depth == maxDepth) {
                    return result;
                }
                //finding hrefs and starting subtasks
                List<CrawlerTask> subtasks = new ArrayList<>();
                int badCount = 0, skipCount  = 0;
                for (Element a : doc.getElementsByTag("a")) {
                    var href = a.attr("href");
                    if (isNull(href) || href.isEmpty() || href.startsWith("#")) {
                        continue;
                    } else if (href.startsWith("//")) {
                        href = String.format("%s:%s", this.url.getProtocol(), href);
                    } else if (href.startsWith("/")) {
                        href = this.domain + href;
                    }
                    try {
                        var url = new URL(href);
                        var fileExt = extractFileExt(href);
                        if (this.url.getHost().equals(url.getHost())
                                && urlsVisited.add(url)
                                && !SKIPPED_EXTS.contains(fileExt)
                        ) {
                            var subtask = new CrawlerTask(url, depth+1);
                            subtask.fork();
                            subtasks.add(subtask);
                        } else {
                            skipCount ++;
                        }
                    } catch (MalformedURLException e) {
                        badCount ++;
                    }
                }
                System.out.printf("depth %d, good|skip|bad counts: %d|%d|%d%n", depth,
                        subtasks.size(), skipCount, badCount);
                //merging results
                for (CrawlerTask subtask : subtasks) {
                    var subtaskResult = subtask.join();
                    timeLog(() -> {
                        subtaskResult.forEach((k, v) -> result.merge(k, v, Integer::sum));
                        return null;
                    }, "merge2");

                }
                System.out.printf("=== Progress: %d of %d complete ===%n",
                        processedCount.addAndGet(subtasks.size()), urlsVisited.size()
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

    }

    /**
     * Time logging wrapper
     *
     * @param sup supplier
     * @param label str for log
     * @param <T> return value type
     * @return value
     */
    private static <T> T timeLog(Supplier<T> sup, String label) {
        var start = System.currentTimeMillis();
        var res = sup.get();
        var finish = System.currentTimeMillis();
        System.out.printf("%s duration: %d ms%n", label, (finish - start));
        return res;
    }

    /**
     * Extract file extension
     *
     * @param href string url
     * @return extension
     */
    private static String extractFileExt(String href) {
        var slashIdx = href.lastIndexOf("/");
        var fileName = href.substring(slashIdx);
        if (!fileName.isEmpty() && fileName.contains(".")) {
            var parts = fileName.split("\\.");
            var ext = parts[parts.length-1];
            // System.out.println("Extension: " + ext);
            return ext;
        }
        return "";
    }

}