package ru.aermakov;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for HTTP crawler with top word counts demo
 */
public class CrawlerDemoAppTest {

    @Test
    public void test() {
        var url = CrawlerDemoAppTest.class.getClassLoader().getResource("in0.html");
        var result = CrawlerDemoApp.crawl(url);
        Assert.assertEquals(2, CrawlerDemoApp.urlsVisited.size());
        Assert.assertEquals(2, CrawlerDemoApp.processedCount.get());
        Assert.assertNotNull(result);
        Assert.assertFalse(result.containsKey("word1"));
        Assert.assertFalse(result.containsKey("&&&"));
        Assert.assertTrue(result.containsKey("word2"));
        Assert.assertTrue(result.containsKey("word3"));
        Assert.assertTrue(result.containsKey("word4"));
        Assert.assertTrue(result.containsKey("abc"));
        Assert.assertTrue(result.containsKey("abcd"));
        Assert.assertEquals(4, (int) result.get("word2"));
        Assert.assertEquals(1, (int) result.get("abc"));
        Assert.assertEquals(1, (int) result.get("abcd"));
        Assert.assertEquals(1, (int) result.get("word3"));
        Assert.assertEquals(1, (int) result.get("word4"));
    }

}
