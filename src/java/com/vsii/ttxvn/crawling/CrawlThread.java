/*package com.vsii.ttxvn.crawling;

public class CrawlThread implements Runnable{
	private Thread thread;
	private String threadName;
	
	public CrawlThread(String name) {
		threadName = name;
		thread = new Thread(this);
		thread.start();
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		SourceUrl currentUrl = new SourceUrl();
		for (SourceUrl sourceUrl : Crawler.mapStatusCrawlingForThreading.keySet()) {
			if (Crawler.mapStatusCrawlingForThreading.get(sourceUrl) == 0) {
				Crawler.mapStatusCrawlingForThreading.put(sourceUrl, 1);
				currentUrl = sourceUrl;	
				break;
			}
		}
		if (currentUrl.getUrl() != null) {
			try
	        {                            
	            Crawler.writeSeedFile(currentUrl.getUrl(), currentUrl.getLangCode(), threadName);
	            Crawler.createRegexUrlFilter(currentUrl.getUrl());
	            Crawler.processCrawling(currentUrl.getFetchDeep().getValue(), currentUrl.getDomain(), currentUrl.getLangCode(), currentUrl.getCrawlingStatus(), threadName);
	        }
	        catch (Exception e)
	        {
	            System.err.println("Failed to crawl data from:");
	//		            for (String url : fetchUrls) {
	                System.err.println(" + " + currentUrl.getUrl());
	//		            }
	            e.printStackTrace();
	        }
		}
	}

	public Thread getThread() {
		return thread;
	}

	public void setThread(Thread thread) {
		this.thread = thread;
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	
}*/
