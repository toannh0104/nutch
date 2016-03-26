package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.Parse;

import com.vsii.ttxvn.crawling.Crawler;
import com.vsii.ttxvn.crawling.SourceUrl;

public class AddField implements IndexingFilter {
	private Configuration conf;

	// implements the filter-method which gives you access to important Objects
	// like NutchDocument
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) {
		
		for (SourceUrl sourceUrl : Crawler.mapSourceUrl.values()) {				
			if (doc.getField("url").toString().contains(sourceUrl.getDomain())) {
				doc.add("reliability", sourceUrl.getReliability());
				doc.add("lang-code", sourceUrl.getLangCode().getCode().toLowerCase());
				break;
			}
		}
			
		return doc;
	}

	// Boilerplate
	public Configuration getConf() {
		return conf;
	}

	// Boilerplate
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}