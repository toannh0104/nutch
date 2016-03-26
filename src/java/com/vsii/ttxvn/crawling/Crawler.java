package com.vsii.ttxvn.crawling;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.CrawlDbMerger;
import org.apache.nutch.crawl.DeduplicationJob;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.CleaningJob;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crawler {

	private static final Logger logger = LoggerFactory.getLogger(Crawler.class);
	private static final String SLASH_STRING = "://";
	private static final String HTTP_STRING = "http";
	private static final String HTTPS_STRING = "https";
	private static final String D_STRING = "-D";
	private static final String SOLR_SERVER_URL = "solr.server.url";

	private static final Map<LanguageCode, String> MAP_SOLR_URL_BY_LANG_CODE = new HashMap<LanguageCode, String>();
	private static final String MERGED = "_merged";
	private static final String OLD = "_old";
	private static final String CRAWL_DIR_LINKDB = "linkdb";
	private static final String CRAWL_DIR_SEGMENTS = "segments";
	private static final String CRAWL_DIR_DB = "crawldb";

	private static final String HADOOP_NUMTHREADS_VAL;
	private static final String NUTCH_TIMELIMITFETCH_VAL;
	private static final String HADOOP_NUMSLAVES_VAL;
	private static final String HADOOP_STAGING_PATH;
	private static final String HADOOP_TASK_TRACKER_PATH;

	// the map hold crawling urls info for purpose mapping reliability after
	// finished crawling data
	public static Map<String, SourceUrl> mapSourceUrl = new HashMap<String, SourceUrl>();
	// the custom Regex Url Filter
	public static List<String> customRegexUrlFilter = new ArrayList<String>();

	public static final String NUTCH_SEED_FILE_PATH;
	public static final String COMMON_OPTS = "-D mapred.reduce.tasks=1 -D mapred.reduce.tasks.speculative.execution=false -D mapred.map.tasks.speculative.execution=false -D mapred.compress.map.output=true";

	public static final String NUTCH_RESOURCES_FOLDER;
	public static final String CRAWL_FOLDER_VI;
	public static final String CRAWL_FOLDER_EN;
	public static final String SEED_FILE_VI;
	public static final String SEED_FILE_EN;
	public static final String CRAWLING_EXCEPTION_FILE_PATH;

	static {
		MAP_SOLR_URL_BY_LANG_CODE.put(LanguageCode.EN,
				System.getenv("ttxvn_server_solr_en_url"));
		MAP_SOLR_URL_BY_LANG_CODE.put(LanguageCode.VI,
				System.getenv("ttxvn_server_solr_vi_url"));
		// MAP_SOLR_URL_BY_LANG_CODE.put(LanguageCode.EN,
		// "http://192.168.0.34:8984/solr");
		// MAP_SOLR_URL_BY_LANG_CODE.put(LanguageCode.VI,
		// "http://192.168.0.34:8983/solr");
		
		NUTCH_RESOURCES_FOLDER = System
				.getenv("ttxvn_nutch_crawl_resources_dir");

		// CRAWL_DIR_ROOT_FOLDER =
		// NUTCH_RESOURCES_FOLDER.concat(File.separator).concat("crawl");
		CRAWL_FOLDER_VI = NUTCH_RESOURCES_FOLDER + File.separator
				+ System.getenv("ttxvn_nutch_crawl_vi_dir");
		CRAWL_FOLDER_EN = NUTCH_RESOURCES_FOLDER + File.separator
				+ System.getenv("ttxvn_nutch_crawl_en_dir");
		
		CRAWLING_EXCEPTION_FILE_PATH = NUTCH_RESOURCES_FOLDER + File.separator + "crawling_exception.txt";
		
		HADOOP_TASK_TRACKER_PATH = System.getenv("hadoop_task_tracker_path");
		HADOOP_STAGING_PATH = System.getenv("hadoop_staging_path");

		NUTCH_SEED_FILE_PATH = NUTCH_RESOURCES_FOLDER.concat(File.separator)
				.concat("seed");

		SEED_FILE_VI = NUTCH_RESOURCES_FOLDER + File.separator + "seed_vi.txt";
		SEED_FILE_EN = NUTCH_RESOURCES_FOLDER + File.separator + "seed_en.txt";

		NUTCH_TIMELIMITFETCH_VAL = System
				.getenv("ttxvn_nutch_time_limit_fetch");
		HADOOP_NUMTHREADS_VAL = System.getenv("ttxvn_nutch_hadoop_num_thread");
		HADOOP_NUMSLAVES_VAL = System.getenv("ttxvn_nutch_hadoop_num_slaves");

		try {
			File file = new File(NUTCH_RESOURCES_FOLDER);

			if (!file.exists()) {
				file.mkdir();
			}

			// file = new File(CRAWL_DIR_ROOT_FOLDER + "_" + EN);
			file = new File(CRAWL_FOLDER_EN);
			if (!file.exists()) {
				file.mkdir();
			}

			// file = new File(CRAWL_DIR_ROOT_FOLDER + "_" + VI);
			file = new File(CRAWL_FOLDER_VI);

			if (!file.exists()) {
				file.mkdir();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getSeedFilePath(LanguageCode languageCode) {
		if (LanguageCode.VI == languageCode) {
			return SEED_FILE_VI;
		}
		return SEED_FILE_EN;
	}

	public static String getCrawlFolder(LanguageCode languageCode) {
		if (LanguageCode.VI == languageCode) {
			return CRAWL_FOLDER_VI;
		}
		return CRAWL_FOLDER_EN;
	}

	private static void writeSeedFile(List<String> hyperLinks,
			LanguageCode languageCode) throws Exception{
		try {
			File seedFile = new File(getSeedFilePath(languageCode));
			List<String> urlLines = new ArrayList<String>();

			if (!seedFile.exists()) {
				seedFile.createNewFile();
			}

			for (String url : hyperLinks) {
				if (url.contains(HTTP_STRING + SLASH_STRING)) {
					urlLines.add(url + "\tnutch.fetchInterval.fixed=60");
				} else {
					if (url.contains("facebook") || url.contains("twitter"))
						urlLines.add(HTTPS_STRING + SLASH_STRING + url
								+ "\tnutch.fetchInterval.fixed=60");
					else
						urlLines.add(HTTP_STRING + SLASH_STRING + url
								+ "\tnutch.fetchInterval.fixed=60");
				}
			}

			FileUtils.writeLines(seedFile, urlLines, false);
		} catch (IOException e) {
			logger.error("-------> Failed to write data to ["
					+ getSeedFilePath(languageCode) + "] file");
			e.printStackTrace();
			throw e;
		}
	}

	private static void writeSeedFile(String url, LanguageCode languageCode) throws Exception {
		try {
			File seedFile = new File(getSeedFilePath(languageCode));
			List<String> urlLines = new ArrayList<String>();

			if (!seedFile.exists()) {
				seedFile.createNewFile();
			}

			if (url.contains(HTTP_STRING + SLASH_STRING)) {
				urlLines.add(url + "\tnutch.fetchInterval.fixed=60");
			} else {
				if (url.contains("facebook") || url.contains("twitter"))
					urlLines.add(HTTPS_STRING + SLASH_STRING + url
							+ "\tnutch.fetchInterval.fixed=60");
				else
					urlLines.add(HTTP_STRING + SLASH_STRING + url
							+ "\tnutch.fetchInterval.fixed=60");
			}

			FileUtils.writeLines(seedFile, urlLines, false);
		} catch (IOException e) {
			logger.error("-------> Failed to write data to ["
					+ getSeedFilePath(languageCode) + "] file");
			e.printStackTrace();
			throw e;
		}
	}

	private static void createRegexUrlFilter(List<String> hyperLinks) {
		// reset regex url filter
		customRegexUrlFilter.clear();
		// add new regex
		for (String hyperlink : hyperLinks) {
			if (hyperlink.contains("facebook") || hyperlink.contains("twitter")) {
				customRegexUrlFilter
						.add(("+^https://([a-z0-9]*\\.)*" + hyperlink).replace(
								"www.", ""));
			} else {
				customRegexUrlFilter
						.add(("+^http://([a-z0-9]*\\.)*" + hyperlink).replace(
								"www.", ""));
			}
		}
	}

	private static void createRegexUrlFilter(String url) {
		// reset regex url filter
		customRegexUrlFilter.clear();
		// add new regex
		// if (url.contains("facebook") || url.contains("twitter")) {
		// customRegexUrlFilter.add(("+^https://([a-z0-9]*\\.)*" +
		// url).replace("www.", ""));
		// } else {
		// customRegexUrlFilter.add(("+^http://([a-z0-9]*\\.)*" +
		// url).replace("www.", ""));
		// }
		customRegexUrlFilter.add(("+^(http|https)://([a-z0-9]*\\.)*" + url)
				.replace("www.", ""));
	}

	/*
	 * return 0 if crawled in successfully otherwise return -1
	 */
	private static int oneTimeCrawling(String commonOptions,
			String allowUpdateNewUrlToDb, String domain,
			LanguageCode languageCode) throws Exception {
		Map<String, String> mapValueProperty = new HashMap<String, String>();
		mapValueProperty.put("db.update.additions.allowed",
				allowUpdateNewUrlToDb);

		int crawlingResult = 0;

		// inject
		crawlingResult += injectJob(mapValueProperty, domain, languageCode);
		// generate
		crawlingResult += generateJob(mapValueProperty, commonOptions, domain,
				languageCode);

		File dir = new File(getCrawlFolder(languageCode) + File.separator
				+ domain + File.separator + CRAWL_DIR_SEGMENTS);

		// logger.debug("--> prepare fetching the crawling data to folder ["
		// + dir.getAbsolutePath() + "] files list:");

		if (dir != null) {
			File[] files = dir.listFiles();
			/** The newest file comes first **/
			if (files != null && files.length > 0) {
				Arrays.sort(files,
						LastModifiedFileComparator.LASTMODIFIED_REVERSE);

				// for (File file : files) {
				// logger.debug(" + " + file.getAbsolutePath() + ": " +
				// file.getName());
				// }
				// fetch
				crawlingResult += fetchJob(mapValueProperty, commonOptions,
						files, domain, languageCode);
				// parse
				crawlingResult += parseJob(mapValueProperty, commonOptions,
						files, domain, languageCode);
				// update
				crawlingResult += updateDbJob(mapValueProperty, commonOptions,
						files, domain, languageCode);
				// invert
				crawlingResult += linkDbJob(mapValueProperty, files, domain,
						languageCode);
				// dedup
				crawlingResult += dedupJob(mapValueProperty, domain,
						languageCode);
				// index
				crawlingResult += indexJob(mapValueProperty, files, domain,
						languageCode);
				// clean
				crawlingResult += cleanJob(mapValueProperty, domain,
						languageCode);
			}
		}
		logger.info("----------------------> finished CRAWLING");

		return (crawlingResult == 0) ? 0 : -1;
	}

	
	public static Integer crawl(Map<String, SourceUrl> urls) throws Exception {
		int result = 0;
		Map<String, SourceUrl> normalizeUrls = null;
		clearHadoopTmpData();
		if (urls != null && urls.size() > 0) {
			normalizeUrls = Crawler.normalizeUrls(urls);

			Map<LanguageCode, Map<Integer, List<SourceUrl>>> urlsGroupByLanguage = prepareDataToFetch2(urls);

			if (urlsGroupByLanguage == null || urlsGroupByLanguage.size() == 0) {
				return result;
			}

			Map<Integer, List<SourceUrl>> urlsGroupByFetchDeep;
			Iterator<Integer> iterator;
			List<SourceUrl> fetchUrls;
			Integer feetDeep;

			for (LanguageCode languageCode : LanguageCode.list()) {
				urlsGroupByFetchDeep = urlsGroupByLanguage.get(languageCode);

				if (urlsGroupByFetchDeep == null
						|| urlsGroupByFetchDeep.size() == 0) {
					continue;
				}

				iterator = urlsGroupByFetchDeep.keySet().iterator();
				// crawls by FetchDeep
				while (iterator.hasNext()) {
					feetDeep = iterator.next();
					fetchUrls = urlsGroupByFetchDeep.get(feetDeep);
					for (SourceUrl sourceUrl : fetchUrls) {
											
						try {
							
							Crawler.writeSeedFile(sourceUrl.getUrl(),
									languageCode);
							Crawler.createRegexUrlFilter(sourceUrl.getUrl());
							Crawler.processCrawling(feetDeep,
									sourceUrl.getDomain(), languageCode,
									sourceUrl.getCrawlingStatus());
						} catch (FileNotFoundException e) {
							logger.error("======================> Failed to crawl data from:");						
							logger.error(" + " + sourceUrl.getUrl());
							result = -1;
							logger.error("======================> FileNotFoundException catched!");
							e.printStackTrace();
						} catch (IOException e) {
							logger.error("======================> Failed to crawl data from:");						
							logger.error(" + " + sourceUrl.getUrl());
							result = -1;
							logger.error("======================> IOException catched!");
							e.printStackTrace();
						} catch (OutOfMemoryError e) {
							logger.error("======================> Failed to crawl data from:");
							logger.error(" + " + sourceUrl.getUrl());
							result = -1;
							logger.error("======================> OutOfMemoryError catched!");
							e.printStackTrace();
						} catch (Exception e) {
							logger.error("======================> Failed to crawl data from:");						
							logger.error(" + " + sourceUrl.getUrl());								
							e.printStackTrace();
						}
					}
				}
			}
		}

		return result;
	}
	
	//old function
	public static Map<String, SourceUrl> crawlOld(Map<String, SourceUrl> urls)
			throws Exception {
		Map<String, SourceUrl> normalizeUrls = null;
		clearHadoopTmpData();
		if (urls != null && urls.size() > 0) {
			normalizeUrls = Crawler.normalizeUrls(urls);

			Map<LanguageCode, Map<Integer, List<SourceUrl>>> urlsGroupByLanguage = prepareDataToFetch2(urls);

			if (urlsGroupByLanguage == null || urlsGroupByLanguage.size() == 0) {
				return normalizeUrls;
			}

			Map<Integer, List<SourceUrl>> urlsGroupByFetchDeep;
			Iterator<Integer> iterator;
			List<SourceUrl> fetchUrls;
			Integer feetDeep;

			for (LanguageCode languageCode : LanguageCode.list()) {
				urlsGroupByFetchDeep = urlsGroupByLanguage.get(languageCode);

				if (urlsGroupByFetchDeep == null
						|| urlsGroupByFetchDeep.size() == 0) {
					continue;
				}

				iterator = urlsGroupByFetchDeep.keySet().iterator();
				// crawls by FetchDeep
				while (iterator.hasNext()) {
					feetDeep = iterator.next();
					fetchUrls = urlsGroupByFetchDeep.get(feetDeep);
					for (SourceUrl sourceUrl : fetchUrls) {
											
						try {
							//test
							if (sourceUrl.getUrl().contains("doisongphapluat"))
								throw new FileNotFoundException();
							//end test
							
							Crawler.writeSeedFile(sourceUrl.getUrl(),
									languageCode);
							Crawler.createRegexUrlFilter(sourceUrl.getUrl());
							Crawler.processCrawling(feetDeep,
									sourceUrl.getDomain(), languageCode,
									sourceUrl.getCrawlingStatus());
						} catch (FileNotFoundException e) {
							System.err.println("Failed to crawl data from:");
						
							System.err.println(" + " + sourceUrl.getUrl());
							
							
							List<String> lines = new ArrayList<String>();
							lines.add("1");
							lines.add(System.lineSeparator());
							FileUtils.writeLines(new File(CRAWLING_EXCEPTION_FILE_PATH), lines);
						
							e.printStackTrace();
						} catch (Exception e) {
							System.err.println("Failed to crawl data from:");
						
							System.err.println(" + " + sourceUrl.getUrl());							
							
							e.printStackTrace();
						}
					}
				}
			}
		}

		return normalizeUrls;
	}

	private static void clearHadoopTmpData() {
		// TODO Auto-generated method stub
		logger.info("==== start clear cache hadoop");
		deleteFilesOlderThanNdays(0, HADOOP_TASK_TRACKER_PATH, logger);
		deleteFilesOlderThanNdays(0, HADOOP_STAGING_PATH, logger);
		logger.info("==== finish clear cache hadoop");
	}

	private static int processCrawling(int deep, String domain,
			LanguageCode languageCode, CrawlingStatus crawlingStatus)
			throws IOException, Exception {
		int crawlingResult;
		if (deep == 1) {
			String dbUpdateAdditionsAllowed = (CrawlingStatus.NOT_CRAWLING == crawlingStatus) ? "true"
					: "false";
			crawlingResult = oneTimeCrawling(COMMON_OPTS,
					dbUpdateAdditionsAllowed, domain, languageCode);
		} else {
			for (int i = 0; i < deep - 1; i++) {
				oneTimeCrawling(COMMON_OPTS, "true", domain, languageCode); // true
			}
			// do not add hyper link to linkdb in last fetching session
			crawlingResult = oneTimeCrawling(COMMON_OPTS, "false", domain,
					languageCode); // false
		}
		// crawldbmerge
		crawlingResult += mergeDbJob(null, domain, languageCode);
		deleteFilesOlderThanNdays(1, getCrawlFolder(languageCode) + File.separator + domain + File.separator + CRAWL_DIR_SEGMENTS, logger);
		return crawlingResult;
	}

	private static int mergeDbJob(Map<String, String> mapValueProperty,
			String domain, LanguageCode languageCode) throws Exception {
		String[] argsMergeDb = new String[] {
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB + MERGED,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB };
		int result = CrawlDbMerger.run(mapValueProperty, argsMergeDb);
		FileUtils
				.deleteDirectory(new File(getCrawlFolder(languageCode)
						+ File.separator + domain + File.separator
						+ CRAWL_DIR_DB + OLD));
		FileUtils.moveDirectory(new File(getCrawlFolder(languageCode)
				+ File.separator + domain + File.separator + CRAWL_DIR_DB),
				new File(getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB + OLD));
		FileUtils.moveDirectory(new File(getCrawlFolder(languageCode)
				+ File.separator + domain + File.separator + CRAWL_DIR_DB
				+ MERGED), new File(getCrawlFolder(languageCode)
				+ File.separator + domain + File.separator + CRAWL_DIR_DB));
		return result;
	}

	private static int cleanJob(Map<String, String> mapValueProperty,
			String domain, LanguageCode languageCode) throws Exception {
		String[] argsClean = new String[] {
				D_STRING,
				SOLR_SERVER_URL + "="
						+ MAP_SOLR_URL_BY_LANG_CODE.get(languageCode),
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB };
		return CleaningJob.run(mapValueProperty, argsClean);
	}

	private static int indexJob(Map<String, String> mapValueProperty,
			File[] files, String domain, LanguageCode languageCode)
			throws Exception {
		String[] argsIndex = new String[] {
				D_STRING,
				SOLR_SERVER_URL + "="
						+ MAP_SOLR_URL_BY_LANG_CODE.get(languageCode),
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB,
				"-linkdb",
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_LINKDB,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_SEGMENTS + File.separator
						+ files[0].getName() };

		return IndexingJob.run(mapValueProperty, argsIndex);
	}

	private static int dedupJob(Map<String, String> mapValueProperty,
			String domain, LanguageCode languageCode) throws Exception {
		String[] argsDedup = new String[] { getCrawlFolder(languageCode)
				+ File.separator + domain + File.separator + CRAWL_DIR_DB };
		return DeduplicationJob.run(mapValueProperty, argsDedup);
	}

	private static int linkDbJob(Map<String, String> mapValueProperty,
			File[] files, String domain, LanguageCode languageCode)
			throws Exception {
		String[] argsLinkDb = new String[] {
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_LINKDB,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_SEGMENTS + File.separator
						+ files[0].getName() };
		return LinkDb.run(mapValueProperty, argsLinkDb);
	}

	private static int updateDbJob(Map<String, String> mapValueProperty,
			String commonOptions, File[] files, String domain,
			LanguageCode languageCode) throws Exception {
		String[] argsUpdate = new String[] {
				commonOptions,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_SEGMENTS + File.separator
						+ files[0].getName(), "-filter" };
		return CrawlDb.run(mapValueProperty, argsUpdate);
	}

	private static int parseJob(Map<String, String> mapValueProperty,
			String commonOptions, File[] files, String domain,
			LanguageCode languageCode) throws Exception {
		String[] argsParse = new String[] {
				commonOptions,
				"-D mapred.skip.attempts.to.start.skipping=2 -D mapred.skip.map.max.skip.records=1",
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_SEGMENTS + File.separator
						+ files[0].getName() };
		return ParseSegment.run(mapValueProperty, argsParse);
	}

	private static int fetchJob(Map<String, String> mapValueProperty,
			String commonOptions, File[] files, String domain,
			LanguageCode languageCode) throws Exception {
		String[] argsFetch = new String[] {
				commonOptions,
				"-D fetcher.timelimit.mins" + "="
						+ Integer.parseInt(NUTCH_TIMELIMITFETCH_VAL),
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_SEGMENTS + File.separator
						+ files[0].getName(), "-noParsing",
				"-threads " + Integer.parseInt(HADOOP_NUMTHREADS_VAL) };
		return Fetcher.run(mapValueProperty, argsFetch);
	}

	private static int generateJob(Map<String, String> mapValueProperty,
			String commonOptions, String domain, LanguageCode languageCode)
			throws Exception {
		String[] argsGenerate = new String[] {
				commonOptions,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB,
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_SEGMENTS,
				"-numFetchers " + HADOOP_NUMSLAVES_VAL, "-noFilter" };
		return Generator.run(mapValueProperty, argsGenerate);
	}

	private static int injectJob(Map<String, String> mapValueProperty,
			String domain, LanguageCode languageCode) throws Exception {
		String[] argsInject = new String[] {
				getCrawlFolder(languageCode) + File.separator + domain
						+ File.separator + CRAWL_DIR_DB,
				getSeedFilePath(languageCode) };
		return Injector.run(mapValueProperty, argsInject);
	}

	public static boolean isAliveUrl(String URLName) {
		/*if (URLName.contains("facebook") || URLName.contains("twitter"))
			return true;
		else {
			if (!URLName.contains(HTTP_STRING))
				URLName = HTTP_STRING + SLASH_STRING + URLName;
			try {
				HttpURLConnection.setFollowRedirects(false);
				HttpURLConnection con = (HttpURLConnection) new URL(URLName)
						.openConnection();
				con.setRequestMethod("HEAD");
				con.setConnectTimeout(5000);
				logger.info("ping server: " + URLName + " - status: "
						+ con.getResponseCode());
				return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
			} catch (Exception e) {
				logger.error("Ping server " + URLName + " failed caused by "
						+ e);
				return false;
			}
		}*/
		return true;
	}

	public static Map<String, SourceUrl> getAliveUrls(
			Map<String, SourceUrl> urls) {
		Map<String, SourceUrl> urlsAlive = null;
		if (urls != null && urls.size() > 0) {
			Iterator<String> iterator = urls.keySet().iterator();
			SourceUrl sourceUrl;
			String url;

			urlsAlive = new HashMap<String, SourceUrl>();

			while (iterator.hasNext()) {
				url = iterator.next();
				sourceUrl = urls.get(url);

				if (sourceUrl.isAlive()) { // remove unreachable URL
					urlsAlive.put(url, sourceUrl);
				}
			}
		}

		return urlsAlive;
	}

	public static Map<LanguageCode, Map<Integer, List<String>>> prepareDataToFetch(
			Map<String, SourceUrl> urls) {
		Map<LanguageCode, Map<Integer, List<String>>> urlsGroupByLanguage = null;

		if (urls != null && urls.size() > 0) {
			Iterator<String> iterator = urls.keySet().iterator();
			Map<Integer, List<String>> urlsGroupByFetchDeep;
			List<String> fetchUrls;
			LanguageCode languageCode;
			SourceUrl sourceUrl;
			String url;
			Integer fetchDeep;
			// reset Map Source URL
			Crawler.mapSourceUrl.clear();
			// initiate Map LanguageCode Container (support VI & EN only)
			urlsGroupByLanguage = new HashMap<LanguageCode, Map<Integer, List<String>>>();

			while (iterator.hasNext()) {
				url = iterator.next();
				sourceUrl = urls.get(url);

				if (sourceUrl.isAlive()) { // filter alive URL
					Crawler.mapSourceUrl.put(url, sourceUrl);
					// classify URL to Map group by LanguageCode first and
					// FetchDeep
					languageCode = sourceUrl.getLangCode();
					urlsGroupByFetchDeep = urlsGroupByLanguage
							.get(languageCode);
					fetchUrls = null;
					fetchDeep = sourceUrl.getFetchDeep().getValue();

					if (urlsGroupByFetchDeep == null) {
						urlsGroupByLanguage.put(languageCode,
								new HashMap<Integer, List<String>>());
					} else {
						fetchUrls = urlsGroupByLanguage.get(languageCode).get(
								fetchDeep);
					}

					if (fetchUrls == null) {
						fetchUrls = new ArrayList<String>();
						urlsGroupByLanguage.get(languageCode).put(fetchDeep,
								fetchUrls);
					}
					fetchUrls.add(url);
				}
			}
		}

		return urlsGroupByLanguage;
	}

	public static Map<LanguageCode, Map<Integer, List<SourceUrl>>> prepareDataToFetch2(
			Map<String, SourceUrl> urls) {
		Map<LanguageCode, Map<Integer, List<SourceUrl>>> urlsGroupByLanguage = null;

		if (urls != null && urls.size() > 0) {
			Iterator<String> iterator = urls.keySet().iterator();
			Map<Integer, List<SourceUrl>> urlsGroupByFetchDeep;
			List<SourceUrl> fetchUrls;
			LanguageCode languageCode;
			SourceUrl sourceUrl;
			String url;
			Integer fetchDeep;
			// reset Map Source URL
			Crawler.mapSourceUrl.clear();
			// initiate Map LanguageCode Container (support VI & EN only)
			urlsGroupByLanguage = new HashMap<LanguageCode, Map<Integer, List<SourceUrl>>>();

			while (iterator.hasNext()) {
				url = iterator.next();
				sourceUrl = urls.get(url);

				if (sourceUrl.isAlive()) { // filter alive URL
					Crawler.mapSourceUrl.put(url, sourceUrl);
					// classify URL to Map group by LanguageCode first and
					// FetchDeep
					languageCode = sourceUrl.getLangCode();
					urlsGroupByFetchDeep = urlsGroupByLanguage
							.get(languageCode);
					fetchUrls = null;
					fetchDeep = sourceUrl.getFetchDeep().getValue();

					if (urlsGroupByFetchDeep == null) {
						urlsGroupByLanguage.put(languageCode,
								new HashMap<Integer, List<SourceUrl>>());
					} else {
						fetchUrls = urlsGroupByLanguage.get(languageCode).get(
								fetchDeep);
					}

					if (fetchUrls == null) {
						fetchUrls = new ArrayList<SourceUrl>();
						urlsGroupByLanguage.get(languageCode).put(fetchDeep,
								fetchUrls);
					}
					fetchUrls.add(sourceUrl);
				}
			}
		}

		return urlsGroupByLanguage;
	}

	/*
	 * loop via urls, check url is alive or not then update status for it
	 */
	public static Map<String, SourceUrl> normalizeUrls(
			Map<String, SourceUrl> urls) {
		if (urls != null && urls.size() > 0) {
			Map<String, SourceUrl> normalizeUrls = urls;
			Iterator<String> iterator = normalizeUrls.keySet().iterator();
			String url;

			while (iterator.hasNext()) {
				url = iterator.next();

				if (isAliveUrl(url)) {
					normalizeUrls.get(url).setAlive(true);
				} else {
					normalizeUrls.get(url).setAlive(false);
				}
			}

			return normalizeUrls;
		}

		return null;
	}

	public static void main(String[] args) {
//		while (true)
//			deleteFilesOlderThanNdays(0, "/ttxvn-resources/test", logger);
		
		
		try {
			Map<String, SourceUrl> urls = new HashMap<String, SourceUrl>();
			// urls.put("www.facebook.com/phongcachcuocsong", new
			// SourceUrl("facebook.com", "vi", 80));
			// urls.put("twitter.com/document", new SourceUrl("twitter.com",
			// "vi", 80));
			// urls.put("www.vsi-international.com", new
			// SourceUrl("vsi-international.com", "vi", 80));

			// urls.put("batdongsan.vietnamnet.vn/fms/chinh-sach-quy-hoach/115630/-khong-ai-muon-pho-co-ha-noi-thanh-khu-do-thi-moi-.html",
			// new
			// SourceUrl("batdongsan.vietnamnet.vn/fms/chinh-sach-quy-hoach/115630/-khong-ai-muon-pho-co-ha-noi-thanh-khu-do-thi-moi-.html",
			// "vietnamnet.vn", LanguageCode.VI, 80, FetchDeep.ONE,
			// CrawlingStatus.NOT_CRAWLING));
			//
			// urls.put("batdongsan.vietnamnet.vn/fms/doanh-nghiep-du-an/109988/truyen-thong-nham-lan-ve-n-h-o-va-thuong-hieu-first-home-.html",
			// new
			// SourceUrl("batdongsan.vietnamnet.vn/fms/doanh-nghiep-du-an/109988/truyen-thong-nham-lan-ve-n-h-o-va-thuong-hieu-first-home-.html",
			// "vietnamnet.vn", LanguageCode.VI, 80, FetchDeep.ONE,
			// CrawlingStatus.NOT_CRAWLING));
			//
			// urls.put("vietnamnet.vn/vn/chinh-tri/205698/bio-rad-hoi-lo-quan-chuc-y-te-nhu-the-nao-.html",
			// new
			// SourceUrl("vietnamnet.vn/vn/chinh-tri/205698/bio-rad-hoi-lo-quan-chuc-y-te-nhu-the-nao-.html",
			// "vietnamnet.vn", LanguageCode.VI, 80, FetchDeep.ONE,
			// CrawlingStatus.NOT_CRAWLING));
			//
			// urls.put("gamesao.vietnamnet.vn/lang-game/gap-nu-dieu-hanh-cuc-tomboy-cua-advance-dino-5233.html",
			// new
			// SourceUrl("gamesao.vietnamnet.vn/lang-game/gap-nu-dieu-hanh-cuc-tomboy-cua-advance-dino-5233.html",
			// "vietnamnet.vn", LanguageCode.VI, 80, FetchDeep.ONE,
			// CrawlingStatus.NOT_CRAWLING));
			// //
			// urls.put("vnmedia.vn/VN/Suc-khoe/Tin-tuc/",
			// new SourceUrl("vnmedia.vn/VN/Suc-khoe/Tin-tuc/", "vnmedia.vn",
			// LanguageCode.VI, 80, FetchDeep.ONE,
			// CrawlingStatus.NOT_CRAWLING));
			//
			// urls.put("soha.vn/giai-tri/am-nhac.htm",
			// new SourceUrl("soha.vn/giai-tri/am-nhac.htm", "soha.vn",
			// LanguageCode.VI, 80, FetchDeep.ONE,
			// CrawlingStatus.NOT_CRAWLING));
			urls.put("dantri.com.vn", new SourceUrl("dantri.com.vn",
					"dantri.com.vn", LanguageCode.VI, 80, FetchDeep.TWO,
					CrawlingStatus.NOT_CRAWLING));
			// urls.put("vnmedia.vn/", new SourceUrl("vnmedia.vn/",
			// "vnmedia.vn", LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("www.doisongphapluat.com", new
			// SourceUrl("www.doisongphapluat.com", "doisongphapluat.com",
			// LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("vnexpress.net", new SourceUrl("vnexpress.net",
			// "vnexpress.net", LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("laodong.com.vn", new SourceUrl("laodong.com.vn",
			// "laodong.com.vn", LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("ngoisao.net/", new SourceUrl("ngoisao.net/",
			// "ngoisao.net", LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("dangcongsan.vn/cpv/", new
			// SourceUrl("dangcongsan.vn/cpv/", "dangcongsan.vn",
			// LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("news.zing.vn", new SourceUrl("news.zing.vn",
			// "news.zing.vn", LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));
			// urls.put("www.facebook.com/bachtv", new
			// SourceUrl("www.facebook.com/bachtv", "facebook.com/bachtv",
			// LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.NOT_CRAWLING));

			// urls.put("www.doisongphapluat.com", new
			// SourceUrl("www.doisongphapluat.com", "doisongphapluat.com",
			// LanguageCode.VI, 80, FetchDeep.TWO, CrawlingStatus.CRAWLED));
			// urls.put("laodong.com.vn", new SourceUrl("laodong.com.vn",
			// "laodong.com.vn", LanguageCode.VI, 80, FetchDeep.TWO,
			// CrawlingStatus.CRAWLED));
			// urls.put("edition.cnn.com/", new SourceUrl("edition.cnn.com/",
			// "edition.cnn.com", LanguageCode.EN, 70, FetchDeep.TWO));
			// urls.put("drupal.genixventures.com", new
			// SourceUrl("drupal.genixventures.com", "en", 80));
			// urls.put("www.france24.com/en/",new
			// SourceUrl("france24.com/en/","en",100));
			// urls.put("www.xinhuanet.com/english/",new
			// SourceUrl("xinhuanet.com/english/","en", 100));
			// urls.put("www.mirror.co.uk/news/",new
			// SourceUrl("mirror.co.uk/news/","en", 90));
			// urls.put("www.breakingnews.com/",new
			// SourceUrl("breakingnews.com/","en", 100));
			// urls.put("edition.cnn.com/",new
			// SourceUrl("edition.cnn.com/","en", 70));
			// urls.put("www.cbsnews.com/",new SourceUrl("cbsnews.com/","en",
			// 100));
			// urls.put("www.foxnews.com",new SourceUrl("foxnews.com","en",
			// 70));
			// urls.put("www.bloomberg.com",new SourceUrl("bloomberg.com","en",
			// 90));
			// urls.put("www.nbcnews.com",new SourceUrl("nbcnews.com","en",
			// 100));
			// urls.put("www.newser.com",new SourceUrl("newser.com","en", 100));
			// urls.put("www.ctvnews.ca",new SourceUrl("ctvnews.ca","en", 70));
			// urls.put("www.usatoday.com",new SourceUrl("usatoday.com","en",
			// 90));
			// urls.put("www.cbc.ca/news",new SourceUrl("cbc.ca/news","en",
			// 100));
			// urls.put("www.nytimes.com",new SourceUrl("nytimes.com","en",
			// 90));
			// urls.put("www.theguardian.com/uk",new
			// SourceUrl("theguardian.com","en", 70));
			// urls.put("www.alexa.com/topsites/category/top/news",new
			// SourceUrl("alexa.com","en", 100));
			// urls.put("www.afp.com/en/home",new
			// SourceUrl("afp.com/en/home","en", 100));
			// urls.put("www.reuters.com",new SourceUrl("reuters.com","en",
			// 100));
			// urls.put("dantri.com.vn",new SourceUrl("dantri.com.vn","vi",
			// 70));
			// urls.put("www.tienphong.vn",new SourceUrl("tienphong.vn","vi",
			// 90));
			// urls.put("nhandan.com.vn",new SourceUrl("nhandan.com.vn","vi",
			// 100));
			// urls.put("chinhphu.vn",new SourceUrl("chinhphu.vn","vi", 100));
			// urls.put("sggp.org.vn",new SourceUrl("sggp.org.vn","vi", 90));
			// urls.put("vnmedia.vn",new SourceUrl("vnmedia.vn","en", 100));
			// urls.put("tuoitre.vn", new SourceUrl("tuoitre.vn", "vi", 70));
			// urls.put("www.thanhnien.com.vn/pages/default.aspx", new
			// SourceUrl("thanhnien.com.vn", "vi", 100));
			// urls.put("daidoanket.vn",new SourceUrl("daidoanket.vn","vi",
			// 90));
			// urls.put("www.nguoiduatin.vn", new SourceUrl("nguoiduatin.vn",
			// "en", 100));
			// urls.put("petrotimes.vn",new SourceUrl("petrotimes.vn","en",
			// 70));
			// urls.put("www.thesaigontimes.vn",new
			// SourceUrl("thesaigontimes.vn","vi", 60));
			// urls.put("www.baomoi.com",new SourceUrl("baomoi.com","vi", 70));
			// urls.put("docbao.vn", new SourceUrl("docbao.vn", "vi", 100));
			// urls.put("www.thanhtra.com.vn",new
			// SourceUrl("thanhtra.com.vn","vi", 70));
			// urls.put("dangcongsan.vn/cpv/",new
			// SourceUrl("dangcongsan.vn","vi", 60));
			// urls.put("www.kyodonews.jp/english/",new
			// SourceUrl("kyodonews.jp","en", 100));
			// urls.put("www.bbc.co.uk/news/",new
			// SourceUrl("bbc.co.uk/news/","en", 100));
			// urls.put("www.news.com.au",new SourceUrl("news.com.au","en",
			// 60));
			// urls.put("news.yahoo.com",new SourceUrl("news.yahoo.com","en",
			// 60));
			// urls.put("time.com",new SourceUrl("time.com","en", 60));
			// urls.put("hanoimoi.com.vn",new SourceUrl("hanoimoi.com.vn","vi",
			// 100));
			// urls.put("laodong.com.vn",new SourceUrl("laodong.com.vn","en",
			// 50));
			// urls.put("infonet.vn",new SourceUrl("infonet.vn","vi", 100));
			// urls.put("news.google.com.vn",new
			// SourceUrl("news.google.com.vn","en", 50));
			// urls.put("vtv.vn",new SourceUrl("vtv.vn","vi", 50));
			// urls.put("vov.vn",new SourceUrl("vov.vn","vi", 100));
			// urls.put("plo.vn",new SourceUrl("plo.vn","vi", 50));
			// urls.put("abcnews.go.com",new SourceUrl("abcnews.go.com","en",
			// 100));
			// urls.put("vnexpress.net",new SourceUrl("vnexpress.net","vi",
			// 50));
			// urls.put("news.zing.vn",new SourceUrl("news.zing.vn","vi", 100));
			// urls.put("vtc.vn",new SourceUrl("vtc.vn","vi", 100));
			// urls.put("vietnamnet.vn",new SourceUrl("vietnamnet.vn","vi",
			// 100));
			// urls.put("danviet.vn",new SourceUrl("danviet.vn","vi", 30));
			// urls.put("english.cntv.cn",new SourceUrl("english.cntv.cn","en",
			// 20));
			// urls.put("english.yonhapnews.co.kr",new
			// SourceUrl("english.yonhapnews.co.kr","en", 100));
			// urls.put("english.kbs.co.kr",new
			// SourceUrl("english.kbs.co.kr","en", 30));
			// urls.put("giaoduc.net.vn",new SourceUrl("giaoduc.net.vn","vi",
			// 30));
			// urls.put("vn.news.yahoo.com/",new
			// SourceUrl("vn.news.yahoo.com/","vi", 100));
			// urls.put("www3.nhk.or.jp/nhkworld/",new
			// SourceUrl("nhk.or.jp/nhkworld/","en", 100));
			Crawler.crawl(urls);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void deleteFilesOlderThanNdays(int daysBack, String dirWay,
			Logger log) {
		File directory = new File(dirWay);
		if (directory.exists()) {
			File[] listFiles = directory.listFiles();
			long purgeTime = System.currentTimeMillis() - (daysBack * 24 * 60 * 60 * 1000);
			if (listFiles != null && listFiles.length > 0) {
				for (File aFile : listFiles) {
					if (aFile != null) {
						if (aFile.lastModified() < purgeTime) {						
							if (aFile.isFile()) {
								if (!aFile.delete())
									log.error("Unable to delete file: " + aFile);
							} else {
								try {
									FileUtils.deleteDirectory(aFile);
								} catch (IOException e) {
									log.error("Unable to delete folder: " + aFile + " caused by " + e);
								}
							}
						}
					}
					
				}
			}
			
		} else {
			log.warn("Files were not deleted, directory " + dirWay
					+ " does'nt exist!");
		}
	}
}