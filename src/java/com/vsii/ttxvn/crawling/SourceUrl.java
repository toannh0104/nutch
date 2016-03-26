/*
 * FILENAME
 *     SourceUrl.java
 *
 * FILE LOCATION
 *     $Source$
 *
 * VERSION
 *     $Id$
 *         @version       $Revision$
 *         Check-Out Tag: $Name$
 *         Locked By:     $Lockers$
 *
 * FORMATTING NOTES
 *     * Lines should be limited to 78 characters.
 *     * Files should contain no tabs.
 *     * Indent code using four-character increments.
 *
 * COPYRIGHT
 *     Copyright (C) 2005 vietsoftware international Inc. All rights reserved.
 *     This software is the confidential and proprietary information of
 *     VSII ("Confidential Information"). You shall not
 *     disclose such Confidential Information and shall use it only in
 *     accordance with the terms of the license agreement you entered into
 *     with VSII.
 */

package com.vsii.ttxvn.crawling;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * The class for carrying the SourceUrl info.
 * </p>
 * 
 * @version 1.0
 * @author phunv
 **/
public class SourceUrl implements Serializable
{
    private static final long serialVersionUID = 6661911829110744744L;

    private long id;
    private String url;
    private LanguageCode langCode;
    private String domain;
    private Integer reliability;
    private Date lastFetched;
    private FetchDeep fetchDeep;
    private boolean alive;
    private CrawlingStatus crawlingStatus;
    
    public SourceUrl()
    {
        super();
    }

    public SourceUrl(String url, String domain, LanguageCode langCode, Integer reliability, FetchDeep fetchDeep, CrawlingStatus crawlingStatus)
    {
        super();
        this.url = url;
        this.domain = domain;
        this.langCode = langCode;
        this.reliability = reliability;
        this.fetchDeep = fetchDeep;
        this.crawlingStatus = crawlingStatus;
    }
    
    
    
    public CrawlingStatus getCrawlingStatus() {
		return crawlingStatus;
	}

	public void setCrawlingStatus(CrawlingStatus crawlingStatus) {
		this.crawlingStatus = crawlingStatus;
	}

	public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    /**
     * @return the url
     */
    public String getUrl()
    {
        return url;
    }

    /**
     * @param url
     *            the url to set
     */
    public void setUrl(String url)
    {
        this.url = url;
    }

    /**
     * <p>
     * Getter for langCode.
     * </p>
     * 
     * @return the langCode
     */
    public LanguageCode getLangCode()
    {
        return langCode;
    }

    /**
     * <p>
     * Setting value for langCode.
     * </p>
     * 
     * @param langCode the langCode to set
     */
    public void setLangCode(LanguageCode langCode)
    {
        this.langCode = langCode;
    }

    /**
     * @return the reliability
     */
    public Integer getReliability()
    {
        return reliability;
    }

    /**
     * @param reliability
     *            the reliability to set
     */
    public void setReliability(Integer reliability)
    {
        this.reliability = reliability;
    }

    /**
     * @return the lastFetched
     */
    public Date getLastFetched()
    {
        return lastFetched;
    }

    /**
     * @param lastFetched
     *            the lastFetched to set
     */
    public void setLastFetched(Date lastFetched)
    {
        this.lastFetched = lastFetched;
    }

    public boolean isAlive()
    {
        return alive;
    }

    public void setAlive(boolean isAlive)
    {
        this.alive = isAlive;
    }

    public String getDomain()
    {
        return domain;
    }

    public void setDomain(String domain)
    {
        this.domain = domain;
    }

    public FetchDeep getFetchDeep()
    {
        return fetchDeep;
    }

    public void setFetchDeep(FetchDeep fetchDeep)
    {
        this.fetchDeep = fetchDeep;
    }

}
