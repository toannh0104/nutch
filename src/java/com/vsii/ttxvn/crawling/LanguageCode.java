/*
 * FILENAME
 *     LanguageCode.java
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

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * The class for purpose of.
 * </p>
 * 
 * @version 1.0
 * @author phunv
 **/
public enum LanguageCode
{
    EN(0, "EN"),
    VI(1, "VI");

    private Integer value;
    private String code;

    private LanguageCode(Integer value, String code)
    {
        this.value = value;
        this.code = code;
    }

    /**
     * <p>
     * Getter for value.
     * </p>
     * 
     * @return the value
     */
    public Integer getValue()
    {
        return value;
    }

    /**
     * <p>
     * Getter for code.
     * </p>
     * 
     * @return the code
     */
    public String getCode()
    {
        return code;
    }

    public static List<LanguageCode> list()
    {
        return Arrays.asList(values());
    }

    public static LanguageCode parse(String code)
    {
        for (LanguageCode langCode : list())
        {
            if (langCode.getCode().equalsIgnoreCase(code))
            {
                return langCode;
            }
        }

        return LanguageCode.VI;
    }
}
