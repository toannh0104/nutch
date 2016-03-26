/*
 * FILENAME
 *     FetchDeep.java
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
 * The class for purpose of storing deep to crawl
 * </p>
 * 
 * @version 1.0
 * @author phunv
 **/
public enum FetchDeep {

	UNIDENTIFIED(0),
	ONE(1),
	TWO(2),
	THREE(3),
	FOUR(4),
	FIVE(5);

	private Integer value;

	private FetchDeep(Integer value) {
		this.value = value;
	}

	public Integer getValue() {
		return value;
	}

	public static List<FetchDeep> list() {
		return Arrays.asList(values());
	}

	public static FetchDeep parse(Integer value) {
		for (FetchDeep fd : list()) {
			if (fd.getValue() == value) {
				return fd;
			}
		}

		return null;
	}
}
