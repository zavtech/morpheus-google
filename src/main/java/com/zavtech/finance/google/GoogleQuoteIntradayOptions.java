/**
 * Copyright (C) 2014-2016 Xavier Witdouck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.finance.google;

import java.time.LocalDateTime;
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Initialiser;

/**
 * A DataFrameRequest implementation to request intraday quotes from Google Finance.
 *
 * Any use of the extracted data from this software should adhere to Google Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class GoogleQuoteIntradayOptions implements DataFrameSource.Options<LocalDateTime, String> {

    private String ticker;
    private int dayCount;

    /**
     * Constructor
     */
    public GoogleQuoteIntradayOptions() {
        this(null, 5);
    }

    /**
     * Constructor
     * @param ticker    the security ticker
     * @param dayCount  the day count
     */
    public GoogleQuoteIntradayOptions(String ticker, int dayCount) {
        this.ticker = ticker;
        this.dayCount = dayCount;
    }

    @Override
    public void validate() {
        Asserts.notNull(getTicker(), "The security ticker must be specified");
    }

    /**
     * Returns the ticker for request
     * @return  the ticker for request
     */
    public String getTicker() {
        return ticker;
    }

    /**
     * Returns the day count for this request
     * @return  the day count for request
     */
    public int getDayCount() {
        return dayCount;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public void setDayCount(int dayCount) {
        this.dayCount = dayCount;
    }
}
