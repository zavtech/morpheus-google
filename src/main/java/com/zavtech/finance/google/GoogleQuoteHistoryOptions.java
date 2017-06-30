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

import java.time.LocalDate;
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Initialiser;

/**
 * A DataFrameRequest implementation to request historical end of day quotes from Google Finance.
 *
 * Any use of the extracted data from this software should adhere to Google Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class GoogleQuoteHistoryOptions implements DataFrameSource.Options<LocalDate,String> {

    private String ticker;
    private LocalDate start;
    private LocalDate end;

    /**
     * Constructor
     */
    public GoogleQuoteHistoryOptions() {
        this(null, null, null);
    }

    /**
     * Constructor
     * @param ticker    the security ticker
     * @param start     the start date for range
     * @param end       the end date for range
     */
    public GoogleQuoteHistoryOptions(String ticker, LocalDate start, LocalDate end) {
        this.ticker = ticker;
        this.start = start;
        this.end = end;
    }

    @Override
    public void validate() {
        Asserts.notNull(getTicker(), "The security ticker must be specified");
        Asserts.notNull(getStart(), "The start date must be specified");
        Asserts.notNull(getEnd(), "The end date must be specified");
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public void setStart(LocalDate start) {
        this.start = start;
    }

    public void setEnd(LocalDate end) {
        this.end = end;
    }

    /**
     * Returns the security ticker for this request
     * @return  the security ticker for request
     */
    public String getTicker() {
        return ticker;
    }

    /**
     * Returns the start date for request
     * @return  the start date for request
     */
    public LocalDate getStart() {
        return start;
    }

    /**
     * Returns the end date for request
     * @return  the end date for request
     */
    public LocalDate getEnd() {
        return end;
    }

}
