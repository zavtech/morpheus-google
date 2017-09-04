/**
 * Copyright (C) 2014-2017 Xavier Witdouck
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

import java.net.URL;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.TextStreamReader;

/**
 * A DataFrameSource implementation that loads historical end of day prices from Google Finance.
 *
 * Any use of the extracted data from this software should adhere to Google Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class GoogleQuoteHistorySource extends DataFrameSource<LocalDate,String,GoogleQuoteHistorySource.Options> {

    private String urlTemplate;

    /**
     * Constructor
     */
    public GoogleQuoteHistorySource() {
        this("http://www.google.com/finance/historical?output=csv&q=<TICKER>&startdate=<START>&enddate=<END>");
    }

    /**
     * Constructor
     * @param urlTemplate   the url template to replace symbol
     */
    public GoogleQuoteHistorySource(String urlTemplate) {
        this.urlTemplate = urlTemplate;
    }

    @Override
    public DataFrame<LocalDate, String> read(Consumer<GoogleQuoteHistorySource.Options> configurator) throws DataFrameException {
        TextStreamReader reader = null;
        final Options options = initOptions(new Options(), configurator);
        try {
            final DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("d-MMM-yy");
            final DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("MMM'+'dd'%2C+'yyyy");
            final String start = formatter2.format(options.start);
            final String end = formatter2.format(options.end);
            final String urlString = urlTemplate.replace("<TICKER>", options.ticker).replace("<START>", start).replace("<END>", end);
            final Index<LocalDate> index = Index.of(LocalDate.class, 1000);
            final Array<String> columns = Array.of("Open", "High", "Low", "Close", "Volume", "Change", "ChangePercent");
            final DataFrame<LocalDate,String> frame = DataFrame.ofDoubles(index, columns);
            final Matcher lineMatcher = Pattern.compile("(\\d{1,2}?-\\p{Alpha}{3}-\\d{2}),(.+),(.+),(.+),(.+),(.+)").matcher("");
            reader = new TextStreamReader(new URL(urlString).openStream());
            while (reader.hasNext()) {
                final String line = reader.nextLine();
                if (lineMatcher.reset(line).matches()) {
                    final LocalDate localDate = LocalDate.parse(lineMatcher.group(1), formatter1);
                    frame.rows().add(localDate, v -> {
                        switch (v.colOrdinal()) {
                            case 0: return Double.parseDouble(lineMatcher.group(2));
                            case 1: return Double.parseDouble(lineMatcher.group(3));
                            case 2: return Double.parseDouble(lineMatcher.group(4));
                            case 3: return Double.parseDouble(lineMatcher.group(5));
                            case 4: return Double.parseDouble(lineMatcher.group(6));
                            default: return v.getDouble();
                        }
                    });
                }
            }
            return calculateChanges(frame);
        } catch (Exception ex) {
            throw new DataFrameException("Failed to load historical quotes from Google Finance for " + options.ticker, ex);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }


    /**
     * Calculates price changes from close to close
     * @param frame the quote frame
     * @return      the same as input
     */
    private DataFrame<LocalDate,String> calculateChanges(DataFrame<LocalDate,String> frame) {
        frame.rows().sort((row1, row2) -> row1.key().compareTo(row2.key()));
        IntStream.range(1, frame.rowCount()).forEach(rowIndex -> {
            final double previous = frame.data().getDouble(rowIndex - 1, "Close");
            final double current = frame.data().getDouble(rowIndex, "Close");
            frame.data().setDouble(rowIndex, "Change", current - previous);
            frame.data().setDouble(rowIndex, "ChangePercent", (current / previous) - 1d);
        });
        return frame;
    }



    public class Options implements DataFrameSource.Options<LocalDate,String> {

        private String ticker;
        private LocalDate start;
        private LocalDate end;


        @Override
        public void validate() {
            Asserts.notNull(ticker, "The security ticker must be specified");
            Asserts.notNull(start, "The start date must be specified");
            Asserts.notNull(end, "The end date must be specified");
            Asserts.assertTrue(start.compareTo(end) < 0, "The start date must be < end date");
        }

        /**
         * Sets the instrument ticker for this request
         * @param ticker    the instrument ticker, such as AAPL for apple
         */
        public void setTicker(String ticker) {
            this.ticker = ticker;
        }

        /**
         * Sets the start date for this request
         * @param start     the start date
         */
        public void setStart(LocalDate start) {
            this.start = start;
        }

        /**
         * Sets the end date for this request
         * @param end       the end date
         */
        public void setEnd(LocalDate end) {
            this.end = end;
        }
    }



    public static void main(String[] args) {
        final LocalDate start = LocalDate.of(2000, 1, 1);
        final LocalDate end = LocalDate.of(2014, 1, 1);
        final Array<String> tickers = Array.of("AAPL");
        DataFrameSource.register(new GoogleQuoteHistorySource());
        tickers.forEach(ticker -> {
            final DataFrame<LocalDate,String> frame = DataFrameSource.lookup(GoogleQuoteHistorySource.class).read(options -> {
                options.setTicker(ticker);
                options.setStart(start);
                options.setEnd(end);
            });
            frame.out().print();
        });
    }

}
