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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.TextStreamReader;

/**
 * A DataFrameSource implementation that loads intraday prices from Google Finance.
 *
 * Any use of the extracted data from this software should adhere to Google Terms and Conditions.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class GoogleQuoteIntradaySource extends DataFrameSource<LocalDateTime,String,GoogleQuoteIntradaySource.Options> {

    private String urlTemplate;

    /**
     * Constructor
     */
    public GoogleQuoteIntradaySource() {
        this("http://www.google.com/finance/getprices?i=60&p=<DAYS>d&f=d,o,h,l,c,v&df=cpct&q=<TICKER>");
    }

    /**
     * Constructor
     * @param urlTemplate   the UTL template string
     */
    public GoogleQuoteIntradaySource(String urlTemplate) {
        this.urlTemplate = urlTemplate;
    }


    @Override
    public DataFrame<LocalDateTime, String> read(Consumer<Options> configurator) throws DataFrameException {
        return DataFrameOptions.whileNotIgnoringDuplicates(() -> {
            TextStreamReader reader = null;
            final Options options = initOptions(new Options(), configurator);
            try {
                long interval = 0L;
                long startTime = 0L;
                final URL url = new URL(urlTemplate.replace("<DAYS>", String.valueOf(options.dayCount)).replace("<TICKER>", options.ticker));
                reader = new TextStreamReader(url.openStream());
                final Matcher intervalMatcher = Pattern.compile("INTERVAL=(\\d+)").matcher("");
                final Matcher firstPriceLineMatcher = Pattern.compile("a(\\d+),(.*),(.*),(.*),(.*),(.*)").matcher("");
                final Matcher standardPriceLineMatcher = Pattern.compile("(\\d+),(.*),(.*),(.*),(.*),(.*)").matcher("");
                final Array<String> fields = Array.of("Open", "High", "Low", "Close", "Volume", "Change", "ChangePercent");
                final ZoneId zoneId = ZoneId.of("America/New_York");
                final Index<LocalDateTime> index = Index.of(LocalDateTime.class, 5000);
                final DataFrame<LocalDateTime,String> frame = DataFrame.ofDoubles(index, fields);
                while (reader.hasNext()) {
                    final String line = reader.nextLine();
                    if (intervalMatcher.reset(line).matches()) {
                        interval = Long.parseLong(intervalMatcher.group(1));
                    } else if (firstPriceLineMatcher.reset(line).matches()) {
                        startTime = Long.parseLong(firstPriceLineMatcher.group(1));
                        final Instant instant = Instant.ofEpochSecond(startTime);
                        final LocalDateTime timestamp = ZonedDateTime.ofInstant(instant, zoneId).toLocalDateTime();
                        frame.rows().add(timestamp, v -> {
                            switch (v.colOrdinal()) {
                                case 0:  return Double.parseDouble(firstPriceLineMatcher.group(2));
                                case 1:  return Double.parseDouble(firstPriceLineMatcher.group(3));
                                case 2:  return Double.parseDouble(firstPriceLineMatcher.group(4));
                                case 3:  return Double.parseDouble(firstPriceLineMatcher.group(5));
                                case 4:  return Double.parseDouble(firstPriceLineMatcher.group(6));
                                default: return v.getDouble();
                            }
                        });
                    } else if (standardPriceLineMatcher.reset(line).matches()) {
                        final long increment = Long.parseLong(standardPriceLineMatcher.group(1));
                        final long seconds = increment * interval;
                        final Instant instant = Instant.ofEpochSecond(startTime + seconds);
                        final LocalDateTime timestamp = ZonedDateTime.ofInstant(instant, zoneId).toLocalDateTime();
                        frame.rows().add(timestamp, v -> {
                            switch (v.colOrdinal()) {
                                case 0:  return Double.parseDouble(standardPriceLineMatcher.group(2));
                                case 1:  return Double.parseDouble(standardPriceLineMatcher.group(3));
                                case 2:  return Double.parseDouble(standardPriceLineMatcher.group(4));
                                case 3:  return Double.parseDouble(standardPriceLineMatcher.group(5));
                                case 4:  return Double.parseDouble(standardPriceLineMatcher.group(6));
                                default: return v.getDouble();
                            }
                        });
                    }
                }
                return  calculateChanges(frame);
            } catch (Exception ex) {
                throw new DataFrameException("Failed to load intraday quotes from Google finance for " + options.ticker, ex);
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        });
    }

    /**
     * Calculates price changes from close to close
     * @param frame the quote frame
     * @return      the same as input
     */
    private DataFrame<LocalDateTime,String> calculateChanges(DataFrame<LocalDateTime,String> frame) {
        IntStream.range(1, frame.rowCount()).forEach(rowIndex -> {
            final double previous = frame.data().getDouble(rowIndex - 1, "Close");
            final double current = frame.data().getDouble(rowIndex, "Close");
            frame.data().setDouble(rowIndex, "Change", current - previous);
            frame.data().setDouble(rowIndex, "ChangePercent", (current / previous) - 1d);
        });
        return frame;
    }


    public class Options implements DataFrameSource.Options<LocalDateTime, String> {

        private String ticker;
        private int dayCount;

        @Override
        public void validate() {
            Asserts.notNull(ticker, "The security ticker must be specified");
            Asserts.assertTrue(dayCount > 0, "The day count must be > 0");
        }

        /**
         * Sets the instrument ticker for this request
         * @param ticker    the ticker reference
         */
        public void setTicker(String ticker) {
            this.ticker = ticker;
        }

        /**
         * Sets the day count for this request
         * @param dayCount  the day count for request
         */
        public void setDayCount(int dayCount) {
            this.dayCount = dayCount;
        }
    }



    public static void main(String[] args) {
        final Array<String> tickers = Array.of("AAPL", "MSFT", "ORCL", "GE", "C");
        DataFrameSource.register(new GoogleQuoteIntradaySource());
        tickers.forEach(ticker -> {
            final DataFrame<LocalDateTime,String> frame = DataFrameSource.lookup(GoogleQuoteIntradaySource.class).read(options -> {
                options.setTicker(ticker);
                options.setDayCount(3);
            });
            frame.out().print();
        });
    }

}
