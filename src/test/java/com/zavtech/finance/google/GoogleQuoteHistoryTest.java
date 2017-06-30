/**
 * Copyright (C) 2014-2016 Xavier Witdouck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.finance.google;

import java.time.LocalDate;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test for the DataFrame source to load end of day quotes from Google Finance
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class GoogleQuoteHistoryTest {

    private GoogleQuoteHistorySource source = new GoogleQuoteHistorySource();
    private Array<String> fields = Array.of("Open", "High", "Low", "Close", "Volume", "Change", "ChangePercent");

    @DataProvider(name="tickers")
    public Object[][] tickers() {
        return new Object[][] { {"AAPL"}, { "IBM" }, { "GE" }, { "MMM" } };
    }

    @Test(dataProvider = "tickers")
    public void testQuoteRequest(String ticker) {
        final LocalDate start = LocalDate.of(2006, 1, 1);
        final LocalDate end = LocalDate.of(2009, 1, 1);
        final GoogleQuoteHistoryOptions options = new GoogleQuoteHistoryOptions(ticker, start, end);
        final DataFrame<LocalDate,String> frame = source.read(options);
        Assert.assertTrue(frame.rowCount() > 0, "There are rows in the frame");
        Assert.assertTrue(frame.colCount() > 0, "There are columns in the frame");
        fields.forEach(field -> Assert.assertTrue(frame.cols().contains(field), "The DataFrame contains column for " + field));
        Assert.assertTrue(frame.rows().firstKey().get().compareTo(start) >= 0);
        Assert.assertTrue(frame.rows().lastKey().get().compareTo(end) <= 0);
        IntStream.range(1, frame.rowCount()).forEach(rowIndex -> {
            final LocalDate previous = frame.rows().key(rowIndex-1);
            final LocalDate current = frame.rows().key(rowIndex);
            final double previousClose = frame.data().getDouble(rowIndex-1, "Close");
            final double currentClose = frame.data().getDouble(rowIndex, "Close");
            final double expectedChange = currentClose - previousClose;
            final double actualChange = frame.data().getDouble(rowIndex, "Change");
            final double expectedChangePercent = currentClose / previousClose - 1d;
            Assert.assertTrue(previous.isBefore(current), "Dates are in ascending order");
            Assert.assertTrue(frame.data().getDouble(rowIndex, "High") >= frame.data().getDouble(rowIndex, "Low"), "High >= Low");
            Assert.assertEquals(actualChange, expectedChange, 0.0001, "The price change is calculated correctly at " + frame.rows().key(rowIndex));
            Assert.assertEquals(frame.data().getDouble(rowIndex, "ChangePercent"), expectedChangePercent, 0.00001);
        });
    }


}