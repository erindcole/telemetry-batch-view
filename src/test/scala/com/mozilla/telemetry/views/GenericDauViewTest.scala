/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import org.scalatest.{FlatSpec, Matchers}

class GenericDauViewTest extends FlatSpec with Matchers {
  // write raw test dataset with date, channel, country, client_id to temp dir
  //   date: 28 days
  //   channel: {release,beta}
  //   country: {a,b,c,d,e}
  //   client_id: indexof(country)*indexof(channel) + 1 from yesterday + 1 from yestercountry
  // create hll test dataset from raw dataset

  // for each test
  // generate dau from hll
  // generate dau from raw
  // check mau/smooth_dau/dau from hll is within some % of raw
  // check that columns are correct

  "dau" must "be accurate" in {
    // no options
  }

  "columns" must "calculate by column" in {
    // --columns channel
  }

  "outlierThreshold" must "filter outliers" in {
    // --outlier-threshold 5
  }

  "top" must "limit values" in {
    // --top-column country --top-count 3
  }

  "where" must "limit values" in {
    // --where channel='release'
  }

  "options" must "work together" in {
    // --columns country --outlier-threshold 5 --top-column country --top-count 3 --where channel='release'
  }
}
