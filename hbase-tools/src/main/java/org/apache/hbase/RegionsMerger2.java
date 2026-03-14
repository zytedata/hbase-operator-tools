/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.Random;

import static org.apache.hadoop.hbase.HConstants.*;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;

/**
 * HBase maintenance tool for merging regions of a specific table, until a target number of regions
 * for the table is reached, or no more merges can complete due to limit in resulting merged region
 * size.
 */
public class RegionsMerger2 extends Configured implements org.apache.hadoop.util.Tool {
  private static final long GIGABYTE = 1024*1024*1024;

  private static final Logger LOG = LoggerFactory.getLogger(RegionsMerger2.class.getName());
  public static final String RESULTING_REGION_UPPER_MARK = "hbase.tools.merge.upper.mark";
  public static final String SLEEP = "hbase.tools.merge.sleep";
  public static final String MAX_SLEEP_ROUNDS = "hbase.tools.merge.sleep.rounds";
  public static final String MAX_ROUNDS_IDLE = "hbase.tools.max.iterations.blocked";
  public static final String MAX_MERGES_PER_ROUND = "hbase.tools.max.merges.perround";
  public static final String MIN_REGION_AGE = "hbase.tools.min.region.age";

  private final Configuration conf;
  private final FileSystem fs;
  private final double resultSizeThreshold;
  private final int sleepBetweenCycles;
  private final int maxSleepRoundsBetweenCycles;
  private final int maxRoundsStuck;
  private final int maxMergesPerRound;
  private final int minRegionAgeDays;

  private ExecutorService executor;

  public RegionsMerger2(Configuration conf) throws IOException {
    this.conf = conf;
    Path basePath = new Path(conf.get(HConstants.HBASE_DIR));
    fs = basePath.getFileSystem(conf);
    resultSizeThreshold = this.conf.getDouble(RESULTING_REGION_UPPER_MARK, 0.7)
      * this.conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
    sleepBetweenCycles = this.conf.getInt(SLEEP, 15000);
    this.maxSleepRoundsBetweenCycles = this.conf.getInt(MAX_SLEEP_ROUNDS, 1);
    this.maxRoundsStuck = this.conf.getInt(MAX_ROUNDS_IDLE, 15);
    this.maxMergesPerRound = this.conf.getInt(MAX_MERGES_PER_ROUND, 30);
    this.minRegionAgeDays = this.conf.getInt(MIN_REGION_AGE, 7);
    this.executor = Executors.newFixedThreadPool(this.maxMergesPerRound);
  }

  private Path getTablePath(TableName table) {
    Path basePath = new Path(conf.get(HConstants.HBASE_DIR));
    basePath = new Path(basePath, "data");
    Path tablePath = new Path(basePath, table.getNamespaceAsString());
    return new Path(tablePath, table.getQualifierAsString());
  }

  private long sumSizeInFS(Path parentPath) throws IOException {
    long size = 0;
    FileStatus[] files = this.fs.listStatus(parentPath);
    for (FileStatus f : files) {
      if (f.isFile()) {
        size += f.getLen();
      } else if (f.isDirectory()) {
        size += sumSizeInFS(f.getPath());
      }
    }
    return size;
  }

  private List<RegionInfo> getOpenRegions(Connection connection, TableName table) throws Exception {
    List<RegionInfo> regions = new ArrayList<>();
    Table metaTbl = connection.getTable(META_TABLE_NAME);
    String tblName = table.getNameAsString();
    RowFilter rowFilter =
      new RowFilter(CompareOperator.EQUAL, new SubstringComparator(tblName + ","));
    SingleColumnValueFilter colFilter = new SingleColumnValueFilter(CATALOG_FAMILY, STATE_QUALIFIER,
      CompareOperator.EQUAL, Bytes.toBytes("OPEN"));
    colFilter.setFilterIfMissing(true);
    Scan scan = new Scan();
    FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filter.addFilter(rowFilter);
    filter.addFilter(colFilter);
    scan.setFilter(filter);
    try (ResultScanner rs = metaTbl.getScanner(scan)) {
      Result r;
      while ((r = rs.next()) != null) {
        RegionInfo region = RegionInfo.parseFrom(r.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER));
        regions.add(region);
      }
    }
    return regions;
  }

  private boolean canMerge(Path path, RegionInfo region1, RegionInfo region2,
    Collection<Pair<RegionInfo, RegionInfo>> alreadyMerging) throws IOException {
    if (
      alreadyMerging.stream()
        .anyMatch(regionPair -> region1.equals(regionPair.getFirst())
          || region2.equals(regionPair.getFirst()) || region1.equals(regionPair.getSecond())
          || region2.equals(regionPair.getSecond()))
    ) {
      return false;
    }
    if (RegionInfo.areAdjacent(region1, region2)) {
      long size1 = sumSizeInFS(new Path(path, region1.getEncodedName())) / GIGABYTE;
      long size2 = sumSizeInFS(new Path(path, region2.getEncodedName())) / GIGABYTE;
      boolean mergeable = (resultSizeThreshold > (size1 + size2));
      if (!mergeable) {
        LOG.warn(
          "Not merging regions {} and {} because resulting region size would get close to "
            + "the {} GB limit. {} total size: {} GB; {} total size:{} GB",
          region1.getEncodedName(), region2.getEncodedName(), resultSizeThreshold,
          region1.getEncodedName(), size1, region2.getEncodedName(), size2);
      }
      return mergeable;
    } else {
      LOG.warn("WARNING: Can't merge regions {} and {} because those are not adjacent.",
        region1.getEncodedName(), region2.getEncodedName());
      return false;
    }
  }

  private boolean hasPreviousMergeRef(Connection conn, RegionInfo region) throws Exception {
    Table meta = conn.getTable(TableName.META_TABLE_NAME);
    Get get = new Get(region.getRegionName());
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = meta.get(get);
    boolean result = HBCKMetaTableAccessor.getMergeRegions(r.rawCells()) != null;
    if (result) {
      LOG.warn("Region {} has an existing merge qualifier and can't be merged until for now. \n "
        + "RegionsMerger will skip this region until merge qualifier is cleaned away. \n "
        + "Consider major compact this region.", region.getEncodedName());
    }
    return result;
  }

  private Future<Void> requestMergeRegions(Admin admin, RegionInfo current,
                                                   RegionInfo previous,
                                                   Map<Future<Void>, Pair<RegionInfo, RegionInfo>> regionsMerging) {
    return executor.submit(() -> {
      Future<Void> f = admin.mergeRegionsAsync(current.getEncodedNameAsBytes(),
        previous.getEncodedNameAsBytes(), false);

      Pair<RegionInfo, RegionInfo> regionPair = new Pair<>(previous, current);
      regionsMerging.put(f, regionPair);
      return null;
    });
  }

  public void mergeRegions(String tblName, int maxNumberOfMerges) throws Exception {
    LOG.info("Table name             : {}", tblName);
    LOG.info("Max number of merges   : {}", maxNumberOfMerges);
    LOG.info("Result size threshold  : {} GB", resultSizeThreshold / GIGABYTE);
    LOG.info("Sleep between cycles   : {}", sleepBetweenCycles);
    LOG.info("Max rounds stuck       : {}", maxRoundsStuck);
    LOG.info("Max merges per round   : {}", maxMergesPerRound);
    LOG.info("Min region age in days : {}", minRegionAgeDays);

    Random rand = new Random();
    long minRegionAgeMilliseconds = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(minRegionAgeDays);

    TableName table = TableName.valueOf(tblName);
    Path tableDir = getTablePath(table);
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      Admin admin = conn.getAdmin();
      LongAdder counter = new LongAdder();
      //LongAdder lastTimeProgressed = new LongAdder();
      LongAdder merges = new LongAdder();
      LongAdder mergesThisRound = new LongAdder();
      LongAdder waitingRounds = new LongAdder();
      LongAdder failureCount = new LongAdder();
      LongAdder successCount = new LongAdder();
      int startIndex, currentRegionIndex;
      RegionInfo current;

      // need to get all regions for the table, regardless of region state
      List<RegionInfo> regions = admin.getRegions(table);
      List<Future<Void>> regionsMergingRequests = new ArrayList<>(20);
      Map<Future<Void>, Pair<RegionInfo, RegionInfo>> regionsMerging = new ConcurrentHashMap<>();
      //long roundsNoProgress = 0;
      while (merges.longValue() < maxNumberOfMerges) {
        LOG.info("Iteration: {}", counter);
        RegionInfo previous = null;
        boolean currentHasMergeRef = true;
        boolean previousHasMergeRef = true;
        // to request merge, regions must be OPEN, though
        regions = getOpenRegions(conn, table);
        LOG.info("Current number of open regions: {}", regions.size());
        LOG.info("Attempting to merge regions to reach the remaining target of {} merges",
            maxNumberOfMerges - merges.longValue());
        startIndex = rand.nextInt(regions.size());
        LOG.info("Starting from region index {}", startIndex);
        for (int i = 0; i < regions.size(); i++) {
          currentRegionIndex = (startIndex + i) % regions.size();
          current = regions.get(currentRegionIndex);

          /*
           * RegionInfo.getRegionId() returns the region's creation timestamp
           * in milliseconds (set at region creation time).
           * This is the standard way to determine region "age" in HBase.
           */
          long regionId = current.getRegionId();
          LOG.info("Evaluating region {}", regionId);

          if (!current.isSplit() && regionId < minRegionAgeMilliseconds) {
            boolean merging;
            if (previous != null) {
              merging = canMerge(tableDir, previous, current, regionsMerging.values());
            } else {
              merging = false;
            }

            if (merging) {
              // Before submitting a merge request, we need to check if any of the region candidates
              // still have merge references from previous cycle
              currentHasMergeRef = hasPreviousMergeRef(conn, current);
              boolean hasMergeRef = previousHasMergeRef || currentHasMergeRef;
              if (!hasMergeRef) {
                LOG.info("Requesting merge...");
                regionsMergingRequests.add(requestMergeRegions(admin, current, previous, regionsMerging));
                merges.increment();
                mergesThisRound.increment();

                LOG.info("({}|{}) Requested the merging of regions {} and {} together.",
                    mergesThisRound.intValue(),
                    merges.intValue(),
                    current.getEncodedName(),
                    previous.getEncodedName());

                if (mergesThisRound.longValue() >= maxMergesPerRound) {
                  LOG.info("The target of region merges this round was reached: Stopping merging.");
                  break;
                }
                if (merges.intValue() >= maxNumberOfMerges) {
                  LOG.info("The target of region total merges was reached: Stopping merging.");
                  break;
                }
              } else {
                LOG.info("Skipping merge of candidates {} and {} because of existing merge "
                    + "qualifiers.", previous.getEncodedName(), current.getEncodedName());
              }
              previous = null;
              previousHasMergeRef = true;
            } else {
              previous = current;
              previousHasMergeRef = currentHasMergeRef;
            }
          } else {
            LOG.debug("Skipping split region: {}", current.getEncodedName());
          }
        }

        LOG.info("Waiting for all the merging requests to be processed...");
        for (Future<Void> f : regionsMergingRequests) {
          f.get();
        }
        LOG.info("All requests where submitted.");

        counter.increment();
        waitingRounds.reset();

        regionsMerging.forEach((f, currentPair) -> {
          LOG.info("Waiting for {} and {} merging.", currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName());
          try {
            f.get();
            successCount.increment();
          } catch (InterruptedException | ExecutionException e) {
            LOG.error("Merging regions failed:", e);
            failureCount.increment();
          }
        });

        LOG.info("All requests completed: Success={} Failures={}",
            successCount.intValue(), failureCount.intValue());

        mergesThisRound.reset();
        regionsMerging.clear();

        if (merges.longValue() < maxNumberOfMerges) {
          LOG.info("Sleeping for {} seconds before starting the next iteration...",
              (sleepBetweenCycles / 1000));
          Thread.sleep(sleepBetweenCycles);
        }

        /*while (!regionsMerging.isEmpty() && waitingRounds.intValue() < maxSleepRoundsBetweenCycles) {
          LOG.info("Sleeping for {} seconds before starting the next iteration...",
              (sleepBetweenCycles / 1000));
          Thread.sleep(sleepBetweenCycles);
          regionsMerging.forEach((f, currentPair) -> {
            if (f.isDone()) {
              LOG.info("Merged regions {} and {} together.", currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName());
              regionsMerging.remove(f);
              //lastTimeProgressed.reset();
              //lastTimeProgressed.add(counter.longValue());
            } else if (f.isCancelled()) {
              //LOG.info("Cancelling merge regions {} and {} together.", currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName());
              regionsMerging.remove(f);
            } //else {
              //LOG.warn("Merge of regions {} and {} isn't completed yet.", currentPair.getFirst(), currentPair.getSecond());
            //}
          });
          waitingRounds.increment();
        }*/
        /*
        if (regionsMerging.isEmpty()) {
          LOG.info("All regions from the previous iteration were merged (total merged: {}).",
              mergesThisRound.intValue());
        } else {
          LOG.warn("Some regions from the previous iteration are still pending (total merged: {}, total pending: {})...",
              mergesThisRound.intValue(), regionsMerging.size());
        }*/

        /*roundsNoProgress = counter.longValue() - lastTimeProgressed.longValue();
        if (roundsNoProgress == this.maxRoundsStuck) {
          LOG.warn("Reached {} iterations without progressing with new merges. Aborting...",
            roundsNoProgress);
          break;
        }*/
        /*
        LOG.info("Major compacting the hbase:meta table before the next round");
        admin.majorCompact(TableName.META_TABLE_NAME);
        Thread.sleep(sleepBetweenCycles);
        */
      }
    }
  }

  @Override
  public int run(String[] args) {
    if (args.length != 2) {
      LOG.error(
        "Wrong number of arguments. " + "Arguments are: <TABLE_NAME> <MAX_NUMBER_OF_MERGES>");
      return 1;
    }
    try {
      this.mergeRegions(args[0], Integer.parseInt(args[1]));
    } catch (Exception e) {
      LOG.error("Merging regions failed:", e);
      return 2;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int errCode = ToolRunner.run(new RegionsMerger2(conf), args);
    if (errCode != 0) {
      System.exit(errCode);
    }
  }
}
