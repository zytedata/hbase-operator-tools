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

import static org.apache.hadoop.hbase.HConstants.CATALOG_FAMILY;
import static org.apache.hadoop.hbase.HConstants.REGIONINFO_QUALIFIER;
import static org.apache.hadoop.hbase.HConstants.STATE_QUALIFIER;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase maintenance tool for merging regions of a specific table, until a target number of regions
 * for the table is reached, or no more merges can complete due to limit in resulting merged region
 * size.
 */
public class RegionsMerger extends Configured implements org.apache.hadoop.util.Tool {
  private static final long GIGABYTE = 1024*1024*1024;

  private static final Logger LOG = LoggerFactory.getLogger(RegionsMerger.class.getName());
  public static final String RESULTING_REGION_UPPER_MARK = "hbase.tools.merge.upper.mark";
  public static final String SLEEP = "hbase.tools.merge.sleep";
  public static final String MAX_ROUNDS_IDLE = "hbase.tools.max.iterations.blocked";
  public static final String MAX_MERGES_PER_ROUND = "hbase.tools.merge.max.merges_per_round";
  public static final String MIN_REGION_AGE_DAYS = "hbase.tools.merge.min.age_days";
  public static final String MERGE_TIMEOUT_SECS = "hbase.tools.merge.timeout_secs";

  private final Configuration conf;
  private final FileSystem fs;
  private final double resultSizeThreshold;
  private final int sleepBetweenCycles;
  private final long maxRoundsStuck;
  private final int maxMergesPerRound;
  private final int minRegionAgeDays;
  private final int mergeTimeoutSecs;

  private final ExecutorService executor;

  public RegionsMerger(Configuration conf) throws IOException {
    this.conf = conf;
    Path basePath = new Path(conf.get(HConstants.HBASE_DIR));
    fs = basePath.getFileSystem(conf);
    resultSizeThreshold = this.conf.getDouble(RESULTING_REGION_UPPER_MARK, 0.9)
        * this.conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
    sleepBetweenCycles = this.conf.getInt(SLEEP, 2000);
    this.maxRoundsStuck = this.conf.getInt(MAX_ROUNDS_IDLE, 10);
    this.maxMergesPerRound = this.conf.getInt(MAX_MERGES_PER_ROUND, 30);
    this.minRegionAgeDays = this.conf.getInt(MIN_REGION_AGE_DAYS, 7);
    this.mergeTimeoutSecs = this.conf.getInt(MERGE_TIMEOUT_SECS, 60);

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
      long size1 = sumSizeInFS(new Path(path, region1.getEncodedName()));
      long size2 = sumSizeInFS(new Path(path, region2.getEncodedName()));
      boolean mergeable = (resultSizeThreshold > (size1 + size2));
      if (!mergeable) {
        LOG.warn(
            "Not merging regions {} and {} because resulting region size would get too close to "
                + "the {} GB limit. {} total size: {} GB; {} total size:{} GB",
            region1.getEncodedName(), region2.getEncodedName(), resultSizeThreshold / GIGABYTE,
            region1.getEncodedName(), size1 / GIGABYTE,
            region2.getEncodedName(), size2 / GIGABYTE);
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
      LOG.warn("Region {} has an existing merge qualifier and can't be merged for now. \n "
          + "RegionsMerger will skip this region until the merge qualifier is cleaned away. \n "
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

  public void mergeRegions(String tblName, int targetRegions) throws Exception {
    LOG.info("Table name               : {}", tblName);
    LOG.info("Target number of regions : {}", targetRegions);
    LOG.info("Max region size threshold: {} GB", resultSizeThreshold / GIGABYTE);
    LOG.info("Sleep between cycles     : {} ms", sleepBetweenCycles);
    LOG.info("Max rounds stuck         : {}", maxRoundsStuck);
    LOG.info("Max merges per round     : {}", maxMergesPerRound);
    LOG.info("Min region age           : {} d", minRegionAgeDays);

    Random rand = new Random();
    long minRegionAgeMilliseconds = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(minRegionAgeDays);

    TableName table = TableName.valueOf(tblName);
    Path tableDir = getTablePath(table);
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      Admin admin = conn.getAdmin();

      long totalIterations = 0;
      long mergeAttemptsThisRound, mergeSubmitsFailedThisRound;
      LongAdder failureCount = new LongAdder();
      LongAdder successCount = new LongAdder();
      long lastSuccessCount = 0;
      long roundsNoProgress = 0;
      long regionsCount = 0;

      // need to get all regions for the table, regardless of region state
      regionsCount = admin.getRegions(table).size();
      List<RegionInfo> regionsOpen;

      List<Future<Void>> mergeSubmitsThisRoundFutures = new ArrayList<>(maxMergesPerRound);
      Map<Future<Void>, Pair<RegionInfo, RegionInfo>> regionsMerging = new ConcurrentHashMap<>();

      int startRegionIndex, currentRegionIndex;
      RegionInfo current, previous;
      boolean currentHasMergeRef, previousHasMergeRef;

      while (regionsCount > targetRegions) {
        LOG.info("Iteration                : {}", totalIterations);
        previous = null;
        currentHasMergeRef = true;
        previousHasMergeRef = true;
        mergeAttemptsThisRound = 0;
        mergeSubmitsFailedThisRound = 0;

        // to request merge, regions must be OPEN, though
        regionsOpen = getOpenRegions(conn, table);
        LOG.info("Current number of regions: {} (open: {})", regionsCount, regionsOpen.size());
        LOG.info("Target number of regions : {}", targetRegions);
        startRegionIndex = rand.nextInt(regionsOpen.size());
        LOG.info("Starting from index      : {}", startRegionIndex);
        for (int i = 0; i < regionsOpen.size(); i++) {
          currentRegionIndex = (startRegionIndex + i) % regionsOpen.size();
          current = regionsOpen.get(currentRegionIndex);

          /*
           * RegionInfo.getRegionId() returns the region's creation timestamp
           * in milliseconds (set at region creation time).
           * This is the standard way to determine region "age" in HBase.
           */
          long regionId = current.getRegionId();

          if (!current.isSplit() && regionId < minRegionAgeMilliseconds) {
            currentHasMergeRef = hasPreviousMergeRef(conn, current);
            if (
                previous != null && canMerge(tableDir, previous, current, regionsMerging.values())
            ) {
              // Before submitting a merge request, we need to check if any of the region candidates
              // still have merge references from previous cycle
              boolean hasMergeRef = previousHasMergeRef || currentHasMergeRef;
              if (!hasMergeRef) {
                mergeSubmitsThisRoundFutures.add(requestMergeRegions(admin, current, previous, regionsMerging));
                mergeAttemptsThisRound++;

                LOG.info("({}/{}) Requested the merging of regions {} and {} together.",
                    mergeAttemptsThisRound,
                    maxMergesPerRound,
                    current.getEncodedName(),
                    previous.getEncodedName());

                if (mergeAttemptsThisRound >= maxMergesPerRound) {
                  LOG.info("The target number of merges this round has been reached: Stopping merging.");
                  break;
                }
                if (regionsCount - mergeAttemptsThisRound <= targetRegions) {
                  LOG.info("The target number of region may have been reached: Stopping merging.");
                  break;
                }

                previous = null;
              } else {
                LOG.info("Skipping merge of candidates {} and {} because of existing merge "
                    + "qualifiers.", previous.getEncodedName(), current.getEncodedName());

                previous = (!currentHasMergeRef) ? current : null;
                previousHasMergeRef = currentHasMergeRef;
              }
            } else {
              previous = (!currentHasMergeRef) ? current : null;
              previousHasMergeRef = currentHasMergeRef;
            }
          } else {
            LOG.debug("Skipping split region: {}", current.getEncodedName());
          }
        }

        LOG.info("Waiting for all the merging requests to be processed...");
        for (Future<Void> f : mergeSubmitsThisRoundFutures) {
          try {
            f.get(mergeTimeoutSecs, TimeUnit.SECONDS);
          } catch (InterruptedException | TimeoutException | ExecutionException e) {
            // Failed submitting merging request. Ignoring this request.
            mergeSubmitsFailedThisRound++;
          }
        }

        mergeSubmitsThisRoundFutures.clear();
        LOG.info("All requests were submitted ({} failed)", mergeSubmitsFailedThisRound);

        // Track merges that are still in progress (e.g., timed out but not failed) so we can
        // re-check them in subsequent rounds instead of dropping tracking entirely.
        Map<Future<Void>, Pair<RegionInfo, RegionInfo>> stillMerging = new ConcurrentHashMap<>();
        regionsMerging.forEach((f, currentPair) -> {
          LOG.info("Waiting for {} and {} to be merged.",
              currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName());
          try {
            f.get(mergeTimeoutSecs, TimeUnit.SECONDS);
            successCount.increment();
          } catch (TimeoutException te) {
            // The merge may still be running on the server; keep tracking it for the next round.
            LOG.warn("Timed out waiting for merge of {} and {} to complete; will re-check later.",
                currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName());
            stillMerging.put(f, currentPair);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while waiting for merge of {} and {}.",
                currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName(), ie);
            failureCount.increment();
          } catch (ExecutionException ee) {
            LOG.error("Merging regions {} and {} failed:",
                currentPair.getFirst().getEncodedName(), currentPair.getSecond().getEncodedName(), ee);
            failureCount.increment();
          }
        });

        // Replace the old map with only those merges that are still in progress.
        regionsMerging.clear();
        regionsMerging.putAll(stillMerging);
        LOG.info("All requests completed (this round): Success={} Failures={} StillInProgress={}",
            successCount.longValue(), failureCount.longValue(), regionsMerging.size());

        totalIterations++;

        if (successCount.longValue() == lastSuccessCount) {
          roundsNoProgress++;
          LOG.warn("No progress this round ({}/{})...", roundsNoProgress, this.maxMergesPerRound);
          if (roundsNoProgress >= this.maxRoundsStuck) {
            LOG.warn("Reached {} iterations without progressing with new merges. Aborting...",
                roundsNoProgress);
            return;
          }
        } else {
          lastSuccessCount = successCount.longValue();
          roundsNoProgress = 0;
        }

        regionsCount = admin.getRegions(table).size();
        if (regionsCount > targetRegions) {
          LOG.info("Sleeping for {} seconds before starting the next iteration...",
              (sleepBetweenCycles / 1000));
          Thread.sleep(sleepBetweenCycles);
        }
      }
    }
  }

  @Override
  public int run(String[] args) {
    if (args.length != 2) {
      LOG.error(
          "Wrong number of arguments. " + "Arguments are: <TABLE_NAME> <TARGET_NUMBER_OF_REGIONS>");
      return 1;
    }
    int returnCode = 0;
    try {
      this.mergeRegions(args[0], Integer.parseInt(args[1]));
    } catch (Exception e) {
      LOG.error("Merging regions failed:", e);
      returnCode = 2;
    } finally {
      executor.shutdown();
    }
    return returnCode;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int errCode = ToolRunner.run(new RegionsMerger(conf), args);
    if (errCode != 0) {
      System.exit(errCode);
    }
  }
}
