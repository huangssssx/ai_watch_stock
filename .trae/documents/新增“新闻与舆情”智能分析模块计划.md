I will upgrade the "Valley Sniper Stock Selection" strategy by integrating the academic optimizations provided.

**Plan:**

1. **Analyze & Prepare**:

   * Confirm data availability (Market Cap) from `akshare`.

   * Create a new script `backend/scripts/选股策略/山谷狙击选股策略_optimized.py` that adapts the provided `ValleySniperAdvanced` logic into the existing script's framework.

   * Replace missing dependencies (`pykalman`, `sklearn`) with existing implementations or standard libraries (`scipy`, `numpy`).

   * Key adaptations:

     * **Dynamic Divergence**: Implement `_get_bb_troughs` using `scipy.signal.argrelextrema`.

     * **Market Cap Tiered Volume**: Use "流通市值" from spot data to adjust volume thresholds.

     * **Overhead Supply**: Implement STH-CB and drawdown checks.

     * **Composite ILLIQ**: Combine Amihud and HL Spread.

     * **VRP**: Implement Volatility Risk Premium calculation.

2. **Implementation**:

   * Write the optimized Python script.

   * Ensure it handles data fetching and errors gracefully (similar to the original script).

3. **Verification**:

   * Run the new script to ensure it executes without errors and produces a valid stock list.

   * Check if the output includes the new indicators/scores.

4. **Deployment**:

   * Update the `stock_screeners` table in `backend/stock_watch.db` (ID 5) with the content of the optimized script.

   * (Optional) Keep the file on disk for reference.

