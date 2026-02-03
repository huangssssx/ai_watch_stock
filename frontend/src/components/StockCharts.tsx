import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, Spin, Empty, Space, Switch, Tooltip as AntTooltip } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { AreaChart, Area, XAxis, YAxis, Tooltip, Line, LineChart, ReferenceLine } from 'recharts';
import { getStockDaily, getStockHistory } from '../api';
import type { Stock, StockPricePoint } from '../types';
import {
  enrichIntradayWithRealtimeBiasBoll20,
  normalizeDate,
  selectPrevDailyCloses,
} from '../utils/indicators';

interface StockChartsProps {
  stocks: Stock[];
  active: boolean; // Only fetch if active
}

interface ChartContainerProps {
  children: (size: { width: number; height: number }) => React.ReactNode;
}

function formatNumber(value: unknown, digits: number): string {
  const num = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(num)) return '-';
  return num.toFixed(digits);
}

function readBoolFromLocalStorage(key: string, fallback: boolean): boolean {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw) as unknown;
    return typeof parsed === 'boolean' ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function writeBoolToLocalStorage(key: string, value: boolean) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    return;
  }
}

const DAILY_CACHE_TTL_MS = 60 * 60 * 1000;
function readDailyHistoryCache(symbol: string): StockPricePoint[] | null {
  try {
    const raw = sessionStorage.getItem(`daily_history_${symbol}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { ts: number; data: StockPricePoint[] } | null;
    if (!parsed || typeof parsed.ts !== 'number' || !Array.isArray(parsed.data)) return null;
    if (Date.now() - parsed.ts > DAILY_CACHE_TTL_MS) return null;
    return parsed.data;
  } catch {
    return null;
  }
}

function writeDailyHistoryCache(symbol: string, data: StockPricePoint[]) {
  try {
    sessionStorage.setItem(`daily_history_${symbol}`, JSON.stringify({ ts: Date.now(), data }));
  } catch {
    return;
  }
}

async function runWithConcurrencyLimit<T>(limit: number, tasks: Array<() => Promise<T>>): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let cursor = 0;
  const workers = new Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
    while (cursor < tasks.length) {
      const idx = cursor;
      cursor += 1;
      results[idx] = await tasks[idx]();
    }
  });
  await Promise.all(workers);
  return results;
}

const ChartContainer: React.FC<ChartContainerProps> = ({ children }) => {
  const ref = React.useRef<HTMLDivElement>(null);
  const [size, setSize] = useState<{ width: number; height: number }>({ width: 0, height: 0 });

  useEffect(() => {
    if (!ref.current) return;

    const measure = () => {
      const rect = ref.current?.getBoundingClientRect();
      if (!rect) return;
      if (rect.width > 0 && rect.height > 0) {
        setSize({ width: rect.width, height: rect.height });
      }
    };

    measure();

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.contentRect.width > 0 && entry.contentRect.height > 0) {
          setSize({ width: entry.contentRect.width, height: entry.contentRect.height });
        }
      }
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={ref} style={{ width: '100%', height: '100%' }}>
      {size.width > 0 && size.height > 0 ? (
        children(size)
      ) : (
        <div style={{ height: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
          <Spin />
        </div>
      )}
    </div>
  );
};

const StockCharts: React.FC<StockChartsProps> = ({ stocks, active }) => {
  const navigate = useNavigate();
  const [dataMap, setDataMap] = useState<Record<string, StockPricePoint[]>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [showBias20, setShowBias20] = useState(() => readBoolFromLocalStorage('charts_show_bias20', false));
  const [showBoll, setShowBoll] = useState(() => readBoolFromLocalStorage('charts_show_boll', false));
  const dailyHistoryRef = useRef<Record<string, StockPricePoint[]>>({});
  const [indicatorStatus, setIndicatorStatus] = useState<Record<string, { ok: boolean; reason?: string }>>({});

  useEffect(() => {
    writeBoolToLocalStorage('charts_show_bias20', showBias20);
  }, [showBias20]);

  useEffect(() => {
    writeBoolToLocalStorage('charts_show_boll', showBoll);
  }, [showBoll]);

  const prefetchDailyHistories = useCallback(async () => {
    if (!active) return;
    if (!(showBias20 || showBoll)) return;
    const tasks = stocks
      .filter((s) => {
        if (dailyHistoryRef.current[s.symbol]) return false;
        const cached = readDailyHistoryCache(s.symbol);
        if (cached) {
          dailyHistoryRef.current[s.symbol] = cached;
          return false;
        }
        return true;
      })
      .map((s) => async () => {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          try {
            const res = await getStockHistory(s.symbol, 'daily');
            if (res.data.ok && res.data.data) {
              dailyHistoryRef.current[s.symbol] = res.data.data;
              writeDailyHistoryCache(s.symbol, res.data.data);
              return;
            }
          } catch {
            continue;
          }
        }
      });

    if (!tasks.length) return;
    await runWithConcurrencyLimit(3, tasks);
  }, [active, showBias20, showBoll, stocks]);

  const fetchIntradayData = useCallback(async () => {
    const tasks = stocks.map((stock) => async () => {
      setLoading((prev) => ({ ...prev, [stock.symbol]: true }));
      try {
        const res = await getStockDaily(stock.symbol);
        if (!res.data.ok || !res.data.data) return;

        const rawPoints = res.data.data ?? [];

        if (!(showBias20 || showBoll)) {
          setDataMap((prev) => ({ ...prev, [stock.symbol]: rawPoints }));
          setIndicatorStatus((prev) => ({ ...prev, [stock.symbol]: { ok: true } }));
          return;
        }

        const dailyHistory = dailyHistoryRef.current[stock.symbol];
        const todayYmd = normalizeDate(rawPoints[rawPoints.length - 1]?.date) ?? '';

        if (!dailyHistory || !todayYmd) {
          setDataMap((prev) => ({ ...prev, [stock.symbol]: rawPoints }));
          setIndicatorStatus((prev) => ({ ...prev, [stock.symbol]: { ok: false, reason: '日线数据未就绪' } }));
          return;
        }

        const prev19 = selectPrevDailyCloses(dailyHistory, todayYmd, 19);
        const enriched = enrichIntradayWithRealtimeBiasBoll20(rawPoints, prev19, {
          window: 20,
          prevRequired: 19,
          bollK: 2,
          ddof: 1,
        });

        if (!enriched.ok) {
          setDataMap((prev) => ({ ...prev, [stock.symbol]: rawPoints }));
          setIndicatorStatus((prev) => ({ ...prev, [stock.symbol]: { ok: false, reason: enriched.reason } }));
          return;
        }

        setDataMap((prev) => ({ ...prev, [stock.symbol]: enriched.points }));
        setIndicatorStatus((prev) => ({ ...prev, [stock.symbol]: { ok: true } }));
      } catch (e) {
        console.error(e);
      } finally {
        setLoading((prev) => ({ ...prev, [stock.symbol]: false }));
      }
    });

    await runWithConcurrencyLimit(6, tasks);
  }, [showBias20, showBoll, stocks]);

  useEffect(() => {
    if (active) {
      prefetchDailyHistories();
      fetchIntradayData();
      const interval = setInterval(fetchIntradayData, 60000);
      return () => {
        clearInterval(interval);
      };
    }
  }, [active, fetchIntradayData, prefetchDailyHistories]);

  useEffect(() => {
    if (!active) return;
    prefetchDailyHistories();
  }, [active, prefetchDailyHistories, showBias20, showBoll]);

  if (!active) return null;

  if (!stocks.length) return <Empty description="暂无股票" />;

  const controls = (
    <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
      <Space size={12}>
        <span>指标</span>
        <Space size={6}>
          <span>BIAS20</span>
          <Switch checked={showBias20} onChange={setShowBias20} />
        </Space>
        <Space size={6}>
          <span>布林带</span>
          <Switch checked={showBoll} onChange={setShowBoll} />
        </Space>
      </Space>
    </div>
  );

  return (
    <div>
      {controls}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '16px' }}>
      {stocks.map(stock => {
        const data = dataMap[stock.symbol] || [];
        const lastPrice = data.length ? data[data.length - 1].close : 0;
        const firstPrice = data.length ? data[0].open : lastPrice; // Use open price of the day as base
        const change = lastPrice - firstPrice;
        const percent = firstPrice ? (change / firstPrice) * 100 : 0;
        const color = change >= 0 ? '#cf1322' : '#3f8600'; // Red up, Green down (China style)
        const needIndicators = showBias20 || showBoll;
        const status = indicatorStatus[stock.symbol];
        const showIndicatorHint = needIndicators && status && !status.ok;

        return (
          <Card 
            key={stock.id} 
            size="small" 
            title={`${stock.name} (${stock.symbol})`} 
            extra={
             data.length > 0 && (
                <Space size={8}>
                  {showIndicatorHint ? (
                    <AntTooltip title={status.reason || '历史日线不足，暂不显示指标'}>
                      <InfoCircleOutlined style={{ color: '#8c8c8c' }} />
                    </AntTooltip>
                  ) : null}
                  <span style={{ color, fontWeight: 'bold' }}>
                      {lastPrice.toFixed(2)} 
                      <span style={{ fontSize: 12, marginLeft: 4 }}>
                          {change >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                          {Math.abs(percent).toFixed(2)}%
                      </span>
                  </span>
                </Space>
             )
            }
            hoverable
            onClick={() =>
              navigate(`/stock/${stock.symbol}`, { state: { stockName: stock.name, returnTo: '/dashboard?tab=charts' } })
            }
          >
            <div style={{ width: '100%' }}>
              <div style={{ height: showBias20 ? 170 : 200, width: '100%' }}>
                {(loading[stock.symbol] && !data.length) ? (
                  <div style={{ height: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                      <Spin />
                  </div>
                ) : (
                  <ChartContainer>
                    {({ width, height }) => (
                      <AreaChart width={width} height={height} data={data}>
                        <defs>
                          <linearGradient id={`color${stock.symbol}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={color} stopOpacity={0.1}/>
                            <stop offset="95%" stopColor={color} stopOpacity={0}/>
                          </linearGradient>
                        </defs>
                        <XAxis dataKey="time" minTickGap={30} tick={{ fontSize: 10 }} />
                        <YAxis domain={['auto', 'auto']} hide />
                        <Tooltip
                          content={({ active: tooltipActive, payload, label }) => {
                            if (!tooltipActive || !payload || !payload.length) return null;
                            const getVal = (key: string) => payload.find((x) => x.dataKey === key)?.value;
                            const closeV = getVal('close');
                            const upperV = getVal('boll_upper');
                            const midV = getVal('boll_mid');
                            const lowerV = getVal('boll_lower');
                            const biasV = getVal('bias20');
                            return (
                              <div style={{ background: 'rgba(255,255,255,0.95)', border: '1px solid #f0f0f0', padding: 8 }}>
                                <div style={{ fontSize: 12, marginBottom: 6 }}>{label}</div>
                                <div style={{ fontSize: 12 }}>价格：{formatNumber(closeV, 2)}元</div>
                                {showBoll ? (
                                  <>
                                    <div style={{ fontSize: 12 }}>布林上轨：{formatNumber(upperV, 2)}元</div>
                                    <div style={{ fontSize: 12 }}>布林中轨：{formatNumber(midV, 2)}元</div>
                                    <div style={{ fontSize: 12 }}>布林下轨：{formatNumber(lowerV, 2)}元</div>
                                  </>
                                ) : null}
                                {showBias20 ? (
                                  <div style={{ fontSize: 12 }}>BIAS20：{formatNumber(biasV, 1)}%</div>
                                ) : null}
                              </div>
                            );
                          }}
                        />
                        <Area 
                            type="monotone" 
                            dataKey="close" 
                            stroke={color} 
                            fillOpacity={1} 
                            fill={`url(#color${stock.symbol})`} 
                            isAnimationActive={false}
                        />
                        {showBoll ? (
                          <>
                            <Line
                              type="monotone"
                              dataKey="boll_mid"
                              stroke="#595959"
                              strokeWidth={1}
                              strokeDasharray="4 4"
                              dot={false}
                              isAnimationActive={false}
                            />
                            <Line
                              type="monotone"
                              dataKey="boll_upper"
                              stroke="#bfbfbf"
                              strokeWidth={1}
                              strokeOpacity={0.6}
                              dot={false}
                              isAnimationActive={false}
                            />
                            <Line
                              type="monotone"
                              dataKey="boll_lower"
                              stroke="#bfbfbf"
                              strokeWidth={1}
                              strokeOpacity={0.6}
                              dot={false}
                              isAnimationActive={false}
                            />
                          </>
                        ) : null}
                      </AreaChart>
                    )}
                  </ChartContainer>
                )}
              </div>
              {showBias20 ? (
                <div style={{ height: 70, width: '100%', marginTop: 8 }}>
                  <ChartContainer>
                    {({ width, height }) => (
                      <LineChart width={width} height={height} data={data}>
                        <XAxis dataKey="time" hide />
                        <YAxis
                          domain={['auto', 'auto']}
                          width={36}
                          tick={{ fontSize: 10 }}
                          tickFormatter={(v) => formatNumber(v, 0)}
                        />
                        <ReferenceLine y={0} stroke="#000" strokeWidth={1} />
                        <ReferenceLine y={5} stroke="#cf1322" strokeDasharray="4 4" />
                        <ReferenceLine y={8} stroke="#cf1322" strokeDasharray="4 4" />
                        <ReferenceLine y={-5} stroke="#3f8600" strokeDasharray="4 4" />
                        <ReferenceLine y={-8} stroke="#3f8600" strokeDasharray="4 4" />
                        <Line
                          type="monotone"
                          dataKey="bias20"
                          stroke="#1890ff"
                          strokeWidth={1}
                          dot={false}
                          isAnimationActive={false}
                        />
                      </LineChart>
                    )}
                  </ChartContainer>
                </div>
              ) : null}
            </div>
          </Card>
        );
      })}
      </div>
    </div>
  );
};

export default StockCharts;
