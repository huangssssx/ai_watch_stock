import React, { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, Spin, Empty } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import { AreaChart, Area, XAxis, YAxis, Tooltip } from 'recharts';
import { getStockDaily } from '../api';
import type { Stock, StockPricePoint } from '../types';

interface StockChartsProps {
  stocks: Stock[];
  active: boolean; // Only fetch if active
}

interface ChartContainerProps {
  children: (size: { width: number; height: number }) => React.ReactNode;
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

  const fetchData = useCallback(() => {
    stocks.forEach(async (stock) => {
      setLoading(prev => ({ ...prev, [stock.symbol]: true }));
      try {
        const res = await getStockDaily(stock.symbol);
        if (res.data.ok && res.data.data) {
          setDataMap(prev => ({ ...prev, [stock.symbol]: res.data.data ?? [] }));
        }
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(prev => ({ ...prev, [stock.symbol]: false }));
      }
    });
  }, [stocks]);

  useEffect(() => {
    if (active) {
      fetchData();
      const interval = setInterval(fetchData, 60000);
      return () => {
        clearInterval(interval);
      };
    }
  }, [active, fetchData]);

  if (!active) return null;

  if (!stocks.length) return <Empty description="暂无股票" />;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '16px' }}>
      {stocks.map(stock => {
        const data = dataMap[stock.symbol] || [];
        const lastPrice = data.length ? data[data.length - 1].close : 0;
        const firstPrice = data.length ? data[0].open : lastPrice; // Use open price of the day as base
        const change = lastPrice - firstPrice;
        const percent = firstPrice ? (change / firstPrice) * 100 : 0;
        const color = change >= 0 ? '#cf1322' : '#3f8600'; // Red up, Green down (China style)

        return (
          <Card 
            key={stock.id} 
            size="small" 
            title={`${stock.name} (${stock.symbol})`} 
            extra={
             data.length > 0 && (
                <span style={{ color, fontWeight: 'bold' }}>
                    {lastPrice.toFixed(2)} 
                    <span style={{ fontSize: 12, marginLeft: 4 }}>
                        {change >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                        {Math.abs(percent).toFixed(2)}%
                    </span>
                </span>
             )
            }
            hoverable
            onClick={() =>
              navigate(`/stock/${stock.symbol}`, { state: { stockName: stock.name, returnTo: '/dashboard?tab=charts' } })
            }
          >
            <div style={{ height: 200, width: '100%' }}>
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
                          labelFormatter={(label) => label}
                          formatter={(value: unknown) => {
                            const num = typeof value === 'number' ? value : Number(value);
                            return [Number.isFinite(num) ? num.toFixed(2) : '-', '价格'];
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
                    </AreaChart>
                  )}
                </ChartContainer>
              )}
            </div>
          </Card>
        );
      })}
    </div>
  );
};

export default StockCharts;
